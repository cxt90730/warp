/*
 * Warp (C) 2019-2020 MinIO, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package cli

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/klauspost/compress/zstd"
	"github.com/minio/cli"
	"github.com/minio/mc/pkg/probe"
	"github.com/minio/pkg/v2/console"
	"github.com/minio/warp/api"
	"github.com/minio/warp/pkg/aggregate"
	"github.com/minio/warp/pkg/bench"

	"github.com/go-echarts/go-echarts/v2/charts"
	"github.com/go-echarts/go-echarts/v2/opts"
	"github.com/go-echarts/go-echarts/v2/types"
)

var analyzeFlags = []cli.Flag{
	cli.StringFlag{
		Name:  "analyze.dur",
		Value: "",
		Usage: "Split analysis into durations of this length. Can be '1s', '5s', '1m', etc.",
	},
	cli.StringFlag{
		Name:  "analyze.interval",
		Value: "",
		Usage: "Split analysis into intervals of this length. Can be '1s', '5s', '1m', etc.",
	},
	cli.BoolFlag{
		Name:  "analyze.chart",
		Usage: "Draw a chart of the analysis.",
	},
	cli.StringFlag{
		Name:  "analyze.out",
		Value: "",
		Usage: "Output aggregated data as to file",
	},
	cli.StringFlag{
		Name:  "analyze.op",
		Value: "",
		Usage: "Only output for this op. Can be GET/PUT/DELETE, etc.",
	},
	cli.StringFlag{
		Name:  "analyze.host",
		Value: "",
		Usage: "Only output for this host.",
	},
	cli.DurationFlag{
		Name:   "analyze.skip",
		Usage:  "Additional duration to skip when analyzing data.",
		Hidden: false,
		Value:  0,
	},
	cli.IntFlag{
		Name:   "analyze.limit",
		Usage:  "Max operations to load for analysis.",
		Hidden: true,
		Value:  0,
	},
	cli.IntFlag{
		Name:   "analyze.offset",
		Usage:  "Skip this number of operations for analysis",
		Hidden: true,
		Value:  0,
	},
	cli.BoolFlag{
		Name:  "analyze.v",
		Usage: "Display additional analysis data.",
	},
	cli.StringFlag{
		Name:  serverFlagName,
		Usage: "When running benchmarks open a webserver to fetch results remotely, eg: localhost:7762",
	},
}

var analyzeCmd = cli.Command{
	Name:   "analyze",
	Usage:  "analyze existing benchmark data",
	Action: mainAnalyze,
	Before: setGlobalsFromContext,
	Flags:  combineFlags(globalFlags, analyzeFlags),
	CustomHelpTemplate: `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} [FLAGS] benchmark-data-file
  -> see https://github.com/minio/warp#analysis

Use - as input to read from stdin.

FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}`,
}

// mainAnalyze is the entry point for analyze command.
func mainAnalyze(ctx *cli.Context) error {
	checkAnalyze(ctx)
	args := ctx.Args()
	if len(args) == 0 {
		console.Fatal("No benchmark data file supplied")
	}
	if len(args) > 1 {
		console.Fatal("Only one benchmark file can be given")
	}
	zstdDec, _ := zstd.NewReader(nil)
	defer zstdDec.Close()
	monitor := api.NewBenchmarkMonitor(ctx.String(serverFlagName))
	defer monitor.Done()
	log := console.Printf
	if globalQuiet {
		log = nil
	}
	for _, arg := range args {
		var input io.Reader
		if arg == "-" {
			input = os.Stdin
		} else {
			f, err := os.Open(arg)
			fatalIf(probe.NewError(err), "Unable to open input file")
			defer f.Close()
			input = f
		}
		err := zstdDec.Reset(input)
		fatalIf(probe.NewError(err), "Unable to read input")
		ops, err := bench.OperationsFromCSV(zstdDec, true, ctx.Int("analyze.offset"), ctx.Int("analyze.limit"), log)
		fatalIf(probe.NewError(err), "Unable to parse input")

		printAnalysis(ctx, ops)
		monitor.OperationsReady(ops, strings.TrimSuffix(filepath.Base(arg), ".csv.zst"), commandLine(ctx))
	}
	return nil
}

func printMixedOpAnalysis(ctx *cli.Context, aggr aggregate.Aggregated, details bool) {
	console.SetColor("Print", color.New(color.FgWhite))
	console.Printf("Mixed operations.")

	if aggr.MixedServerStats == nil {
		console.Errorln("No mixed stats")
	}
	for _, ops := range aggr.Operations {
		if details {
			console.Println("\n----------------------------------------")
		} else {
			console.Println("")
		}
		console.SetColor("Print", color.New(color.FgHiWhite))
		pct := 0.0
		if aggr.MixedServerStats.Operations > 0 {
			pct = 100.0 * float64(ops.Throughput.Operations) / float64(aggr.MixedServerStats.Operations)
		}
		duration := ops.EndTime.Sub(ops.StartTime).Truncate(time.Second)

		if !details || ops.Skipped {
			console.Printf("Operation: %v, %d%%, Concurrency: %d, Ran %v.\n", ops.Type, int(pct+0.5), ops.Concurrency, duration)
		} else {
			sz := ""
			if ops.SingleSizedRequests != nil && ops.SingleSizedRequests.ObjSize > 0 {
				sz = fmt.Sprintf("Size: %d bytes. ", ops.SingleSizedRequests.ObjSize)
			}
			console.Printf("Operation: %v - total: %v, %.01f%%, %vConcurrency: %d, Ran %v, starting %v\n", ops.Type, ops.Throughput.Operations, pct, sz, ops.Concurrency, duration, ops.StartTime.Truncate(time.Millisecond))
		}
		console.SetColor("Print", color.New(color.FgWhite))

		if ops.Skipped {
			console.Println("Skipping3", ops.Type, "too few samples. Longer benchmark run required for reliable results.")
			continue
		}

		if ops.Errors > 0 {
			console.SetColor("Print", color.New(color.FgHiRed))
			console.Println("Errors:", ops.Errors)
			if details {
				for _, err := range ops.FirstErrors {
					console.Println(err)
				}
			}
			console.SetColor("Print", color.New(color.FgWhite))
		}
		eps := ops.ThroughputByHost
		if len(eps) == 1 || !details {
			console.Println(" * Throughput:", ops.Throughput.StringDetails(details))
		}

		if len(eps) > 1 && details {
			console.SetColor("Print", color.New(color.FgWhite))
			console.Println("\nThroughput by host:")

			for _, ep := range ops.HostNames {
				totals := eps[ep]
				console.SetColor("Print", color.New(color.FgWhite))
				console.Print(" * ", ep, ": Avg: ", totals.StringDetails(details), ".")
				if totals.Errors > 0 {
					console.SetColor("Print", color.New(color.FgHiRed))
					console.Print(" Errors: ", totals.Errors)
				}
				console.Println("")
			}
		}

		if details {
			printRequestAnalysis(ctx, ops, details)
			console.SetColor("Print", color.New(color.FgWhite))
		}
	}
	console.SetColor("Print", color.New(color.FgHiWhite))
	dur := time.Duration(aggr.MixedServerStats.MeasureDurationMillis) * time.Millisecond
	dur = dur.Round(time.Second)
	console.Printf("\nCluster Total: %v over %v.\n", aggr.MixedServerStats.StringDetails(details), dur)
	if aggr.MixedServerStats.Errors > 0 {
		console.SetColor("Print", color.New(color.FgHiRed))
		console.Print("Total Errors:", aggr.MixedServerStats.Errors, ".\n")
	}
	console.SetColor("Print", color.New(color.FgWhite))
	if eps := aggr.MixedThroughputByHost; len(eps) > 1 && details {
		for ep, ops := range eps {
			console.Println(" * "+ep+":", ops.StringDetails(details))
		}
	}
}

func printAnalysis(ctx *cli.Context, o bench.Operations) {
	details := ctx.Bool("analyze.v")
	var wrSegs io.Writer
	prefiltered := false
	if fn := ctx.String("analyze.out"); fn != "" {
		if fn == "-" {
			wrSegs = os.Stdout
		} else {
			f, err := os.Create(fn)
			fatalIf(probe.NewError(err), "Unable to create create analysis output")
			defer console.Println("Aggregated data saved to", fn)
			defer f.Close()
			wrSegs = f
		}
	}
	if onlyHost := ctx.String("analyze.host"); onlyHost != "" {
		o2 := o.FilterByEndpoint(onlyHost)
		if len(o2) == 0 {
			hosts := o.Endpoints()
			console.Println("Host not found, valid hosts are:")
			for _, h := range hosts {
				console.Println("\t* %s", h)
			}
			return
		}
		prefiltered = true
		o = o2
	}

	if wantOp := ctx.String("analyze.op"); wantOp != "" {
		prefiltered = prefiltered || o.IsMixed()
		o = o.FilterByOp(wantOp)
	}
	durFn := func(total time.Duration) time.Duration {
		if total <= 0 {
			return 0
		}
		return analysisDur(ctx, total)
	}

	var aggrs []aggregate.Aggregated
	interval := ctx.String("analyze.interval")
	if interval != "" {
		if len(o) == 0 {
			console.Fatalln("No operations found")
			return
		}
		iTime, err := time.ParseDuration(interval)
		if err != nil {
			console.Fatalln("Invalid interval:", err)
			return
		}
		start := o[0].Start
		startIndex, endIndex := 0, 0
		for _, op := range o {
			if op.Start.Before(start.Add(iTime)) {
				endIndex++
				continue
			}
			oo := o[startIndex:endIndex]
			aggrs = append(aggrs, printAggregation(ctx, oo, prefiltered, durFn, wrSegs, details))
			startIndex = endIndex
			start = start.Add(iTime)
		}
		aggrs = append(aggrs, printAggregation(ctx, o[startIndex:], prefiltered, durFn, wrSegs, details))
		if drawLine := ctx.Bool("analyze.chart"); drawLine {
			// TODO: draw chart
			http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
				lines := DrawLineCharts(aggrs)
				if lines == nil {
					w.Header().Set("Content-Type", "text/html")
					w.WriteHeader(http.StatusOK)
					w.Write([]byte("<html><head><title>No data to analysis</title></head><body>"))
					return
				}
				for _, line := range lines {
					if line != nil && len(line.MultiSeries) != 0 {
						line.Render(w)
					}
				}
			})
			http.ListenAndServe(":8081", nil)
		}
	} else {
		printAggregation(ctx, o, prefiltered, durFn, wrSegs, details)
	}
}

func DrawLineCharts(aggrs []aggregate.Aggregated) []*charts.Line {
	var lines []*charts.Line
	// create a Avg line instance
	var RequestDelayAvgLineDataSeries []LineDataSeries
	var RequestDelay90LineDataSeries []LineDataSeries
	var RequestDelay99LineDataSeries []LineDataSeries
	var RequestDelay999LineDataSeries []LineDataSeries
	var RequestDelayStdDevLineDataSeries []LineDataSeries
	var RequestDelayMaxLineDataSeries []LineDataSeries
	var RequestDelayMinLineDataSeries []LineDataSeries
	var RequestTTFB999LineDataSeries []LineDataSeries
	var ThroughputAvgLineDataSeries []LineDataSeries
	var RequestSuccessLineDataSeries []LineDataSeries

	var ThroughputTotalLineDataSeries []LineDataSeries
	var RequestTotalLineDataSeries []LineDataSeries
	var RequestRangeLineDataSeries []LineDataSeries

	var keyRange = make(map[int]string)

	if len(aggrs) == 0 {
		fmt.Println("No data to analysis1")

		return nil
	}
	if aggrs[0].Operations == nil {
		fmt.Println("No data to analysis2")

		return nil
	}

	if aggrs[0].Operations[0].SingleSizedRequests != nil {
		RequestDelayAvgLineDataSeries = append(RequestDelayAvgLineDataSeries, LineDataSeries{Name: "requests_avg_time(ms)"})
		RequestDelay90LineDataSeries = append(RequestDelay90LineDataSeries, LineDataSeries{Name: "requests_90_time(ms)"})
		RequestDelay99LineDataSeries = append(RequestDelay99LineDataSeries, LineDataSeries{Name: "requests_99_time(ms)"})
		RequestDelay999LineDataSeries = append(RequestDelay999LineDataSeries, LineDataSeries{Name: "requests_999_time(ms)"})
		RequestDelayStdDevLineDataSeries = append(RequestDelayStdDevLineDataSeries, LineDataSeries{Name: "requests_std_dev_time(ms)"})
		RequestDelayMaxLineDataSeries = append(RequestDelayMaxLineDataSeries, LineDataSeries{Name: "requests_max_time(ms)"})
		RequestDelayMinLineDataSeries = append(RequestDelayMinLineDataSeries, LineDataSeries{Name: "requests_min_time(ms)"})
		if aggrs[0].Operations[0].SingleSizedRequests.FirstByte != nil {
			RequestTTFB999LineDataSeries = append(RequestTTFB999LineDataSeries, LineDataSeries{Name: "requests_ttfb999_time(ms)"})
		}
		ThroughputAvgLineDataSeries = append(ThroughputAvgLineDataSeries, LineDataSeries{Name: "throughput average(MiB/s)"})
		ThroughputTotalLineDataSeries = append(ThroughputTotalLineDataSeries, LineDataSeries{Name: "throughput total(MiB/s)"})
		RequestTotalLineDataSeries = append(RequestTotalLineDataSeries, LineDataSeries{Name: "ops total"})
		RequestSuccessLineDataSeries = append(RequestSuccessLineDataSeries,
			LineDataSeries{Name: "requests_completed"}, LineDataSeries{Name: "requests_success"}, LineDataSeries{Name: "requests_failed"})
	} else {
		for i := range aggrs {
			for j := range aggrs[i].Operations {
				if aggrs[i].Operations[j].MultiSizedRequests != nil {
					for k := range aggrs[i].Operations[j].MultiSizedRequests.BySize {
						keyRange[aggrs[i].Operations[j].MultiSizedRequests.BySize[k].MinSize] = aggrs[i].Operations[j].MultiSizedRequests.BySize[k].KeyString()
					}
				}
			}
		}

		fmt.Println("len MultiSizedRequests by size", len(keyRange))
		for _, v := range keyRange {
			RequestDelayAvgLineDataSeries = append(RequestDelayAvgLineDataSeries, LineDataSeries{Name: v})
			RequestDelay90LineDataSeries = append(RequestDelay90LineDataSeries, LineDataSeries{Name: v})
			RequestDelay99LineDataSeries = append(RequestDelay99LineDataSeries, LineDataSeries{Name: v})
			RequestDelay999LineDataSeries = append(RequestDelay999LineDataSeries, LineDataSeries{Name: v})
			RequestDelayStdDevLineDataSeries = append(RequestDelayStdDevLineDataSeries, LineDataSeries{Name: v})
			RequestTTFB999LineDataSeries = append(RequestTTFB999LineDataSeries, LineDataSeries{Name: v})
			ThroughputAvgLineDataSeries = append(ThroughputAvgLineDataSeries, LineDataSeries{Name: v})
			RequestRangeLineDataSeries = append(RequestRangeLineDataSeries, LineDataSeries{Name: v})
		}
		RequestSuccessLineDataSeries = append(RequestSuccessLineDataSeries,
			LineDataSeries{Name: "requests_completed"}, LineDataSeries{Name: "requests_success"}, LineDataSeries{Name: "requests_failed"})

		RequestTotalLineDataSeries = append(RequestTotalLineDataSeries, LineDataSeries{Name: "ops total"})
		ThroughputTotalLineDataSeries = append(ThroughputTotalLineDataSeries, LineDataSeries{Name: "throughput total(MiB/s)"})
	}
	keys := make([]int, 0, len(keyRange))
	for key := range keyRange {
		keys = append(keys, key)
	}
	sort.Ints(keys)
	for _, aggr := range aggrs {
		for _, o := range aggr.Operations {
			if o.SingleSizedRequests != nil {
				reqs := *o.SingleSizedRequests
				RequestDelayAvgLineDataSeries[0].Data = append(RequestDelayAvgLineDataSeries[0].Data, opts.LineData{Value: reqs.DurAvgMillis})
				RequestDelay90LineDataSeries[0].Data = append(RequestDelay90LineDataSeries[0].Data, opts.LineData{Value: reqs.Dur90Millis})
				RequestDelay99LineDataSeries[0].Data = append(RequestDelay99LineDataSeries[0].Data, opts.LineData{Value: reqs.Dur99Millis})
				RequestDelay999LineDataSeries[0].Data = append(RequestDelay999LineDataSeries[0].Data, opts.LineData{Value: reqs.Dur999Millis})
				RequestDelayStdDevLineDataSeries[0].Data = append(RequestDelayStdDevLineDataSeries[0].Data, opts.LineData{Value: reqs.StdDev})
				RequestDelayMaxLineDataSeries[0].Data = append(RequestDelayMaxLineDataSeries[0].Data, opts.LineData{Value: reqs.SlowestMillis})
				RequestDelayMinLineDataSeries[0].Data = append(RequestDelayMinLineDataSeries[0].Data, opts.LineData{Value: reqs.FastestMillis})

				if reqs.FirstByte != nil {
					RequestTTFB999LineDataSeries[0].Data = append(RequestTTFB999LineDataSeries[0].Data, opts.LineData{Value: reqs.FirstByte.P999Millis})
				}
				ThroughputAvgLineDataSeries[0].Data = append(ThroughputAvgLineDataSeries[0].Data, opts.LineData{Value: o.Throughput.AverageBPS / (1 << 20)})
				RequestSuccessLineDataSeries[0].Data = append(RequestSuccessLineDataSeries[0].Data, opts.LineData{Value: o.N})
				RequestSuccessLineDataSeries[1].Data = append(RequestSuccessLineDataSeries[1].Data, opts.LineData{Value: o.N - o.Errors})
				RequestSuccessLineDataSeries[2].Data = append(RequestSuccessLineDataSeries[2].Data, opts.LineData{Value: o.Errors})
				RequestSuccessLineDataSeries[2].Opts = append(RequestSuccessLineDataSeries[2].Opts, charts.WithLineStyleOpts(opts.LineStyle{Color: "red"}))

				RequestTotalLineDataSeries[0].Data = append(RequestTotalLineDataSeries[0].Data, opts.LineData{Value: o.N})
				ThroughputTotalLineDataSeries[0].Data = append(ThroughputTotalLineDataSeries[0].Data, opts.LineData{Value: o.Throughput.AverageBPS / (1 << 20)})

			} else {
				if o.MultiSizedRequests == nil {
					continue
				}
				reqs := *o.MultiSizedRequests
				var NErrors = o.Errors
				for i, k := range keys {
					matched := false
					for _, s := range reqs.BySize {
						if s.KeyString() == keyRange[k] {
							matched = true
							fmt.Println("matched", s.KeyString(), k)
							RequestDelayAvgLineDataSeries[i].Data = append(RequestDelayAvgLineDataSeries[i].Data, opts.LineData{Value: s.DurAvgMillis})
							RequestDelay90LineDataSeries[i].Data = append(RequestDelay90LineDataSeries[i].Data, opts.LineData{Value: s.Dur90Millis})
							RequestDelay99LineDataSeries[i].Data = append(RequestDelay99LineDataSeries[i].Data, opts.LineData{Value: s.Dur99Millis})
							RequestDelay999LineDataSeries[i].Data = append(RequestDelay999LineDataSeries[i].Data, opts.LineData{Value: s.Dur999Millis})
							RequestDelayStdDevLineDataSeries[i].Data = append(RequestDelayStdDevLineDataSeries[i].Data, opts.LineData{Value: s.StdDev})
							if s.FirstByte != nil {
								RequestTTFB999LineDataSeries[i].Data = append(RequestTTFB999LineDataSeries[i].Data, opts.LineData{Value: s.FirstByte.P999Millis})
								NErrors += s.FirstByte.OverSecond
							}
							ThroughputAvgLineDataSeries[i].Data = append(ThroughputAvgLineDataSeries[i].Data, opts.LineData{Value: s.BpsAverage / (1 << 20)})
							RequestRangeLineDataSeries[i].Data = append(RequestRangeLineDataSeries[i].Data, opts.LineData{Value: s.Requests})
							break
						}
					}
					if !matched {
						fmt.Println("not matched", k)
						RequestDelayAvgLineDataSeries[i].Data = append(RequestDelayAvgLineDataSeries[i].Data, opts.LineData{Value: 0})
						RequestDelay90LineDataSeries[i].Data = append(RequestDelay90LineDataSeries[i].Data, opts.LineData{Value: 0})
						RequestDelay99LineDataSeries[i].Data = append(RequestDelay99LineDataSeries[i].Data, opts.LineData{Value: 0})
						RequestDelay999LineDataSeries[i].Data = append(RequestDelay999LineDataSeries[i].Data, opts.LineData{Value: 0})
						RequestTTFB999LineDataSeries[i].Data = append(RequestTTFB999LineDataSeries[i].Data, opts.LineData{Value: 0})
						RequestDelayStdDevLineDataSeries[i].Data = append(RequestDelayStdDevLineDataSeries[i].Data, opts.LineData{Value: 0})
						ThroughputAvgLineDataSeries[i].Data = append(ThroughputAvgLineDataSeries[i].Data, opts.LineData{Value: 0})
						RequestRangeLineDataSeries[i].Data = append(RequestRangeLineDataSeries[i].Data, opts.LineData{Value: 0})
					}
				}
				RequestSuccessLineDataSeries[0].Data = append(RequestSuccessLineDataSeries[0].Data, opts.LineData{Value: o.N})
				RequestSuccessLineDataSeries[1].Data = append(RequestSuccessLineDataSeries[1].Data, opts.LineData{Value: o.N - NErrors})
				RequestSuccessLineDataSeries[2].Data = append(RequestSuccessLineDataSeries[2].Data, opts.LineData{Value: NErrors})
				RequestSuccessLineDataSeries[2].Opts = append(RequestSuccessLineDataSeries[2].Opts, charts.WithLineStyleOpts(opts.LineStyle{Color: "red"}))

				RequestTotalLineDataSeries[0].Data = append(RequestTotalLineDataSeries[0].Data, opts.LineData{Value: o.N})
				ThroughputTotalLineDataSeries[0].Data = append(ThroughputTotalLineDataSeries[0].Data, opts.LineData{Value: o.Throughput.AverageBPS / (1 << 20)})
			}

		}

	}

	lines = append(lines, drawLineChart("Requests Avg", "Requests Avg(ms)", RequestDelayAvgLineDataSeries))
	lines = append(lines, drawLineChart("Requests 90", "Requests 90(ms)", RequestDelay90LineDataSeries))
	lines = append(lines, drawLineChart("Requests 99", "Requests 99(ms)", RequestDelay99LineDataSeries))
	lines = append(lines, drawLineChart("Requests 999", "Requests 999(ms)", RequestDelay999LineDataSeries))
	lines = append(lines, drawLineChart("Requests StdDev", "Requests StdDev(ms)", RequestDelayStdDevLineDataSeries))
	lines = append(lines, drawLineChart("Requests Max", "Requests Max(ms)", RequestDelayMaxLineDataSeries))
	lines = append(lines, drawLineChart("Requests Min", "Requests Min(ms)", RequestDelayMinLineDataSeries))
	lines = append(lines, drawLineChart("Requests TTFB 999", "Requests TTFB 999(ms)", RequestTTFB999LineDataSeries))
	lines = append(lines, drawLineChart("Throughput Avg", "Throughput Avg(MiB/s)", ThroughputAvgLineDataSeries))
	lines = append(lines, drawLineChart("Requests Success", "Requests Success", RequestSuccessLineDataSeries))
	lines = append(lines, drawLineChart("Throughput Total", "Throughput Total(MiB/s)", ThroughputTotalLineDataSeries))
	lines = append(lines, drawLineChart("Requests Total", "Requests Total", RequestTotalLineDataSeries))
	lines = append(lines, drawLineChart("Requests Range", "Requests Range", RequestRangeLineDataSeries))
	return lines
}

func drawLineChart(title, subtitle string, dataSeries []LineDataSeries) *charts.Line {
	// create a new line instance
	line := charts.NewLine()
	// set some global options like Title/Legend/ToolTip or anything else
	line.SetGlobalOptions(
		charts.WithInitializationOpts(opts.Initialization{Theme: types.ThemeRoma}),
		charts.WithTitleOpts(opts.Title{
			Title:    title,
			Subtitle: subtitle,
		}))

	// Put data into instance
	if len(dataSeries) != 0 {
		var x []string
		for i := 0; i < len(dataSeries[0].Data); i++ {
			x = append(x, strconv.Itoa(i))
		}

		line = line.SetXAxis(x)
		for _, series := range dataSeries {
			line = line.AddSeries(series.Name, series.Data, series.Opts...)
		}
		line.SetSeriesOptions(charts.WithLineChartOpts(opts.LineChart{Smooth: opts.Bool(false)}))
	}
	return line
}

type LineDataSeries struct {
	Name string
	Data []opts.LineData
	Opts []charts.SeriesOpts
}

func printAggregation(ctx *cli.Context, o bench.Operations, prefiltered bool, durFn func(time.Duration) time.Duration, wrSegs io.Writer, details bool) aggregate.Aggregated {
	aggr := aggregate.Aggregate(o, aggregate.Options{
		Prefiltered: prefiltered,
		DurFunc:     durFn,
		SkipDur:     ctx.Duration("analyze.skip"),
	})
	if wrSegs != nil {
		for _, ops := range aggr.Operations {
			writeSegs(ctx, wrSegs, o.FilterByOp(ops.Type), !(aggr.Mixed || prefiltered), details)
		}
	}

	if globalJSON {
		b, err := json.MarshalIndent(aggr, "", "  ")
		fatalIf(probe.NewError(err), "Unable to marshal data.")
		if err != nil {
			console.Errorln(err)
		}
		os.Stdout.Write(b)
		return aggr
	}

	if aggr.Mixed {
		printMixedOpAnalysis(ctx, aggr, details)
		return aggr
	}

	for _, ops := range aggr.Operations {
		typ := ops.Type
		console.SetColor("Print", color.New(color.FgHiWhite))
		interval := ctx.String("analyze.interval")
		if interval != "" {
			console.Printf("\n----------------------------------------")
			console.Printf("\nStart Time: %v", o[0].Start)
		}
		console.Println("\n----------------------------------------")

		opo := ops.ObjectsPerOperation
		console.SetColor("Print", color.New(color.FgHiWhite))
		hostsString := ""
		if ops.Hosts > 1 {
			hostsString = fmt.Sprintf(" Hosts: %d.", ops.Hosts)
		}
		if ops.Clients > 1 {
			hostsString = fmt.Sprintf("%s Warp Instances: %d.", hostsString, ops.Clients)
		}
		ran := ops.EndTime.Sub(ops.StartTime).Truncate(time.Second)
		sz := ""
		if ops.SingleSizedRequests != nil && ops.SingleSizedRequests.ObjSize > 0 {
			sz = fmt.Sprintf("Size: %d bytes. ", ops.SingleSizedRequests.ObjSize)
		}
		if opo > 1 {
			if details && ops.Concurrency > 0 {
				console.Printf("Operation: %v (%d). Ran %v. Objects per operation: %d. %vConcurrency: %d.%s\n", typ, ops.N, ran, opo, sz, ops.Concurrency, hostsString)
			} else {
				console.Printf("Operation: %v\n", typ)
			}
		} else {
			if details && ops.Concurrency > 0 {
				console.Printf("Operation: %v (%d). Ran %v. %vConcurrency: %d.%s\n", typ, ops.N, ran, sz, ops.Concurrency, hostsString)
			} else {
				console.Printf("Operation: %v. Concurrency: %v\n", typ, ops.Concurrency)
			}
		}
		if ops.Errors > 0 {
			console.SetColor("Print", color.New(color.FgHiRed))
			console.Println("Errors:", ops.Errors)
			if details {
				console.SetColor("Print", color.New(color.FgWhite))
				console.Println("First Errors:")
				for _, err := range ops.FirstErrors {
					console.Println(" *", err)
				}
				console.Println("")
			}
		}

		if ops.Skipped {
			console.SetColor("Print", color.New(color.FgHiWhite))
			console.Println("Skipping2", typ, "too few samples. Longer benchmark run required for reliable results.")
			continue
		}

		if details {
			printRequestAnalysis(ctx, ops, details)
			console.SetColor("Print", color.New(color.FgHiWhite))
			console.Println("\nThroughput:")
		}
		console.SetColor("Print", color.New(color.FgWhite))
		console.Println("* Average:", ops.Throughput.StringDetails(details))

		if eps := ops.ThroughputByHost; len(eps) > 1 {
			console.SetColor("Print", color.New(color.FgHiWhite))
			console.Println("\nThroughput by host:")

			for _, ep := range ops.HostNames {
				ops := eps[ep]
				console.SetColor("Print", color.New(color.FgWhite))
				console.Print(" * ", ep, ":")
				if !details {
					console.Print(" Avg: ", ops.StringDetails(details), "\n")
				} else {
					console.Print("\n")
				}
				if ops.Errors > 0 {
					console.SetColor("Print", color.New(color.FgHiRed))
					console.Println("Errors:", ops.Errors)
				}
				if details {
					seg := ops.Segmented
					console.SetColor("Print", color.New(color.FgWhite))
					if seg == nil || len(seg.Segments) <= 1 {
						console.Println("Skipping1", typ, "host:", ep, " - Too few samples. Longer benchmark run needed for reliable results.")
						continue
					}
					console.SetColor("Print", color.New(color.FgWhite))
					console.Println("\t- Average: ", ops.StringDetails(false))
					console.Println("\t- Fastest:", aggregate.BPSorOPS(seg.FastestBPS, seg.FastestOPS))
					console.Println("\t- 50% Median:", aggregate.BPSorOPS(seg.MedianBPS, seg.MedianOPS))
					console.Println("\t- Slowest:", aggregate.BPSorOPS(seg.SlowestBPS, seg.SlowestOPS))
				}
			}
		}
		segs := ops.Throughput.Segmented
		dur := time.Millisecond * time.Duration(segs.SegmentDurationMillis)
		console.SetColor("Print", color.New(color.FgHiWhite))
		console.Print("\nThroughput, split into ", len(segs.Segments), " x ", dur, ":\n")
		console.SetColor("Print", color.New(color.FgWhite))
		console.Println(" * Fastest:", aggregate.SegmentSmall{BPS: segs.FastestBPS, OPS: segs.FastestOPS, Start: segs.FastestStart}.StringLong(dur, details))
		console.Println(" * 50% Median:", aggregate.SegmentSmall{BPS: segs.MedianBPS, OPS: segs.MedianOPS, Start: segs.MedianStart}.StringLong(dur, details))
		console.Println(" * Slowest:", aggregate.SegmentSmall{BPS: segs.SlowestBPS, OPS: segs.SlowestOPS, Start: segs.SlowestStart}.StringLong(dur, details))
	}
	return aggr
}

func writeSegs(ctx *cli.Context, wrSegs io.Writer, ops bench.Operations, allThreads, details bool) {
	if wrSegs == nil {
		return
	}
	totalDur := ops.Duration()
	aDur := analysisDur(ctx, totalDur)
	ops.SortByStartTime()
	segs := ops.Segment(bench.SegmentOptions{
		From:           time.Time{},
		PerSegDuration: aDur,
		AllThreads:     allThreads && !ops.HasError(),
	})
	if len(segs) == 0 {
		return
	}

	segs.SortByTime()
	err := segs.CSV(wrSegs)
	errorIf(probe.NewError(err), "Error writing analysis")
	start := segs[0].Start
	wantSegs := len(segs)

	// Write segments per endpoint
	eps := ops.SortSplitByEndpoint()
	epsSorted := stringKeysSorted(eps)
	if details && len(eps) > 1 {
		for _, ep := range epsSorted {
			ops := eps[ep]
			segs := ops.Segment(bench.SegmentOptions{
				From:           start,
				PerSegDuration: aDur,
				AllThreads:     false,
			})
			if len(segs) <= 1 {
				continue
			}
			if len(segs) > wantSegs {
				segs = segs[:wantSegs]
			}
			totals := ops.Total(false)
			if totals.TotalBytes > 0 {
				segs.SortByThroughput()
			} else {
				segs.SortByObjsPerSec()
			}
			segs.SortByTime()
			err := segs.CSV(wrSegs)
			errorIf(probe.NewError(err), "Error writing analysis")
		}
	}
}

func printRequestAnalysis(_ *cli.Context, ops aggregate.Operation, details bool) {
	console.SetColor("Print", color.New(color.FgHiWhite))

	if ops.SingleSizedRequests != nil {
		reqs := *ops.SingleSizedRequests
		// Single type, require one operation per thread.

		console.Print("\nRequests considered: ", reqs.Requests, ":\n")
		console.SetColor("Print", color.New(color.FgWhite))

		if reqs.Skipped {
			console.Println("Not enough requests")
			return
		}

		console.Print(
			" * Avg: ", time.Duration(reqs.DurAvgMillis)*time.Millisecond,
			", 50%: ", time.Duration(reqs.DurMedianMillis)*time.Millisecond,
			", 90%: ", time.Duration(reqs.Dur90Millis)*time.Millisecond,
			", 99%: ", time.Duration(reqs.Dur99Millis)*time.Millisecond,
			", 999%: ", time.Duration(reqs.Dur999Millis)*time.Millisecond,
			", Fastest: ", time.Duration(reqs.FastestMillis)*time.Millisecond,
			", Slowest: ", time.Duration(reqs.SlowestMillis)*time.Millisecond,
			", StdDev: ", time.Duration(reqs.StdDev)*time.Millisecond,
			"\n")

		if reqs.FirstByte != nil {
			console.Println(" * TTFB:", reqs.FirstByte)
		}

		if details && reqs.FirstAccess != nil {
			reqs := reqs.FirstAccess
			console.Print(
				" * First Access: Avg: ", time.Duration(reqs.DurAvgMillis)*time.Millisecond,
				", 50%: ", time.Duration(reqs.DurMedianMillis)*time.Millisecond,
				", 90%: ", time.Duration(reqs.Dur90Millis)*time.Millisecond,
				", 99%: ", time.Duration(reqs.Dur99Millis)*time.Millisecond,
				", 999%: ", time.Duration(reqs.Dur999Millis)*time.Millisecond,
				", Fastest: ", time.Duration(reqs.FastestMillis)*time.Millisecond,
				", Slowest: ", time.Duration(reqs.SlowestMillis)*time.Millisecond,
				", StdDev: ", time.Duration(reqs.StdDev)*time.Millisecond,
				"\n")
			if reqs.FirstByte != nil {
				console.Print(" * First Access TTFB: ", reqs.FirstByte, "\n")
			}
		}
		if details && reqs.LastAccess != nil {
			reqs := reqs.LastAccess
			console.Print(
				" * Last Access: Avg: ", time.Duration(reqs.DurAvgMillis)*time.Millisecond,
				", 50%: ", time.Duration(reqs.DurMedianMillis)*time.Millisecond,
				", 90%: ", time.Duration(reqs.Dur90Millis)*time.Millisecond,
				", 99%: ", time.Duration(reqs.Dur99Millis)*time.Millisecond,
				", 999%: ", time.Duration(reqs.Dur999Millis)*time.Millisecond,
				", Fastest: ", time.Duration(reqs.FastestMillis)*time.Millisecond,
				", Slowest: ", time.Duration(reqs.SlowestMillis)*time.Millisecond,
				", StdDev: ", time.Duration(reqs.StdDev)*time.Millisecond,
				"\n")
			if reqs.FirstByte != nil {
				console.Print(" * Last Access TTFB: ", reqs.FirstByte, "\n")
			}
		}

		var oversecond int
		if reqs.FirstByte != nil {
			oversecond = reqs.FirstByte.OverSecond
		}
		console.SetColor("Print", color.New(color.FgHiWhite))
		console.Println("\nRequest Summary:")
		console.SetColor("Print", color.New(color.FgWhite))
		console.Println(" * TotalRequests:", ops.N,
			", Errors:", ops.Errors,
			", Over second:", oversecond,
			", SLA:", float64(ops.N-ops.Errors-oversecond)*100/float64(ops.N), "%",
			"\n")

		if eps := reqs.ByHost; len(eps) > 1 && details {
			console.SetColor("Print", color.New(color.FgHiWhite))
			console.Println("\nRequests by host:")

			for _, ep := range reqs.HostNames {
				reqs := eps[ep]
				if reqs.Requests <= 1 {
					continue
				}
				console.SetColor("Print", color.New(color.FgWhite))
				console.Println(" *", ep, "-", reqs.Requests, "requests:",
					"\n\t- Avg:", time.Duration(reqs.DurAvgMillis)*time.Millisecond,
					"Fastest:", time.Duration(reqs.FastestMillis)*time.Millisecond,
					"Slowest:", time.Duration(reqs.SlowestMillis)*time.Millisecond,
					"50%:", time.Duration(reqs.DurMedianMillis)*time.Millisecond,
					"90%:", time.Duration(reqs.Dur90Millis)*time.Millisecond,
					"StdDev:", time.Duration(reqs.StdDev)*time.Millisecond)
				if reqs.FirstByte != nil {
					console.Println("\t- First Byte:", reqs.FirstByte)
				}
			}
		}
		return
	}

	// Multi sized
	if ops.MultiSizedRequests == nil {
		console.Fatalln("Neither single-sized nor multi-sized requests found")
	}
	reqs := *ops.MultiSizedRequests
	console.Print("\nRequests considered: ", reqs.Requests, ". Multiple sizes, average ", reqs.AvgObjSize, " bytes:\n")
	console.SetColor("Print", color.New(color.FgWhite))

	if reqs.Skipped {
		console.Println("Not enough requests")
	}

	sizes := reqs.BySize
	var oversecond int
	for _, s := range sizes {

		console.SetColor("Print", color.New(color.FgHiWhite))
		console.Print("\nRequest size ", s.MinSizeString, " -> ", s.MaxSizeString, ". Requests - ", s.Requests, ":\n")
		console.SetColor("Print", color.New(color.FgWhite))

		console.Print("\nRequests considered: ", reqs.Requests, ":\n")
		console.SetColor("Print", color.New(color.FgWhite))

		if reqs.Skipped {
			console.Println("Not enough requests")
			return
		}

		console.Print(
			" * Avg: ", time.Duration(s.DurAvgMillis)*time.Millisecond,
			", 50%: ", time.Duration(s.DurMedianMillis)*time.Millisecond,
			", 90%: ", time.Duration(s.Dur90Millis)*time.Millisecond,
			", 99%: ", time.Duration(s.Dur99Millis)*time.Millisecond,
			", 999%: ", time.Duration(s.Dur999Millis)*time.Millisecond,
			", StdDev: ", time.Duration(s.StdDev)*time.Millisecond,
			"\n")

		console.Print(""+
			" * Throughput: Average: ", bench.Throughput(s.BpsAverage),
			", 50%: ", bench.Throughput(s.BpsMedian),
			", 90%: ", bench.Throughput(s.Bps90),
			", 99%: ", bench.Throughput(s.Bps99),
			", Fastest: ", bench.Throughput(s.BpsFastest),
			", Slowest: ", bench.Throughput(s.BpsSlowest),
			"\n")

		if s.FirstByte != nil {
			console.Println(" * TTFB:", s.FirstByte)
			oversecond += s.FirstByte.OverSecond
		}

		if s.FirstAccess != nil {
			s := s.FirstAccess
			console.Print(""+
				" * First Access: Average: ", bench.Throughput(s.BpsAverage),
				", 50%: ", bench.Throughput(s.BpsMedian),
				", 90%: ", bench.Throughput(s.Bps90),
				", 99%: ", bench.Throughput(s.Bps99),
				", Fastest: ", bench.Throughput(s.BpsFastest),
				", Slowest: ", bench.Throughput(s.BpsSlowest),
				"\n")
			if s.FirstByte != nil {
				console.Print(" * First Access TTFB: ", s.FirstByte, "\n")
			}
		}

	}

	console.SetColor("Print", color.New(color.FgHiWhite))
	console.Println("\nRequest Summary:")
	console.SetColor("Print", color.New(color.FgWhite))
	console.Println(" * TotalRequests:", ops.N,
		", Errors:", ops.Errors,
		", Over second:", oversecond,
		", SLA:", float64(ops.N-ops.Errors-oversecond)*100/float64(ops.N), "%",
		"\n")
	if eps := reqs.ByHost; len(eps) > 1 && details {
		console.SetColor("Print", color.New(color.FgHiWhite))
		console.Println("\nRequests by host:")

		for _, ep := range ops.HostNames {
			s := eps[ep]
			if s.Requests <= 1 {
				continue
			}
			console.SetColor("Print", color.New(color.FgWhite))
			console.Println(" *", ep, "-", s.Requests, "requests:",
				"\n\t- Avg:", bench.Throughput(s.BpsAverage),
				"Fastest:", bench.Throughput(s.BpsFastest),
				"Slowest:", bench.Throughput(s.BpsSlowest),
				"50%:", bench.Throughput(s.BpsMedian),
				"90%:", bench.Throughput(s.Bps90))
			if s.FirstByte != nil {
				console.Println(" * TTFB:", s.FirstByte)
			}
		}
	}
}

// analysisDur returns the analysis duration or 0 if un-parsable.
func analysisDur(ctx *cli.Context, total time.Duration) time.Duration {
	dur := ctx.String("analyze.dur")
	if dur == "" {
		if total == 0 {
			return 0
		}
		// Find appropriate duration
		// We want the smallest segmentation duration that produces at most this number of segments.
		const wantAtMost = 400

		// Standard durations to try:
		stdDurations := []time.Duration{time.Second, 5 * time.Second, 15 * time.Second, time.Minute, 5 * time.Minute, 15 * time.Minute, time.Hour, 3 * time.Hour}
		for _, d := range stdDurations {
			dur = d.String()
			if total/d <= wantAtMost {
				break
			}
		}
	}
	d, err := time.ParseDuration(dur)
	fatalIf(probe.NewError(err), "Invalid -analyze.dur value")
	return d
}

func checkAnalyze(ctx *cli.Context) {
	if analysisDur(ctx, time.Minute) == 0 {
		err := errors.New("-analyze.dur cannot be 0")
		fatal(probe.NewError(err), "Invalid -analyze.dur value")
	}
}

// stringKeysSorted returns the keys as a sorted string slice.
func stringKeysSorted[K string, V any](m map[K]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, string(k))
	}
	sort.Strings(keys)
	return keys
}
