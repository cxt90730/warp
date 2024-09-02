package main

import (
	"context"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"github.com/influxdata/influxdb-client-go/v2/api/http"
	"github.com/klauspost/compress/zstd"
	"github.com/minio/warp/pkg/bench"
)

func main() {
	from := flag.String("from", "./test.csv.zst", "local file path")
	to := flag.String("to", "http://8POoLz_oFQj_9o41LL1AHiiTIdEC-kHf2L-QGLWENy8u-mXSaqrHS7SM7kJeKcvmiCB7rVnfSswaNSA-wyV4sQ==@127.0.0.1:8086/influxbucket/org",
		"influxdb url")
	flag.Parse()

	influxClient := getInfluxClient(*to)
	defer influxClient.Flush()

	reader, closer := getFileReader(*from)
	defer closer.Close()

	measurementName := strings.TrimSuffix(filepath.Base(*from), ".zst")
	measurementName = strings.TrimSuffix(measurementName, ".csv")
	err := migrate2InfluxDB(reader, influxClient, measurementName)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to migrate to influxdb: %s\n", err)
		os.Exit(1)
	}
}

func parseInfluxURL(s string) (*url.URL, error) {
	u, err := url.Parse(s)
	if err != nil {
		return nil, err
	}
	switch u.Scheme {
	case "":
		return nil, errors.New("influxdb: no scheme specified (http/https)")
	case "http", "https":
	default:
		return nil, fmt.Errorf("influxdb: unknown scheme %s - must be http/https", u.Scheme)
	}
	path := strings.Split(strings.TrimPrefix(u.Path, "/"), "/")
	if len(path) != 2 {
		return nil, fmt.Errorf("influxdb: unexpected path. Want 'bucket/org', got '%s'", strings.TrimPrefix(u.Path, "/"))
	}
	if len(path[0]) == 0 {
		return nil, errors.New("influxdb: empty bucket specified")
	}
	if len(u.RawQuery) > 0 {
		_, err = url.ParseQuery(u.RawQuery)
		if err != nil {
			return nil, err
		}
	}

	// org can be empty
	// token can be empty
	return u, nil
}

func getInfluxClient(influxUrl string) api.WriteAPI {
	u, err := parseInfluxURL(influxUrl)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to parse influxdb parameter: %s\n", err)
		os.Exit(1)
	}

	token := ""
	if u.User != nil {
		token = u.User.Username()
	}

	// Create a new client using an InfluxDB server base URL and an authentication token
	serverURL := u.Scheme + "://" + u.Host
	client := influxdb2.NewClientWithOptions(serverURL, token,
		influxdb2.DefaultOptions().SetMaxRetryTime(1000).SetMaxRetries(2))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ok, err := client.Ping(ctx)
	if !ok {
		fmt.Fprintf(os.Stderr, "Unable to reach influxdb: %s\n", err)
		os.Exit(1)
	}

	path := strings.Split(strings.TrimPrefix(u.Path, "/"), "/")
	writeAPI := client.WriteAPI(path[1], path[0])
	writeAPI.SetWriteFailedCallback(func(_ string, err http.Error, _ uint) bool {
		fmt.Fprintf(os.Stderr, "Unable to write to influxdb: %v\n", err)
		return false
	})
	return writeAPI
}

func getFileReader(path string) (reader io.Reader, closer io.Closer) {
	f, err := os.Open(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to open file %s: %s\n", path, err)
		os.Exit(1)
	}

	if strings.HasSuffix(path, ".zst") {
		decoder, err := zstd.NewReader(f)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Unable to create zstd decoder: %s\n", err)
			os.Exit(1)
		}
		return decoder, f
	} else {
		return f, f
	}
}

func migrate2InfluxDB(reader io.Reader,
	influxClient api.WriteAPI, measurementName string) error {

	cr := csv.NewReader(reader)
	cr.Comma = '\t'
	cr.ReuseRecord = true
	cr.Comment = '#'
	header, err := cr.Read()
	if err != nil {
		return err
	}
	fieldIdx := make(map[string]int)
	for i, s := range header {
		fieldIdx[s] = i
	}

	count := 0
	for {
		values, err := cr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if len(values) == 0 {
			continue
		}

		opErr := values[fieldIdx["error"]]
		if opErr != "" {
			fmt.Fprintf(os.Stdout, "Skipping operation with error: %s\n", opErr)
			continue
		}

		start, err := time.Parse(time.RFC3339Nano, values[fieldIdx["start"]])
		if err != nil {
			return err
		}
		var ttfb *time.Time
		if fb := values[fieldIdx["first_byte"]]; fb != "" {
			t, err := time.Parse(time.RFC3339Nano, fb)
			if err != nil {
				return err
			}
			ttfb = &t
		}
		end, err := time.Parse(time.RFC3339Nano, values[fieldIdx["end"]])
		if err != nil {
			return err
		}
		size, err := strconv.ParseInt(values[fieldIdx["bytes"]], 10, 64)
		if err != nil {
			return err
		}
		thread, err := strconv.ParseUint(values[fieldIdx["thread"]], 10, 16)
		if err != nil {
			return err
		}
		objs, err := strconv.ParseInt(values[fieldIdx["n_objects"]], 10, 64)
		if objs != 1 {
			fmt.Fprintf(os.Stdout,
				"Skipping operation with multiple objects: %s\n", values[fieldIdx["op"]])
			continue
		}
		if err != nil {
			return err
		}
		var endpoint, clientID string
		if idx, ok := fieldIdx["endpoint"]; ok {
			endpoint = values[idx]
		}
		if idx, ok := fieldIdx["client_id"]; ok {
			clientID = values[idx]
		}
		file := values[fieldIdx["file"]]

		op := bench.Operation{
			OpType:    values[fieldIdx["op"]],
			ObjPerOp:  int(objs),
			Start:     start,
			FirstByte: ttfb,
			End:       end,
			Size:      size,
			File:      file,
			Thread:    uint16(thread),
			Endpoint:  endpoint,
			ClientID:  clientID,
		}

		point := influxdb2.NewPointWithMeasurement(measurementName)
		point.AddTag("op", op.OpType)
		point.AddField("start", op.Start.UnixNano())
		if op.FirstByte != nil {
			point.AddField("ttfb", op.FirstByte.Sub(op.Start).Nanoseconds())
		}
		point.AddField("end", op.End.UnixNano())
		point.AddField("size", op.Size)

		point.AddField("duration", op.End.Sub(op.Start).Nanoseconds())
		point.SetTime(op.Start)

		influxClient.WritePoint(point)

		count++
		if count%10_000_000 == 0 {
			fmt.Printf("Processed %d records\n", count)
			// os.Exit(0)
		}
	}
	return nil
}
