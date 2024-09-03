/*
 * Warp (C) 2019-2023 MinIO, Inc.
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

package bench

import (
	"bufio"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/minio/pkg/v2/console"
)

type Collector struct {
	rcv   chan Operation
	rcvWg sync.WaitGroup
	extra []chan<- Operation
	// The mutex protects the ops above.
	// Once ops have been added, they should no longer be modified.
	opsMu sync.Mutex

	clientID     string
	fileName     string
	file         *os.File
	zstdEncoder  *zstd.Encoder
	bufferWriter *bufio.Writer
	opIndex      atomic.Int64
}

func NewCollector(clientID, fileName string) *Collector {
	fileName = fileName + ".csv.zst"
	f, err := os.Create(fileName)
	if err != nil {
		console.Fatal("Unable to write benchmark data:", err)
	}
	enc, err := zstd.NewWriter(f, zstd.WithEncoderLevel(zstd.SpeedBetterCompression))
	if err != nil {
		console.Fatal("Unable to create zstd encoder: %v", err)
	}

	bw := bufio.NewWriter(enc)
	_, err = bw.WriteString("idx\tthread\top\tclient_id\tn_objects\tbytes\tendpoint\tfile\terror\tstart\tfirst_byte\tend\tduration_ns\n")
	if err != nil {
		console.Fatal("Unable to write csv header:", err)
	}

	r := &Collector{
		rcv: make(chan Operation, 10000),

		clientID:     clientID,
		fileName:     fileName,
		file:         f,
		zstdEncoder:  enc,
		bufferWriter: bw,
	}
	r.rcvWg.Add(1)
	go func() {
		defer r.rcvWg.Done()
		for op := range r.rcv {
			for _, ch := range r.extra {
				ch <- op
			}
			r.opsMu.Lock()
			err := r.writeOp(op)
			if err != nil {
				console.Fatal("Unable to write operation:", err)
			}
			r.opsMu.Unlock()
		}
	}()
	return r
}

func (c *Collector) writeOp(op Operation) error {
	var ttfb string
	if op.FirstByte != nil {
		ttfb = op.FirstByte.Format(time.RFC3339Nano)
	}
	_, err := fmt.Fprintf(c.bufferWriter,
		"%d\t%d\t%s\t%s\t%d\t%d\t%s\t%s\t%s\t%s\t%s\t%s\t%d\n",
		c.opIndex.Add(1)-1, op.Thread, op.OpType, c.clientID, op.ObjPerOp, op.Size, csvEscapeString(op.Endpoint), op.File, csvEscapeString(op.Err), op.Start.Format(time.RFC3339Nano), ttfb, op.End.Format(time.RFC3339Nano), op.End.Sub(op.Start)/time.Nanosecond)
	if err != nil {
		return err
	}
	return nil
}

// NewNullCollector collects operations, but discards them.
func NewNullCollector() *Collector {
	r := &Collector{
		rcv: make(chan Operation, 10000),
	}
	r.rcvWg.Add(1)
	go func() {
		defer r.rcvWg.Done()
		for op := range r.rcv {
			for _, ch := range r.extra {
				ch <- op
			}
		}
	}()
	return r
}

// AutoTerm will check if throughput is within 'threshold' (0 -> ) for wantSamples,
// when the current operations are split into 'splitInto' segments.
// The minimum duration for the calculation can be set as well.
// Segment splitting may cause less than this duration to be used.
func (c *Collector) AutoTerm(ctx context.Context, op string, threshold float64, wantSamples, splitInto int, minDur time.Duration) context.Context {
	return context.Background()
}

func (c *Collector) Receiver() chan<- Operation {
	return c.rcv
}

func (c *Collector) Close() chan Operation {
	close(c.rcv)
	c.rcvWg.Wait()
	for _, ch := range c.extra {
		close(ch)
	}
	if c.file != nil {
		c.bufferWriter.Flush()
		c.zstdEncoder.Close()
		c.file.Close()
	}
	ops := make(chan Operation, 10000)
	go sendFile(c.fileName, ops)
	return ops
}

func sendFile(fileName string, ops chan<- Operation) {
	defer close(ops)

	f, err := os.Open(fileName)
	if err != nil {
		console.Fatalf("Unable to open %s: %v", fileName, err)
	}
	defer f.Close()

	decoder, err := zstd.NewReader(f)
	if err != nil {
		console.Fatalf("Unable to create zstd decoder: %v", err)
	}
	defer decoder.Close()

	csvReader := csv.NewReader(decoder)
	csvReader.Comma = '\t'
	csvReader.ReuseRecord = true
	csvReader.Comment = '#'
	header, err := csvReader.Read()
	if err != nil {
		console.Fatalf("Unable to read header: %v", err)
	}
	fieldIdx := make(map[string]int)
	for i, s := range header {
		fieldIdx[s] = i
	}

	for {
		values, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			console.Fatalf("Unable to read line: %v", err)
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
			console.Fatalf("Unable to parse start time: %v", err)
		}
		var ttfb *time.Time
		if fb := values[fieldIdx["first_byte"]]; fb != "" {
			t, err := time.Parse(time.RFC3339Nano, fb)
			if err != nil {
				console.Fatalf("Unable to parse first byte time: %v", err)
			}
			ttfb = &t
		}
		end, err := time.Parse(time.RFC3339Nano, values[fieldIdx["end"]])
		if err != nil {
			console.Fatalf("Unable to parse end time: %v", err)
		}
		size, err := strconv.ParseInt(values[fieldIdx["bytes"]], 10, 64)
		if err != nil {
			console.Fatalf("Unable to parse size: %v", err)
		}
		thread, err := strconv.ParseUint(values[fieldIdx["thread"]], 10, 16)
		if err != nil {
			console.Fatalf("Unable to parse thread: %v", err)
		}
		objs, err := strconv.ParseInt(values[fieldIdx["n_objects"]], 10, 64)
		if objs != 1 {
			fmt.Fprintf(os.Stdout,
				"Skipping operation with multiple objects: %s\n", values[fieldIdx["op"]])
			continue
		}
		if err != nil {
			console.Fatalf("Unable to parse objects: %v", err)
		}
		var endpoint, clientID string
		if idx, ok := fieldIdx["endpoint"]; ok {
			endpoint = values[idx]
		}
		if idx, ok := fieldIdx["client_id"]; ok {
			clientID = values[idx]
		}
		file := values[fieldIdx["file"]]

		op := Operation{
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

		ops <- op
	}
}
