package edgefleet

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"
)

// This file guards the line collector's asymptotic behavior: splitting a
// long physical record into lines must cost work roughly linear in the
// cumulative input bytes and the number of reads, no matter how small the
// reads are. A collector that rescans the pending tail after every Read is
// quadratic in the line length when a long record arrives byte by byte; the
// tests below use lines large enough that such a regression cannot finish
// within a generous deadline, while the linear collector finishes in well
// under a second.

// byteAtATimeReader serves its whole stream one byte per Read, the worst
// case for a line collector that rescans its pending tail.
type byteAtATimeReader struct {
	data []byte
	pos  int
}

func (r *byteAtATimeReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	p[0] = r.data[r.pos]
	r.pos++
	return 1, nil
}

// runWithDeadline runs fn on a goroutine and fails the test if it does not
// return within the (deliberately generous) deadline.
func runWithDeadline(t *testing.T, deadline time.Duration, fn func() error) error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- fn() }()
	select {
	case err := <-done:
		return err
	case <-time.After(deadline):
		t.Fatalf("aggregation did not finish within %v: per-line work must grow linearly, not quadratically, with the line length", deadline)
		return nil
	}
}

// TestAggregateLongLineByteByByteStaysLinear feeds a record with a
// multi-megabyte multibyte key one byte per Read. The quadratic regression
// this guards against needs minutes here; the linear collector needs a
// fraction of a second, so the two-minute deadline cannot flake. The key
// (including its Chinese characters) and the window result must survive
// intact.
func TestAggregateLongLineByteByByteStaysLinear(t *testing.T) {
	key := "长传感器记录-" + strings.Repeat("温湿度读数", 600000) + "-甲" // ~9 MB of key
	record := fmt.Sprintf(`{"type":"event","key":%q,"time":100,"value":2}`, key)
	input := record + "\n" + `{"type":"watermark","time":1000}` + "\n"
	want := mustMarshalWindow(t, AggregateResult{Key: key, Start: 0, End: 1000, Count: 1, Sum: 2})

	var out, late bytes.Buffer
	err := runWithDeadline(t, 2*time.Minute, func() error {
		return RunAggregate(&byteAtATimeReader{data: []byte(input)}, 1000, &out, &late)
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.String() != want {
		t.Fatalf("window output mismatch for the byte-by-byte long line:\n got: %.120q...\nwant: %.120q...", out.String(), want)
	}
	if late.String() != "" {
		t.Fatalf("unexpected late notices: %q", late.String())
	}
}

// TestAggregateAlternatingLongShortChunkSizes delivers a stream that
// alternates long and short records with alternating read sizes (1 byte,
// then 4 KiB, then 1 byte, ...), so a read may carry the tail of a short
// record together with the start of a long one. The result must be
// byte-identical to the one-shot reference.
func TestAggregateAlternatingLongShortChunkSizes(t *testing.T) {
	sc := buildLongRecordScenario(t, 64*1024)

	// One-shot reference.
	refOut, refLate, err := runFixedOnce(t, strings.NewReader(sc.input))
	if err != nil {
		t.Fatalf("one-shot: unexpected error: %v", err)
	}

	// Alternating read sizes: 1, 4096, 1, 4096, ... bytes per read.
	var cuts []int
	at := 0
	size := 1
	for at+size < len(sc.input) {
		at += size
		cuts = append(cuts, at)
		if size == 1 {
			size = 4096
		} else {
			size = 1
		}
	}
	out, late, err := runFixedOnce(t, newChunkReader(sc.input, cuts...))
	if err != nil {
		t.Fatalf("alternating chunk sizes: unexpected error: %v", err)
	}
	if out != refOut {
		t.Fatalf("alternating chunk sizes changed window output:\n got: %.200q\nwant: %.200q", out, refOut)
	}
	if late != refLate {
		t.Fatalf("alternating chunk sizes changed late notices:\n got: %q\nwant: %q", late, refLate)
	}
}
