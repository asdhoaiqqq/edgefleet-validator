package edgefleet

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
)

// This file pins the distinction between a clean end of input and a reader
// failure mid-record. At clean EOF the last unterminated (but otherwise
// complete) line is still a record; after a non-EOF read error only lines
// whose newline has already been received are records, even when the line and
// the error came back from the same Read. The unread-looking tail must never
// become an event, watermark or idle declaration, and so can never produce a
// late notice or a window result; the caller gets the reader's own error
// through errors.Is.

// collectedLine is one line handed to the readAggregateLines callback.
type collectedLine struct {
	text string
	no   int
}

func collectLines(t *testing.T, r io.Reader) ([]collectedLine, error) {
	t.Helper()
	var got []collectedLine
	err := readAggregateLines(r, func(line string, lineNo int) error {
		got = append(got, collectedLine{text: line, no: lineNo})
		return nil
	})
	return got, err
}

// TestReadAggregateLinesEOFVsReadError checks the primitive directly: clean
// EOF delivers one final unterminated line; a non-EOF error never does.
func TestReadAggregateLinesEOFVsReadError(t *testing.T) {
	t.Run("clean EOF keeps unterminated final line", func(t *testing.T) {
		got, err := collectLines(t, strings.NewReader("a\nb\r\nc"))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		want := []collectedLine{{"a", 1}, {"b", 2}, {"c", 3}}
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("lines = %v, want %v (CR stripped, final line kept)", got, want)
		}
	})
	t.Run("EOF exactly on a newline adds no blank line", func(t *testing.T) {
		got, err := collectLines(t, strings.NewReader("a\n"))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		want := []collectedLine{{"a", 1}}
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("lines = %v, want %v", got, want)
		}
	})
	t.Run("blank physical lines count", func(t *testing.T) {
		got, err := collectLines(t, strings.NewReader("\n\nthird\n"))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		want := []collectedLine{{"", 1}, {"", 2}, {"third", 3}}
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("lines = %v, want %v", got, want)
		}
	})
	t.Run("read error drops the unterminated tail", func(t *testing.T) {
		input := "first\nsecond\ntail-without-newline"
		failAt := len("first\nsecond\n") + len("tail-without-newline")
		got, err := collectLines(t, &failingAfterReader{
			data: []byte(input), failAt: failAt, err: errSentinelChunkedRead,
		})
		if !errors.Is(err, errSentinelChunkedRead) {
			t.Fatalf("error = %v, want the reader's original error via errors.Is", err)
		}
		want := []collectedLine{{"first", 1}, {"second", 2}}
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("lines = %v, want only newline-terminated lines %v", got, want)
		}
	})
	t.Run("error with bytes still completes co-delivered lines", func(t *testing.T) {
		// One Read hands back a complete line, an unterminated tail and the
		// error together. Only the complete line is delivered.
		input := "first\ndangling"
		got, err := collectLines(t, &failingAfterReader{
			data: []byte(input), failAt: len(input),
			err: errSentinelChunkedRead, errOnBoundary: true,
		})
		if !errors.Is(err, errSentinelChunkedRead) {
			t.Fatalf("error = %v, want reader error", err)
		}
		want := []collectedLine{{"first", 1}}
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("lines = %v, want %v", got, want)
		}
	})
	t.Run("callback error wins over a co-arriving read error", func(t *testing.T) {
		stop := errors.New("callback stop")
		var delivered int
		r := &failingAfterReader{
			data:   []byte("first\nrest"),
			failAt: len("first\nrest"),
			err:    errSentinelChunkedRead, errOnBoundary: true,
		}
		err := readAggregateLines(r, func(string, int) error {
			delivered++
			return stop
		})
		if !errors.Is(err, stop) {
			t.Fatalf("error = %v, want callback error to take precedence", err)
		}
		if delivered != 1 {
			t.Fatalf("callback ran %d times, want 1", delivered)
		}
	})
	t.Run("tail is dropped byte by byte too", func(t *testing.T) {
		input := "first\nsecond\ndangling"
		failAt := len(input)
		got, err := collectLines(t, &failingAfterReader{
			data: []byte(input), failAt: failAt,
			err: errSentinelChunkedRead, chunk: 1, errOnBoundary: true,
		})
		if !errors.Is(err, errSentinelChunkedRead) {
			t.Fatalf("error = %v, want reader error", err)
		}
		want := []collectedLine{{"first", 1}, {"second", 2}}
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("lines = %v, want %v", got, want)
		}
	})
}

// TestReadAggregateErrorUnterminatedTailIsNotARecord drives the exact scenario
// from the fix: length-1000 windows, event key=k time=100 value=2 with a
// newline, then a time=1000 watermark JSON without a newline, and the reader
// fails. The watermark must not close [0,1000); a malformed tail must not be
// reported as an input error either -- the read failure is what surfaces.
func TestReadAggregateErrorUnterminatedTailIsNotARecord(t *testing.T) {
	event := `{"type":"event","key":"k","time":100,"value":2}` + "\n"
	watermark := `{"type":"watermark","time":1000}`

	cases := []struct {
		name string
		tail string
	}{
		{"complete-looking watermark tail", watermark},
		{"truncated watermark tail", `{"type":"watermark","time":1000`},
		{"garbage tail", `{"type":wate`},
	}
	deliveries := []struct {
		name           string
		errOnBoundary  bool
		chunk          int
		errorAfterTail bool
	}{
		{"error on the next read", false, 0, true},
		{"tail bytes and error in one read", true, 1 << 20, false},
		{"one byte per read, error with last byte", true, 1, false},
	}
	for _, tc := range cases {
		for _, dl := range deliveries {
			name := tc.name + "/" + dl.name
			t.Run(name, func(t *testing.T) {
				input := event + tc.tail
				var r io.Reader
				if dl.errorAfterTail {
					// The read carrying the tail succeeds; the following read fails.
					r = &failingAfterReader{data: []byte(input), failAt: len(input), err: errSentinelChunkedRead}
				} else {
					r = &failingAfterReader{data: []byte(input), failAt: len(input), err: errSentinelChunkedRead, errOnBoundary: dl.errOnBoundary, chunk: dl.chunk}
				}
				var out, late bytes.Buffer
				err := RunAggregate(r, 1000, &out, &late)
				if !errors.Is(err, errSentinelChunkedRead) {
					t.Fatalf("error = %T %v, want the reader's original error via errors.Is", err, err)
				}
				if _, ok := err.(*InputError); ok {
					t.Fatalf("read failure must not be masked by a JSON/input error: %v", err)
				}
				if out.String() != "" {
					t.Fatalf("unterminated watermark must not close a window, stdout = %q", out.String())
				}
				if late.String() != "" {
					t.Fatalf("unterminated tail must not produce late notices: %q", late.String())
				}
			})
		}
	}
}

// TestReadAggregateErrorTerminatedRecordSameReadStillProcessed: when the
// failing Read also delivers the watermark's newline, the window must be
// emitted first, in order, and only then does the run fail with the read
// error.
func TestReadAggregateErrorTerminatedRecordSameReadStillProcessed(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":100,"value":2}`,
		`{"type":"watermark","time":1000}`,
		``,
	}, "\n") // ends with the watermark's newline
	wantWindow := `{"key":"k","start":0,"end":1000,"count":1,"sum":2}` + "\n"

	check := func(t *testing.T, r io.Reader) {
		var out, late bytes.Buffer
		err := RunAggregate(r, 1000, &out, &late)
		if !errors.Is(err, errSentinelChunkedRead) {
			t.Fatalf("error = %v, want reader error after processing complete lines", err)
		}
		if out.String() != wantWindow {
			t.Fatalf("window closed by the newline-terminated watermark must be emitted:\n got: %q\nwant: %q", out.String(), wantWindow)
		}
		if late.String() != "" {
			t.Fatalf("unexpected late notices: %q", late.String())
		}
	}

	t.Run("newline and error in the same read", func(t *testing.T) {
		check(t, &failingAfterReader{
			data: []byte(input), failAt: len(input),
			err: errSentinelChunkedRead, errOnBoundary: true,
		})
	})
	t.Run("one byte per read, last byte with error", func(t *testing.T) {
		check(t, &failingAfterReader{
			data: []byte(input), failAt: len(input),
			err: errSentinelChunkedRead, errOnBoundary: true, chunk: 1,
		})
	})
}

// TestReadAggregateErrorUnterminatedLateEventMakesNoNoise: a tail event that
// the current watermark would judge late must not produce a late-event notice
// when the read fails before its newline arrives.
func TestReadAggregateErrorUnterminatedLateEventMakesNoNoise(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"watermark","time":500}`, // line 1 complete
		`{"type":"event","key":"k","time":100,"value":2}`,
	}, "\n") // line 2 unterminated
	var out, late bytes.Buffer
	err := RunAggregate(&failingAfterReader{
		data: []byte(input), failAt: len(input),
		err: errSentinelChunkedRead, errOnBoundary: true,
	}, 1000, &out, &late)
	if !errors.Is(err, errSentinelChunkedRead) {
		t.Fatalf("error = %v, want reader error", err)
	}
	if late.String() != "" {
		t.Fatalf("unterminated late event must not yield a late notice: %q", late.String())
	}
	if out.String() != "" {
		t.Fatalf("no window results possible, got %q", out.String())
	}
}

// TestReadAggregateErrorUnterminatedIdleDoesNotCloseWindows: an idle
// declaration whose newline never arrives must not move the partitioned
// effective watermark or close anything.
func TestReadAggregateErrorUnterminatedIdleDoesNotCloseWindows(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":100,"value":2,"partition":0}`,
		`{"type":"watermark","time":1000,"partition":0}`,
		`{"type":"idle","partition":1}`, // would make the effective watermark 1000
	}, "\n")
	var out, late bytes.Buffer
	err := RunAggregatePartitionedSliding(&failingAfterReader{
		data: []byte(input), failAt: len(input),
		err: errSentinelChunkedRead, errOnBoundary: true,
	}, 1000, 1000, 2, &out, &late)
	if !errors.Is(err, errSentinelChunkedRead) {
		t.Fatalf("error = %v, want reader error", err)
	}
	if out.String() != "" {
		t.Fatalf("unterminated idle declaration must not close [0,1000): %q", out.String())
	}

	// Control: the same idle record WITH a newline delivered together with
	// the read error closes the window before the failure surfaces.
	terminated := input + "\n"
	var out2, late2 bytes.Buffer
	err2 := RunAggregatePartitionedSliding(&failingAfterReader{
		data: []byte(terminated), failAt: len(terminated),
		err: errSentinelChunkedRead, errOnBoundary: true,
	}, 1000, 1000, 2, &out2, &late2)
	if !errors.Is(err2, errSentinelChunkedRead) {
		t.Fatalf("control error = %v, want reader error", err2)
	}
	want := `{"key":"k","start":0,"end":1000,"count":1,"sum":2}` + "\n"
	if out2.String() != want {
		t.Fatalf("newline-terminated idle record must close the window first:\n got: %q\nwant: %q", out2.String(), want)
	}
}

// TestReadAggregateErrorEarlierRecordOrOutputFailureKept: a record error or an
// output write error on a complete line delivered by the same Read that
// carries the read failure must remain the reported failure; the read error,
// learned only afterwards, must not overwrite it.
func TestReadAggregateErrorEarlierRecordOrOutputFailureKept(t *testing.T) {
	t.Run("record error on a complete line wins", func(t *testing.T) {
		input := `{"type":"bogus"}` + "\n" + `{"type":"watermark","time":1000}`
		var out, late bytes.Buffer
		err := RunAggregate(&failingAfterReader{
			data: []byte(input), failAt: len(input),
			err: errSentinelChunkedRead, errOnBoundary: true,
		}, 1000, &out, &late)
		var ie *InputError
		if !errors.As(err, &ie) || ie.Line != 1 {
			t.Fatalf("error = %v, want *InputError on physical line 1", err)
		}
		if errors.Is(err, errSentinelChunkedRead) {
			t.Fatalf("the record error must replace the later read error: %v", err)
		}
	})
	t.Run("output write error on a complete line wins", func(t *testing.T) {
		input := strings.Join([]string{
			`{"type":"event","key":"k","time":100,"value":2}`,
			`{"type":"watermark","time":1000}`,
			`{"type":"event","key":"k","time":900,"value":3}`, // unterminated tail
		}, "\n")
		var late bytes.Buffer
		err := RunAggregate(&failingAfterReader{
			data: []byte(input), failAt: len(input),
			err: errSentinelChunkedRead, errOnBoundary: true,
		}, 1000, &errOnlyWriter{err: errSentinelOutput}, &late)
		var oe *OutputError
		if !errors.As(err, &oe) {
			t.Fatalf("error = %v, want *OutputError", err)
		}
		if !errors.Is(err, errSentinelOutput) {
			t.Fatalf("errors.Is must reach the writer error: %v", err)
		}
		if errors.Is(err, errSentinelChunkedRead) {
			t.Fatalf("the output error must replace the read error learned after it: %v", err)
		}
	})
}

// TestReadAggregateCleanEOFContractStillHolds summarizes the behavior that had
// to stay byte-identical through the reader rewrite: a valid final record
// without a trailing newline processes at clean EOF, while a damaged final
// record is an input error carrying its physical line number.
func TestReadAggregateCleanEOFContractStillHolds(t *testing.T) {
	t.Run("valid unterminated final record processes", func(t *testing.T) {
		input := `{"type":"event","key":"k","time":100,"value":2}` + "\n" +
			`{"type":"watermark","time":1000}`
		var out, late bytes.Buffer
		if err := RunAggregate(strings.NewReader(input), 1000, &out, &late); err != nil {
			t.Fatalf("clean EOF: %v", err)
		}
		want := `{"key":"k","start":0,"end":1000,"count":1,"sum":2}` + "\n"
		if out.String() != want {
			t.Fatalf("got %q, want %q", out.String(), want)
		}
	})
	t.Run("damaged unterminated final record is a line-numbered input error", func(t *testing.T) {
		input := `{"type":"event","key":"k","time":100,"value":2}` + "\n" +
			`{"type":"watermark","time":1000`
		var out, late bytes.Buffer
		err := RunAggregate(strings.NewReader(input), 1000, &out, &late)
		var ie *InputError
		if !errors.As(err, &ie) || ie.Line != 2 {
			t.Fatalf("error = %v, want *InputError on physical line 2", err)
		}
		if !strings.Contains(ie.Reason, "invalid JSON") {
			t.Fatalf("reason = %q, want invalid JSON", ie.Reason)
		}
		if out.String() != "" {
			t.Fatalf("nothing may be emitted: %q", out.String())
		}
	})
}
