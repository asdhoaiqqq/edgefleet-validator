package edgefleet

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
)

// errOnlyWriter rejects every write with err, accepting no bytes.
type errOnlyWriter struct{ err error }

func (w *errOnlyWriter) Write(p []byte) (int, error) { return 0, w.err }

// prefixErrWriter accepts up to prefix bytes of one write, then errors,
// keeping the accepted prefix so callers can inspect what landed.
type prefixErrWriter struct {
	prefix int
	err    error
	got    bytes.Buffer
}

func (w *prefixErrWriter) Write(p []byte) (int, error) {
	k := w.prefix
	if k > len(p) {
		k = len(p)
	}
	if k > 0 {
		w.got.Write(p[:k])
	}
	if k < len(p) {
		return k, w.err
	}
	return k, nil
}

// shortWriter always accepts at most accept bytes and never errors.
type shortWriter struct {
	accept int
	calls  int
	got    bytes.Buffer
}

func (w *shortWriter) Write(p []byte) (int, error) {
	w.calls++
	k := w.accept
	if k > len(p) {
		k = len(p)
	}
	if k > 0 {
		w.got.Write(p[:k])
	}
	return k, nil
}

// failOnCallWriter succeeds (buffering) until its failAt-th call, then
// errors; it counts every attempted write.
type failOnCallWriter struct {
	calls  int
	failAt int
	err    error
	got    bytes.Buffer
}

func (w *failOnCallWriter) Write(p []byte) (int, error) {
	w.calls++
	if w.calls == w.failAt {
		return 0, w.err
	}
	return w.got.Write(p)
}

var errSentinelOutput = errors.New("synthetic output device failure")

// closingInput builds an input whose last line closes exactly one window
// [wantStart,wantEnd) for key "k", using one of the four entry shapes.
type outputFixture struct {
	name      string
	run       func(r io.Reader, out, late io.Writer) error
	input     string
	wantStart int64
	wantEnd   int64
	closeLine int // physical line that triggers the window output
}

func outputFixtures() []outputFixture {
	fixed := strings.Join([]string{
		`{"type":"event","key":"k","time":500,"value":7}`, // line 1
		`{"type":"watermark","time":1000}`,                // line 2 closes [0,1000)
	}, "\n")
	sliding := strings.Join([]string{
		`{"type":"event","key":"k","time":700,"value":7}`, // [0,1000),[500,1500)
		`{"type":"watermark","time":1000}`,                // line 2 closes only [0,1000)
	}, "\n")
	partitioned := strings.Join([]string{
		`{"type":"event","key":"k","time":500,"value":7,"partition":0}`,
		`{"type":"watermark","time":1000,"partition":1}`, // p1 reports
		`{"type":"watermark","time":1000,"partition":0}`, // line 3: effective 1000, closes
	}, "\n")
	idleDriven := strings.Join([]string{
		`{"type":"event","key":"k","time":500,"value":7,"partition":0}`,
		`{"type":"watermark","time":1000,"partition":0}`,
		`{"type":"idle","partition":1}`, // line 3: p1 idle lets effective reach 1000
	}, "\n")
	return []outputFixture{
		{
			name:      "fixed",
			run:       func(r io.Reader, out, late io.Writer) error { return RunAggregate(r, 1000, out, late) },
			input:     fixed,
			wantStart: 0,
			wantEnd:   1000,
			closeLine: 2,
		},
		{
			name: "sliding",
			run: func(r io.Reader, out, late io.Writer) error {
				return RunAggregateSliding(r, 1000, 500, out, late)
			},
			input:     sliding,
			wantStart: 0,
			wantEnd:   1000,
			closeLine: 2,
		},
		{
			name: "partitioned",
			run: func(r io.Reader, out, late io.Writer) error {
				return RunAggregatePartitioned(r, 1000, 2, out, late)
			},
			input:     partitioned,
			wantStart: 0,
			wantEnd:   1000,
			closeLine: 3,
		},
		{
			name: "idle-driven",
			run: func(r io.Reader, out, late io.Writer) error {
				return RunAggregatePartitionedSliding(r, 1000, 1000, 2, out, late)
			},
			input:     idleDriven,
			wantStart: 0,
			wantEnd:   1000,
			closeLine: 3,
		},
	}
}

func TestWindowResultWriteErrorFails(t *testing.T) {
	for _, fx := range outputFixtures() {
		t.Run(fx.name, func(t *testing.T) {
			out := &errOnlyWriter{err: errSentinelOutput}
			err := fx.run(strings.NewReader(fx.input), out, &bytes.Buffer{})
			var oe *OutputError
			if !errors.As(err, &oe) {
				t.Fatalf("expected *OutputError, got %T: %v", err, err)
			}
			if oe.Line != fx.closeLine {
				t.Errorf("Line = %d, want %d", oe.Line, fx.closeLine)
			}
			if oe.Kind != "window result" {
				t.Errorf("Kind = %q, want %q", oe.Kind, "window result")
			}
			wantDetail := fmt.Sprintf("key %q window [%d,%d)", "k", fx.wantStart, fx.wantEnd)
			if !strings.Contains(err.Error(), wantDetail) {
				t.Errorf("error %q must identify %s", err.Error(), wantDetail)
			}
			if !errors.Is(err, errSentinelOutput) {
				t.Errorf("errors.Is must expose the writer's original error, got %v", err)
			}
		})
	}
}

func TestWindowResultPrefixThenErrorFails(t *testing.T) {
	for _, fx := range outputFixtures() {
		t.Run(fx.name, func(t *testing.T) {
			out := &prefixErrWriter{prefix: 3, err: errSentinelOutput}
			err := fx.run(strings.NewReader(fx.input), out, &bytes.Buffer{})
			var oe *OutputError
			if !errors.As(err, &oe) {
				t.Fatalf("expected *OutputError, got %T: %v", err, err)
			}
			if oe.Kind != "window result" || oe.Line != fx.closeLine {
				t.Errorf("got Kind=%q Line=%d, want window result at line %d", oe.Kind, oe.Line, fx.closeLine)
			}
			if !errors.Is(err, errSentinelOutput) {
				t.Errorf("errors.Is must expose the writer's error after a prefix, got %v", err)
			}
			if out.got.Len() != 3 {
				t.Errorf("accepted prefix must remain in the writer, got %q", out.got.String())
			}
		})
	}
}

func TestWindowResultShortWriteWithoutErrorFails(t *testing.T) {
	for _, accept := range []int{0, 1, 4} {
		for _, fx := range outputFixtures() {
			t.Run(fmt.Sprintf("%s/accept%d", fx.name, accept), func(t *testing.T) {
				out := &shortWriter{accept: accept}
				err := fx.run(strings.NewReader(fx.input), out, &bytes.Buffer{})
				var oe *OutputError
				if !errors.As(err, &oe) {
					t.Fatalf("expected *OutputError for a nil-error short write, got %T: %v", err, err)
				}
				if !errors.Is(err, io.ErrShortWrite) {
					t.Errorf("short write must be errors.Is io.ErrShortWrite, got %v", err)
				}
				if oe.Kind != "window result" || oe.Line != fx.closeLine {
					t.Errorf("got Kind=%q Line=%d, want window result at line %d", oe.Kind, oe.Line, fx.closeLine)
				}
				wantDetail := fmt.Sprintf("key %q window [%d,%d)", "k", fx.wantStart, fx.wantEnd)
				if !strings.Contains(err.Error(), wantDetail) {
					t.Errorf("error %q must identify %s", err.Error(), wantDetail)
				}
			})
		}
	}
}

func TestWindowResultFailureStopsFurtherOutput(t *testing.T) {
	// Two windows are ready at the line-3 watermark. The first (end 1000)
	// fails; the second (end 2000) must never be written, and the malformed
	// line after it must never be processed.
	input := strings.Join([]string{
		`{"type":"event","key":"a","time":100,"value":1}`,
		`{"type":"event","key":"b","time":1100,"value":1}`,
		`{"type":"watermark","time":2000}`,
		`not json`, // must not be reached
	}, "\n")
	out := &failOnCallWriter{failAt: 1, err: errSentinelOutput}
	err := RunAggregate(strings.NewReader(input), 1000, out, &bytes.Buffer{})
	var oe *OutputError
	if !errors.As(err, &oe) {
		t.Fatalf("expected *OutputError, got %T: %v (later input must not be processed)", err, err)
	}
	if oe.Kind != "window result" || oe.Line != 3 {
		t.Errorf("got Kind=%q Line=%d, want window result at line 3", oe.Kind, oe.Line)
	}
	if out.calls != 1 {
		t.Errorf("writer was called %d times, want exactly 1 (no later windows)", out.calls)
	}
}

func TestFullyWrittenResultsRetainedWhenLaterWriteFails(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"a","time":100,"value":1}`,
		`{"type":"watermark","time":1000}`, // line 2: [0,1000) written in full
		`{"type":"event","key":"b","time":1100,"value":1}`,
		`{"type":"watermark","time":2000}`, // line 4: [1000,2000) fails
	}, "\n")
	out := &failOnCallWriter{failAt: 2, err: errSentinelOutput}
	err := RunAggregate(strings.NewReader(input), 1000, out, &bytes.Buffer{})
	var oe *OutputError
	if !errors.As(err, &oe) || oe.Line != 4 {
		t.Fatalf("expected *OutputError at line 4, got %T: %v", err, err)
	}
	wantFirst := `{"key":"a","start":0,"end":1000,"count":1,"sum":1}` + "\n"
	if got := out.got.String(); got != wantFirst {
		t.Fatalf("earlier complete result must stay intact, got %q want %q", got, wantFirst)
	}
}

// lateInput puts the late event on physical line 3 (blank lines count).
const lateInput = "\n" + // line 1 blank
	`{"type":"watermark","time":1000}` + "\n" + // line 2
	"\n" + // line 3 blank
	`{"type":"event","key":"k","time":999,"value":1}` + "\n" + // line 4 late
	`not json` // line 5 must never be reached

func TestLateNoticeWriteErrorFails(t *testing.T) {
	late := &errOnlyWriter{err: errSentinelOutput}
	err := RunAggregate(strings.NewReader(lateInput), 1000, &bytes.Buffer{}, late)
	var oe *OutputError
	if !errors.As(err, &oe) {
		t.Fatalf("expected *OutputError, got %T: %v", err, err)
	}
	if oe.Line != 4 {
		t.Errorf("Line = %d, want 4 (blank lines count)", oe.Line)
	}
	if oe.Kind != "late-event notice" {
		t.Errorf("Kind = %q, want late-event notice", oe.Kind)
	}
	msg := err.Error()
	if !strings.Contains(msg, "event time 999") || !strings.Contains(msg, "watermark 1000") {
		t.Errorf("error %q must identify skipped event time 999 and watermark 1000", msg)
	}
	if !errors.Is(err, errSentinelOutput) {
		t.Errorf("errors.Is must expose the writer's original error, got %v", err)
	}
}

func TestLateNoticePrefixThenErrorFails(t *testing.T) {
	late := &prefixErrWriter{prefix: 5, err: errSentinelOutput}
	err := RunAggregate(strings.NewReader(lateInput), 1000, &bytes.Buffer{}, late)
	var oe *OutputError
	if !errors.As(err, &oe) || oe.Line != 4 || oe.Kind != "late-event notice" {
		t.Fatalf("expected late-notice *OutputError at line 4, got %T: %v", err, err)
	}
	if !errors.Is(err, errSentinelOutput) {
		t.Errorf("errors.Is must expose the writer's error after a prefix, got %v", err)
	}
	if late.got.String() != "line " {
		t.Errorf("accepted prefix must remain in the writer, got %q", late.got.String())
	}
}

func TestLateNoticeShortWriteWithoutErrorFails(t *testing.T) {
	for _, accept := range []int{0, 7} {
		late := &shortWriter{accept: accept}
		err := RunAggregate(strings.NewReader(lateInput), 1000, &bytes.Buffer{}, late)
		var oe *OutputError
		if !errors.As(err, &oe) || oe.Line != 4 || oe.Kind != "late-event notice" {
			t.Fatalf("expected late-notice *OutputError at line 4, got %T: %v", err, err)
		}
		if !errors.Is(err, io.ErrShortWrite) {
			t.Errorf("short write must be errors.Is io.ErrShortWrite, got %v", err)
		}
		msg := err.Error()
		if !strings.Contains(msg, "event time 999") || !strings.Contains(msg, "watermark 1000") {
			t.Errorf("error %q must identify event time and watermark", msg)
		}
	}
}

func TestLateNoticeFailureStopsProcessing(t *testing.T) {
	// Late notice fails; the malformed line 5 must not be read as an input
	// error, and nothing may land on the window-result writer.
	late := &errOnlyWriter{err: errSentinelOutput}
	out := &failOnCallWriter{failAt: 100, err: errSentinelOutput}
	err := RunAggregate(strings.NewReader(lateInput), 1000, out, late)
	var oe *OutputError
	if !errors.As(err, &oe) {
		t.Fatalf("expected *OutputError, got %T: %v", err, err)
	}
	if out.calls != 0 {
		t.Errorf("no window results may be attempted after notice failure, got %d calls", out.calls)
	}
}

func TestLateNoticeFailureAlsoFailsInPartitionedMode(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"watermark","time":1000,"partition":0}`,
		`{"type":"watermark","time":1000,"partition":1}`,                // line 2: effective 1000
		`{"type":"event","key":"k","time":999,"value":1,"partition":0}`, // line 3 late
	}, "\n")
	late := &errOnlyWriter{err: errSentinelOutput}
	err := RunAggregatePartitionedSliding(strings.NewReader(input), 1000, 1000, 2, &bytes.Buffer{}, late)
	var oe *OutputError
	if !errors.As(err, &oe) || oe.Line != 3 || oe.Kind != "late-event notice" {
		t.Fatalf("expected late-notice *OutputError at line 3, got %T: %v", err, err)
	}
	if !errors.Is(err, errSentinelOutput) {
		t.Errorf("errors.Is must expose the writer's original error, got %v", err)
	}
}

func TestSuccessfulLateNoticeStillSkippedAndProcessingContinues(t *testing.T) {
	// Sanity check for the compatible happy path: notice fully written, the
	// skipped event contributes nothing and the later record closes normally.
	input := strings.Join([]string{
		`{"type":"watermark","time":1000}`,
		`{"type":"event","key":"k","time":999,"value":1}`, // late line 2
		`{"type":"event","key":"k","time":1000,"value":5}`,
		`{"type":"watermark","time":2000}`,
	}, "\n")
	var out, late bytes.Buffer
	if err := RunAggregate(strings.NewReader(input), 1000, &out, &late); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	wantNotice := "line 2: late event time=999 below current watermark 1000, skipped\n"
	if late.String() != wantNotice {
		t.Errorf("late notice = %q, want %q", late.String(), wantNotice)
	}
	wantOut := `{"key":"k","start":1000,"end":2000,"count":1,"sum":5}` + "\n"
	if out.String() != wantOut {
		t.Errorf("window output = %q, want %q", out.String(), wantOut)
	}
}
