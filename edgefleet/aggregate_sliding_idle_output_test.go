package edgefleet

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
)

// slidingIdleBatchLines is the shared fixture for the idle-triggered batch
// tests: window 1000, slide 600, two partitions. Both partitions feed the
// same keys into overlapping windows; partition 0's watermark (1600) is high
// enough to close [0,1000) and [600,1600) while partition 1 sits at 100, so
// nothing closes until partition 1 declares idle on physical line 12 (blank
// lines 1 and 11 count). Key "é" exercises UTF-8 byte ordering ("a" < "b" <
// "é" by bytes). The event at 1300 also opens [1200,2200), which stays open
// at effective watermark 1600 and must never be emitted.
var slidingIdleBatchLines = []string{
	``, // line 1: blank, counts toward physical line numbers
	`{"type":"event","key":"a","time":700,"value":5,"partition":0}`,
	`{"type":"event","key":"b","time":700,"value":2,"partition":0}`,
	`{"type":"event","key":"é","time":700,"value":1,"partition":0}`,
	`{"type":"event","key":"a","time":1000,"value":7,"partition":1}`,
	`{"type":"event","key":"b","time":1000,"value":3,"partition":1}`,
	`{"type":"event","key":"é","time":1000,"value":4,"partition":1}`,
	`{"type":"event","key":"a","time":1300,"value":1,"partition":0}`,
	`{"type":"watermark","time":1600,"partition":0}`,
	`{"type":"watermark","time":100,"partition":1}`, // line 10: effective 100, nothing closes
	``,                              // line 11: blank
	`{"type":"idle","partition":1}`, // line 12: effective 1600, closes the batch
}

// slidingIdleBatchOutput is the full batch the line-12 idle declaration
// emits: end ascending, then key in UTF-8 byte order, one merged count and
// sum per key and window with no partition field.
var slidingIdleBatchOutput = []string{
	`{"key":"a","start":0,"end":1000,"count":1,"sum":5}`,
	`{"key":"b","start":0,"end":1000,"count":1,"sum":2}`,
	`{"key":"é","start":0,"end":1000,"count":1,"sum":1}`,
	`{"key":"a","start":600,"end":1600,"count":3,"sum":13}`,
	`{"key":"b","start":600,"end":1600,"count":2,"sum":5}`,
	`{"key":"é","start":600,"end":1600,"count":2,"sum":5}`,
}

// prefixFailAtWriter buffers full writes until its failAt-th call, which
// accepts only prefix bytes before reporting err.
type prefixFailAtWriter struct {
	calls  int
	failAt int
	prefix int
	err    error
	got    bytes.Buffer
}

func (w *prefixFailAtWriter) Write(p []byte) (int, error) {
	w.calls++
	if w.calls == w.failAt {
		k := w.prefix
		if k > len(p) {
			k = len(p)
		}
		w.got.Write(p[:k])
		return k, w.err
	}
	return w.got.Write(p)
}

// fullErrWriter accepts every write in full, buffering the bytes, but its
// failAt-th call still reports err alongside the complete byte count: the
// receiver holds the whole record -- every JSON byte and the trailing
// newline -- yet the write is a failure.
type fullErrWriter struct {
	calls  int
	failAt int
	err    error
	got    bytes.Buffer
}

func (w *fullErrWriter) Write(p []byte) (int, error) {
	w.calls++
	w.got.Write(p)
	if w.calls == w.failAt {
		return len(p), w.err
	}
	return len(p), nil
}

// shortFailAtWriter buffers full writes until its failAt-th call, which
// accepts only accept bytes and reports no error.
type shortFailAtWriter struct {
	calls  int
	failAt int
	accept int
	got    bytes.Buffer
}

func (w *shortFailAtWriter) Write(p []byte) (int, error) {
	w.calls++
	if w.calls == w.failAt {
		k := w.accept
		if k > len(p) {
			k = len(p)
		}
		w.got.Write(p[:k])
		return k, nil
	}
	return w.got.Write(p)
}

// With no write failure the idle declaration emits every window its new
// effective watermark closes: contributions from both partitions stay merged
// per key and window, ordering is end ascending then key in UTF-8 byte
// order, and the declaration itself adds no extra record. The still-open
// [1200,2200) window is not flushed at end of input.
func TestPartitionedSlidingIdleClosesMergedWindowBatch(t *testing.T) {
	stdout, stderr, err := runPartitionedSliding(t, strings.Join(slidingIdleBatchLines, "\n"), 1000, 600, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stderr != "" {
		t.Fatalf("unexpected stderr: %q", stderr)
	}
	want := strings.Join(slidingIdleBatchOutput, "\n") + "\n"
	if stdout != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, want)
	}
}

// The second result of the idle-triggered batch accepts a prefix then fails:
// the run stops at once with an *OutputError on the idle declaration's
// physical line (blank lines count) naming the failed key and window, the
// writer's original error stays visible through errors.Is, the fully written
// first result and the accepted prefix stay in place, and the rest of the
// batch is never written. Records after the idle line — including an event
// for the now-idle partition and a malformed line — are never processed, so
// their input errors cannot replace the output error, and no window is
// flushed at end of input.
func TestPartitionedSlidingIdleBatchPrefixWriteFailure(t *testing.T) {
	lines := append(append([]string{}, slidingIdleBatchLines...),
		`{"type":"event","key":"z","time":2000,"value":1,"partition":1}`, // line 13: idle-partition event, must not be reached
		`not json`, // line 14: must not be reached
	)
	out := &prefixFailAtWriter{failAt: 2, prefix: 10, err: errSentinelOutput}
	err := RunAggregatePartitionedSliding(strings.NewReader(strings.Join(lines, "\n")), 1000, 600, 2, out, &bytes.Buffer{})

	var oe *OutputError
	if !errors.As(err, &oe) {
		t.Fatalf("expected *OutputError, got %T: %v (later input must not be processed)", err, err)
	}
	if oe.Line != 12 {
		t.Errorf("Line = %d, want 12 (the idle declaration; blank lines count)", oe.Line)
	}
	if oe.Kind != "window result" {
		t.Errorf("Kind = %q, want %q", oe.Kind, "window result")
	}
	wantDetail := `key "b" window [0,1000)`
	if !strings.Contains(err.Error(), wantDetail) {
		t.Errorf("error %q must identify %s", err.Error(), wantDetail)
	}
	if !errors.Is(err, errSentinelOutput) {
		t.Errorf("errors.Is must expose the writer's original error, got %v", err)
	}

	wantWritten := slidingIdleBatchOutput[0] + "\n" + `{"key":"b"`
	if out.got.String() != wantWritten {
		t.Errorf("retained content = %q, want first full result plus accepted prefix %q", out.got.String(), wantWritten)
	}
	if out.calls != 2 {
		t.Errorf("writer was called %d times, want exactly 2 (rest of the batch never written)", out.calls)
	}
}

// A nil-error short write mid-batch is the same fatal output failure:
// errors.Is reports io.ErrShortWrite, accepted content stays, the batch and
// the input stop immediately.
func TestPartitionedSlidingIdleBatchShortWriteFailure(t *testing.T) {
	lines := append(append([]string{}, slidingIdleBatchLines...),
		`not json`, // line 13: must not be reached
	)
	out := &shortFailAtWriter{failAt: 2, accept: 6}
	err := RunAggregatePartitionedSliding(strings.NewReader(strings.Join(lines, "\n")), 1000, 600, 2, out, &bytes.Buffer{})

	var oe *OutputError
	if !errors.As(err, &oe) {
		t.Fatalf("expected *OutputError for a nil-error short write, got %T: %v", err, err)
	}
	if oe.Line != 12 {
		t.Errorf("Line = %d, want 12 (the idle declaration; blank lines count)", oe.Line)
	}
	if oe.Kind != "window result" {
		t.Errorf("Kind = %q, want %q", oe.Kind, "window result")
	}
	if !errors.Is(err, io.ErrShortWrite) {
		t.Errorf("short write must be errors.Is io.ErrShortWrite, got %v", err)
	}
	wantDetail := `key "b" window [0,1000)`
	if !strings.Contains(err.Error(), wantDetail) {
		t.Errorf("error %q must identify %s", err.Error(), wantDetail)
	}

	wantWritten := slidingIdleBatchOutput[0] + "\n" + `{"key"`
	if out.got.String() != wantWritten {
		t.Errorf("retained content = %q, want first full result plus accepted prefix %q", out.got.String(), wantWritten)
	}
	if out.calls != 2 {
		t.Errorf("writer was called %d times, want exactly 2 (rest of the batch never written)", out.calls)
	}
}

// A writer that accepts a window result in full -- every JSON byte and the
// trailing newline -- but still returns an error has failed the write: the
// complete byte count cannot cancel the error. When the idle declaration's
// very first result fails this way the run stops at once with an
// *OutputError on the idle declaration's physical line (blank lines count),
// the receiver keeps the fully written record it already holds, the record
// is not deleted or resent, and no other window of the batch is written.
// Records after the idle line -- watermarks that would close the still-open
// [1200,2200) window and a malformed line -- are never processed, so the
// open window is not flushed at end of input and no later input error can
// replace the output failure.
func TestPartitionedSlidingIdleBatchFullWriteThenErrorFails(t *testing.T) {
	lines := append(append([]string{}, slidingIdleBatchLines...),
		`{"type":"watermark","time":3000,"partition":0}`, // line 13: must not be reached
		`{"type":"watermark","time":3000,"partition":1}`, // line 14: would close [1200,2200); must not be reached
		`not json`, // line 15: must not be reached
	)
	out := &fullErrWriter{failAt: 1, err: errSentinelOutput}
	err := RunAggregatePartitionedSliding(strings.NewReader(strings.Join(lines, "\n")), 1000, 600, 2, out, &bytes.Buffer{})

	var oe *OutputError
	if !errors.As(err, &oe) {
		t.Fatalf("expected *OutputError, got %T: %v (a complete write cannot cancel the writer's error)", err, err)
	}
	if oe.Line != 12 {
		t.Errorf("Line = %d, want 12 (the idle declaration; blank lines count)", oe.Line)
	}
	if oe.Kind != "window result" {
		t.Errorf("Kind = %q, want %q", oe.Kind, "window result")
	}
	wantDetail := `key "a" window [0,1000)`
	if !strings.Contains(err.Error(), wantDetail) {
		t.Errorf("error %q must identify %s", err.Error(), wantDetail)
	}
	if !errors.Is(err, errSentinelOutput) {
		t.Errorf("errors.Is must expose the writer's original error, got %v", err)
	}
	if errors.Is(err, io.ErrShortWrite) {
		t.Errorf("a complete write must not be reported as io.ErrShortWrite, got %v", err)
	}

	wantWritten := slidingIdleBatchOutput[0] + "\n"
	if out.got.String() != wantWritten {
		t.Errorf("retained content = %q, want exactly the fully written first result %q", out.got.String(), wantWritten)
	}
	if out.calls != 1 {
		t.Errorf("writer was called %d times, want exactly 1 (no resend, no later windows)", out.calls)
	}
}

// The same rule when the fully-written-but-failed result sits in the middle
// of the idle-triggered batch: the merged per-key results written before it
// and the failed result itself all stay in the receiver exactly once and in
// batch order, the failed record is not resent, and the remaining windows of
// the batch are never written.
func TestPartitionedSlidingIdleBatchFullWriteThenErrorMidBatch(t *testing.T) {
	lines := append(append([]string{}, slidingIdleBatchLines...),
		`{"type":"watermark","time":3000,"partition":0}`, // line 13: must not be reached
		`{"type":"watermark","time":3000,"partition":1}`, // line 14: would close [1200,2200); must not be reached
		`not json`, // line 15: must not be reached
	)
	out := &fullErrWriter{failAt: 4, err: errSentinelOutput}
	err := RunAggregatePartitionedSliding(strings.NewReader(strings.Join(lines, "\n")), 1000, 600, 2, out, &bytes.Buffer{})

	var oe *OutputError
	if !errors.As(err, &oe) {
		t.Fatalf("expected *OutputError, got %T: %v", err, err)
	}
	if oe.Line != 12 || oe.Kind != "window result" {
		t.Errorf("got Kind=%q Line=%d, want window result at line 12", oe.Kind, oe.Line)
	}
	wantDetail := `key "a" window [600,1600)`
	if !strings.Contains(err.Error(), wantDetail) {
		t.Errorf("error %q must identify %s", err.Error(), wantDetail)
	}
	if !errors.Is(err, errSentinelOutput) {
		t.Errorf("errors.Is must expose the writer's original error, got %v", err)
	}
	if errors.Is(err, io.ErrShortWrite) {
		t.Errorf("a complete write must not be reported as io.ErrShortWrite, got %v", err)
	}

	wantWritten := strings.Join(slidingIdleBatchOutput[:4], "\n") + "\n"
	if out.got.String() != wantWritten {
		t.Errorf("retained content = %q, want the three earlier results plus the fully written failed result %q", out.got.String(), wantWritten)
	}
	if out.calls != 4 {
		t.Errorf("writer was called %d times, want exactly 4 (no resend, no later windows)", out.calls)
	}
}
