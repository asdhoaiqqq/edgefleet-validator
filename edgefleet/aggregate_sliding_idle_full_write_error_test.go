package edgefleet

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
)

// fullFailAtWriter accepts every write in full, buffering it, but its
// failAt-th call still reports err: the receiver holds the complete record
// (the whole JSON object plus its trailing newline) and the write is a
// failure anyway. A complete write must not cancel that error.
type fullFailAtWriter struct {
	calls  int
	failAt int
	err    error
	got    bytes.Buffer
}

func (w *fullFailAtWriter) Write(p []byte) (int, error) {
	w.calls++
	n, _ := w.got.Write(p)
	if w.calls == w.failAt {
		return n, w.err
	}
	return n, nil
}

// idleBatchUnreachableTail holds records that must never be processed once
// the idle-triggered batch fails: a watermark that would close the still-open
// [1200,2200) window and produce new results, and a malformed line that would
// be an input error. Neither may add output or replace the output failure.
var idleBatchUnreachableTail = []string{
	`{"type":"watermark","time":5000,"partition":0}`, // would close [1200,2200); must not be reached
	`not json`, // must not be reached
}

// The first result of the idle-triggered batch is written in full -- every
// JSON byte and the trailing newline reached the receiver -- yet the writer
// reports an error on that same call. The complete write does not cancel the
// error: the run fails at once with an *OutputError on the idle declaration's
// physical line (blank lines count) naming the failed key and window, the
// writer's original error stays visible through errors.Is and is not
// rewritten as io.ErrShortWrite, and the fully written record stays in place
// -- it is neither deleted nor resent, and the rest of the batch is never
// written. Records after the idle line are never processed, so a watermark
// that would close the still-open [1200,2200) window adds no output and a
// malformed line cannot replace the output error; no window is flushed at
// end of input.
func TestPartitionedSlidingIdleBatchFullWriteErrorOnFirstResult(t *testing.T) {
	lines := append(append([]string{}, slidingIdleBatchLines...), idleBatchUnreachableTail...)
	out := &fullFailAtWriter{failAt: 1, err: errSentinelOutput}
	err := RunAggregatePartitionedSliding(strings.NewReader(strings.Join(lines, "\n")), 1000, 600, 2, out, &bytes.Buffer{})

	var oe *OutputError
	if !errors.As(err, &oe) {
		t.Fatalf("expected *OutputError, got %T: %v (a complete write does not cancel the writer's error)", err, err)
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
		t.Errorf("errors.Is must expose the writer's original error even though every byte landed, got %v", err)
	}
	if errors.Is(err, io.ErrShortWrite) {
		t.Errorf("a complete write must not be reported as io.ErrShortWrite, got %v", err)
	}

	wantWritten := slidingIdleBatchOutput[0] + "\n"
	if out.got.String() != wantWritten {
		t.Errorf("retained content = %q, want exactly the one fully written result %q (kept, not deleted or resent)", out.got.String(), wantWritten)
	}
	if out.calls != 1 {
		t.Errorf("writer was called %d times, want exactly 1 (failed record not resent, rest of the batch never written)", out.calls)
	}
}

// The same rule mid-batch: the fourth result (a middle one) lands completely
// and the write still fails. The three earlier complete results and the
// errored-but-complete fourth all stay in place, the remaining two windows of
// the batch are never written, and the run stops with an *OutputError for the
// middle key and window on the idle declaration's line.
func TestPartitionedSlidingIdleBatchFullWriteErrorMidBatch(t *testing.T) {
	lines := append(append([]string{}, slidingIdleBatchLines...), idleBatchUnreachableTail...)
	out := &fullFailAtWriter{failAt: 4, err: errSentinelOutput}
	err := RunAggregatePartitionedSliding(strings.NewReader(strings.Join(lines, "\n")), 1000, 600, 2, out, &bytes.Buffer{})

	var oe *OutputError
	if !errors.As(err, &oe) {
		t.Fatalf("expected *OutputError, got %T: %v (a complete write does not cancel the writer's error)", err, err)
	}
	if oe.Line != 12 {
		t.Errorf("Line = %d, want 12 (the idle declaration; blank lines count)", oe.Line)
	}
	if oe.Kind != "window result" {
		t.Errorf("Kind = %q, want %q", oe.Kind, "window result")
	}
	wantDetail := `key "a" window [600,1600)`
	if !strings.Contains(err.Error(), wantDetail) {
		t.Errorf("error %q must identify %s", err.Error(), wantDetail)
	}
	if !errors.Is(err, errSentinelOutput) {
		t.Errorf("errors.Is must expose the writer's original error even though every byte landed, got %v", err)
	}
	if errors.Is(err, io.ErrShortWrite) {
		t.Errorf("a complete write must not be reported as io.ErrShortWrite, got %v", err)
	}

	wantWritten := strings.Join(slidingIdleBatchOutput[:4], "\n") + "\n"
	if out.got.String() != wantWritten {
		t.Errorf("retained content = %q, want the three earlier results plus the fully written failed result %q", out.got.String(), wantWritten)
	}
	if out.calls != 4 {
		t.Errorf("writer was called %d times, want exactly 4 (failed record not resent, rest of the batch never written)", out.calls)
	}
}
