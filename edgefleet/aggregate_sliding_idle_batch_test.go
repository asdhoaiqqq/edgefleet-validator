package edgefleet

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
)

// Fixture for "one idle declaration closes several overlapping sliding
// windows in one batch" (window 1000ms, starts every 600ms, two partitions).
//
//   - p0: "a" @100 value 2 -> [0,1000)
//   - p0: "beta" @700 value 5 -> [0,1000), [600,1600)
//   - p0: "k" @700 value 4 -> [0,1000), [600,1600)
//   - p1: "k" @900 value 6 -> [0,1000), [600,1600)
//
// p0 then reports watermark 1600 while p1 only reports 600, so the effective
// watermark is 600 and nothing closes. Declaring p1 idle on physical line 7
// drops it from the minimum; the effective watermark jumps to p0's 1600 and
// the declaration itself closes one batch:
//
//	end 1000: "a" {c1,s2}, "beta" {c1,s5}, "k"   {c2,s10}  (p0+p1 merged)
//	end 1600: "beta" {c1,s5}, "k" {c2,s10}
//
// ordered by end ascending and then by key UTF-8 byte order
// ("a" < "beta" < "k"). The trailing p0 watermark 2200 only adds output if
// processing wrongly continues after a mid-batch failure: the failed and
// not-yet-written windows all stay open in the map, so it would resend them.
func idleBatchLines() []string {
	return []string{
		`{"type":"event","key":"a","time":100,"value":2,"partition":0}`,
		`{"type":"event","key":"beta","time":700,"value":5,"partition":0}`,
		`{"type":"event","key":"k","time":700,"value":4,"partition":0}`,
		`{"type":"event","key":"k","time":900,"value":6,"partition":1}`,
		`{"type":"watermark","time":1600,"partition":0}`,
		`{"type":"watermark","time":600,"partition":1}`, // effective = 600: nothing closes
		`{"type":"idle","partition":1}`,                // line 7: effective = 1600, closes the batch
		`{"type":"watermark","time":2200,"partition":0}`,
	}
}

func resultLine(t *testing.T, key string, start, end int64, count, sum int64) string {
	t.Helper()
	b, err := json.Marshal(AggregateResult{Key: key, Start: start, End: end, Count: count, Sum: sum})
	if err != nil {
		t.Fatalf("marshal expected result: %v", err)
	}
	return string(b) + "\n"
}

// idleBatchWant is the complete healthy batch, in output order.
func idleBatchWant(t *testing.T) string {
	t.Helper()
	return strings.Join([]string{
		strings.TrimSuffix(resultLine(t, "a", 0, 1000, 1, 2), "\n"),
		strings.TrimSuffix(resultLine(t, "beta", 0, 1000, 1, 5), "\n"),
		strings.TrimSuffix(resultLine(t, "k", 0, 1000, 2, 10), "\n"),
		strings.TrimSuffix(resultLine(t, "beta", 600, 1600, 1, 5), "\n"),
		strings.TrimSuffix(resultLine(t, "k", 600, 1600, 2, 10), "\n"),
		"",
	}, "\n")
}

// runIdleBatch runs the fixture through the public entry point, inserting
// blankCount blank physical lines immediately before the idle declaration to
// prove line numbers count physical lines rather than valid records.
func runIdleBatch(t *testing.T, out, late io.Writer, blankCount int) error {
	t.Helper()
	lines := idleBatchLines()
	if blankCount > 0 {
		head := append([]string{}, lines[:6]...)
		head = append(head, make([]string, blankCount)...)
		lines = append(head, lines[6:]...)
	}
	return RunAggregatePartitionedSliding(strings.NewReader(strings.Join(lines, "\n")+"\n"),
		1000, 600, 2, out, late)
}

// Happy path: the idle declaration for the lagging partition immediately
// outputs every window ready under the remaining active partition's
// watermark, in end-ascending then key-UTF-8 order. Pre-idle events from both
// partitions survive and merge per key/window; results use the existing
// window fields with no partition field; there is no error and no extra idle
// result record.
func TestIdleClosesOverlappingSlidingBatch(t *testing.T) {
	var out, late bytes.Buffer
	if err := runIdleBatch(t, &out, &late, 0); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := out.String(); got != idleBatchWant(t) {
		t.Fatalf("idle batch mismatch:\n got: %q\nwant: %q", got, idleBatchWant(t))
	}
	if late.Len() != 0 {
		t.Fatalf("idle declaration must not emit late notices: %q", late.String())
	}
}

// callGatedWriter fully buffers each write until its failAt-th call; that
// call accepts only prefix bytes and then returns gateErr. A nil gateErr
// models a writer that accepts too few bytes without reporting an error
// (io.ErrShortWrite must follow); a non-nil one models a prefix-then-error
// device. Earlier calls' content is buffered verbatim.
type callGatedWriter struct {
	calls   int
	failAt  int
	prefix  int
	gateErr error
	got     bytes.Buffer
}

func (w *callGatedWriter) Write(p []byte) (int, error) {
	w.calls++
	if w.calls != w.failAt {
		return w.got.Write(p)
	}
	k := w.prefix
	if k > len(p) {
		k = len(p)
	}
	if k > 0 {
		w.got.Write(p[:k])
	}
	if k < len(p) {
		return k, w.gateErr
	}
	return k, nil
}

// Mid-batch write failure after one complete result: the failing result's
// writer accepts a prefix and returns an error. The run fails at once and the
// error points at the triggering idle declaration (physical line 7), naming
// the failed result's key and window range and exposing the writer's own
// error via errors.Is. Complete earlier results and the accepted prefix stay
// verbatim (no undo, no completion, no resend); the rest of the batch, the
// trailing watermark record and any end-of-input flush are all abandoned.
func TestIdleBatchPrefixThenErrorFailsAtOnce(t *testing.T) {
	first := resultLine(t, "a", 0, 1000, 1, 2)
	failed := resultLine(t, "beta", 0, 1000, 1, 5)

	out := &callGatedWriter{failAt: 2, prefix: 4, gateErr: errSentinelOutput}
	err := runIdleBatch(t, out, &bytes.Buffer{}, 0)
	var oe *OutputError
	if !errors.As(err, &oe) {
		t.Fatalf("expected *OutputError, got %T: %v", err, err)
	}
	if oe.Line != 7 {
		t.Errorf("Line = %d, want 7 (the idle declaration that triggered the batch)", oe.Line)
	}
	if oe.Kind != "window result" {
		t.Errorf("Kind = %q, want %q", oe.Kind, "window result")
	}
	wantDetail := fmt.Sprintf("key %q window [%d,%d)", "beta", 0, 1000)
	if !strings.Contains(err.Error(), wantDetail) {
		t.Errorf("error %q must identify %s", err.Error(), wantDetail)
	}
	if !errors.Is(err, errSentinelOutput) {
		t.Errorf("errors.Is must expose the writer's original error, got %v", err)
	}

	wantKept := first + failed[:4] // full "a" line plus a 4-byte prefix of "beta"
	if got := out.got.String(); got != wantKept {
		t.Fatalf("complete result and accepted prefix retained verbatim:\n got: %q\nwant: %q", got, wantKept)
	}
}

// A writer that accepts too few bytes without returning an error is equally
// fatal: the caller recognizes io.ErrShortWrite, with identical retention and
// stop-processing behavior. The second ready line ("beta") is longer than the
// first ("a"), so the accept budget fully takes the first line and only a
// prefix of the second.
func TestIdleBatchShortWriteWithoutErrorFails(t *testing.T) {
	first := resultLine(t, "a", 0, 1000, 1, 2)
	failed := resultLine(t, "beta", 0, 1000, 1, 5)

	// failAt 2: the "a" line is fully buffered; the "beta" line takes a
	// 2-byte prefix with no error, which must surface as io.ErrShortWrite.
	out := &callGatedWriter{failAt: 2, prefix: 2, gateErr: nil}
	err := runIdleBatch(t, out, &bytes.Buffer{}, 0)
	var oe *OutputError
	if !errors.As(err, &oe) {
		t.Fatalf("expected *OutputError for a nil-error short write, got %T: %v", err, err)
	}
	if !errors.Is(err, io.ErrShortWrite) {
		t.Errorf("short write must be errors.Is io.ErrShortWrite, got %v", err)
	}
	if oe.Line != 7 || oe.Kind != "window result" {
		t.Errorf("got Kind=%q Line=%d, want window result at the line-7 idle declaration", oe.Kind, oe.Line)
	}
	wantDetail := fmt.Sprintf("key %q window [%d,%d)", "beta", 0, 1000)
	if !strings.Contains(err.Error(), wantDetail) {
		t.Errorf("error %q must identify %s", err.Error(), wantDetail)
	}

	wantKept := first + failed[:2]
	if got := out.got.String(); got != wantKept {
		t.Fatalf("retained bytes mismatch:\n got: %q\nwant: %q", got, wantKept)
	}
	// Exactly two writes attempted: the full "a" result and the truncated
	// "beta" one. The other three batch results are never attempted, the line-8
	// watermark is never processed (it would resend windows left in the map),
	// and nothing is flushed at end of input.
	if out.calls != 2 {
		t.Errorf("writer called %d times, want exactly 2", out.calls)
	}
}

// Blank physical lines before the idle declaration are skipped but counted:
// on a write failure the error's line must be the idle declaration's physical
// line, not its ordinal among valid records.
func TestIdleBatchFailureLineNumbersCountBlankLines(t *testing.T) {
	const blanks = 2 // idle declaration moves from physical line 7 to line 9
	out := &failOnCallWriter{failAt: 1, err: errSentinelOutput}
	err := runIdleBatch(t, out, &bytes.Buffer{}, blanks)
	var oe *OutputError
	if !errors.As(err, &oe) {
		t.Fatalf("expected *OutputError, got %T: %v", err, err)
	}
	if wantLine := 7 + blanks; oe.Line != wantLine {
		t.Errorf("Line = %d, want %d (physical lines, blanks included)", oe.Line, wantLine)
	}
	if oe.Kind != "window result" {
		t.Errorf("Kind = %q, want %q", oe.Kind, "window result")
	}
	if !errors.Is(err, errSentinelOutput) {
		t.Errorf("errors.Is must expose the writer's original error, got %v", err)
	}
}
