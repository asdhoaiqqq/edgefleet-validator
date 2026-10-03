package edgefleet

import (
	"fmt"
	"math"
	"strings"
	"testing"
)

// Regression safeguards for cumulative sums in overlapping sliding windows at
// the signed 64-bit value boundaries. Each valid event adds its full value to
// every window that contains it (never split across the overlap), and each
// window accumulates independently, so the same event can be safe in one
// window and overflow in another. Sums exactly equal to math.MaxInt64 or
// math.MinInt64 are legal and must close with their exact integer text
// intact; crossing either boundary is fatal at the triggering input line and
// must name the window that actually overflowed. Scope is the legacy
// single-watermark RunAggregateSliding entry point.

// The full value lands in every containing window, not a share of it: one
// event inside the overlap of two windows raises both sums by the whole
// value. The value 2^53+1 is not representable as a float64 (it would round
// to 9007199254740992), so the exact output text also proves no precision was
// lost anywhere on the way in or out.
func TestAggregateSlidingSumFullValueEachWindowNoSplit(t *testing.T) {
	const v = 9007199254740993 // 2^53 + 1
	input := strings.Join([]string{
		fmt.Sprintf(`{"type":"event","key":"k","time":700,"value":%d}`, v),
		`{"type":"watermark","time":1600}`,
	}, "\n")
	stdout, stderr, err := runSliding(t, input, 1000, 600)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stderr != "" {
		t.Fatalf("unexpected stderr: %q", stderr)
	}
	want := fmt.Sprintf(
		`{"key":"k","start":0,"end":1000,"count":1,"sum":%d}`+"\n"+
			`{"key":"k","start":600,"end":1600,"count":1,"sum":%d}`+"\n", v, v)
	if stdout != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, want)
	}
	got := decodeAggregateResults(t, stdout)
	for i, r := range got {
		if r.Sum != v || r.Count != 1 {
			t.Errorf("row %d = %+v, want count 1 and exact sum %d", i, r, v)
		}
	}
}

// A cumulative sum that lands exactly on math.MaxInt64 is legal and closes
// with the exact integer text. The two overlapping windows of key "k" reach
// the boundary independently: [0,1000) gets the final +1 from an early event
// that [600,1600) never sees, and [600,1600) reaches it one event later, so
// the test also pins per-window independent accumulation for one key.
func TestAggregateSlidingSumReachesMaxInt64Exactly(t *testing.T) {
	const M = math.MaxInt64
	input := strings.Join([]string{
		fmt.Sprintf(`{"type":"event","key":"k","time":700,"value":%d}`, M-1), // [0,1000) and [600,1600)
		`{"type":"event","key":"k","time":100,"value":1}`,                    // only [0,1000): sum exactly M
		`{"type":"watermark","time":1000}`,
		`{"type":"event","key":"k","time":1100,"value":1}`, // only [600,1600): sum exactly M
		`{"type":"watermark","time":1600}`,
	}, "\n")
	stdout, stderr, err := runSliding(t, input, 1000, 600)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stderr != "" {
		t.Fatalf("unexpected stderr: %q", stderr)
	}
	want := fmt.Sprintf(
		`{"key":"k","start":0,"end":1000,"count":2,"sum":%d}`+"\n"+
			`{"key":"k","start":600,"end":1600,"count":2,"sum":%d}`+"\n", M, M)
	if stdout != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, want)
	}
	got := decodeAggregateResults(t, stdout)
	wantRows := []AggregateResult{
		{Key: "k", Start: 0, End: 1000, Count: 2, Sum: M},
		{Key: "k", Start: 600, End: 1600, Count: 2, Sum: M},
	}
	if len(got) != len(wantRows) {
		t.Fatalf("got %d rows, want %d: %v", len(got), len(wantRows), got)
	}
	for i := range wantRows {
		if got[i] != wantRows[i] {
			t.Errorf("row %d = %+v, want %+v", i, got[i], wantRows[i])
		}
	}
}

// The negative boundary is symmetric: a sum exactly equal to math.MinInt64
// closes successfully with its exact text, and a later positive event that
// brings the running sum back inside the range keeps accumulating normally.
func TestAggregateSlidingSumReachesMinInt64Exactly(t *testing.T) {
	const m = math.MinInt64
	input := strings.Join([]string{
		fmt.Sprintf(`{"type":"event","key":"k","time":700,"value":%d}`, m), // [0,1000) and [600,1600)
		`{"type":"watermark","time":1000}`,
		`{"type":"event","key":"k","time":1100,"value":1}`, // [600,1600): m+1, back in range
		`{"type":"watermark","time":1600}`,
	}, "\n")
	stdout, stderr, err := runSliding(t, input, 1000, 600)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stderr != "" {
		t.Fatalf("unexpected stderr: %q", stderr)
	}
	want := fmt.Sprintf(
		`{"key":"k","start":0,"end":1000,"count":1,"sum":%d}`+"\n"+
			`{"key":"k","start":600,"end":1600,"count":2,"sum":%d}`+"\n", m, m+1)
	if stdout != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, want)
	}
	got := decodeAggregateResults(t, stdout)
	if len(got) != 2 || got[0].Sum != m || got[1].Sum != m+1 {
		t.Fatalf("decoded sums must be exactly %d and %d, got %v", m, m+1, got)
	}
}

// Positive and negative values cancel inside a window, but the count keeps
// tracking accepted events: a window whose sum returns to zero is still a
// non-empty window and closes with its true count, and further events keep
// accumulating on top of the cancelled sum.
func TestAggregateSlidingSumZeroWindowStillEmitted(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":700,"value":5}`,
		`{"type":"event","key":"k","time":701,"value":-5}`, // both windows back to sum 0, count 2
		`{"type":"watermark","time":1000}`,
		`{"type":"event","key":"k","time":1100,"value":4}`, // [600,1600): count 3, sum 4
		`{"type":"watermark","time":1600}`,
	}, "\n")
	stdout, stderr, err := runSliding(t, input, 1000, 600)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stderr != "" {
		t.Fatalf("unexpected stderr: %q", stderr)
	}
	want := strings.Join([]string{
		`{"key":"k","start":0,"end":1000,"count":2,"sum":0}`, // zero sum, but not an empty window
		`{"key":"k","start":600,"end":1600,"count":3,"sum":4}`,
		``,
	}, "\n")
	if stdout != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, want)
	}
}

// The same event can be safe in an earlier containing window and overflow in
// a later one: [600,1600) accepts the +1 without trouble while [1200,2200),
// already holding math.MaxInt64 from an earlier event, cannot. The fatal
// error must name [1200,2200) — the window that truly overflowed — and not
// the safe [600,1600). Output already written stays, and neither the later
// cancelling negative event nor the final watermark is ever processed, so a
// would-be-cancelled final sum can never mask the mid-stream overflow.
func TestAggregateSlidingSumOverflowNamesLaterWindow(t *testing.T) {
	const M = math.MaxInt64
	input := strings.Join([]string{
		``, // line 1 blank: physical line numbers still count it
		`{"type":"event","key":"z","time":100,"value":9}`,                   // line 2: only z[0,1000)
		fmt.Sprintf(`{"type":"event","key":"k","time":1800,"value":%d}`, M), // line 3: k[1200,2200) and k[1800,2800)
		`{"type":"watermark","time":1000}`,                                  // line 4: closes z[0,1000) only
		``,                                                                  // line 5 blank
		`{"type":"event","key":"k","time":1300,"value":1}`,                  // line 6: safe in [600,1600), overflows [1200,2200)
		fmt.Sprintf(`{"type":"event","key":"k","time":1800,"value":%d}`, -M), // line 7: would cancel; never processed
		`{"type":"watermark","time":2800}`,                                   // line 8: never processed
	}, "\n")
	stdout, stderr, err := runSliding(t, input, 1000, 600)
	if err == nil {
		t.Fatal("expected cumulative sum overflow, got nil")
	}
	inputErr, ok := err.(*InputError)
	if !ok {
		t.Fatalf("expected *InputError, got %T: %v", err, err)
	}
	if inputErr.Line != 6 {
		t.Errorf("line = %d, want 6 (blank lines must count)", inputErr.Line)
	}
	if !strings.Contains(inputErr.Reason, "cumulative sum overflow") {
		t.Fatalf("reason = %q, want cumulative sum overflow", inputErr.Reason)
	}
	if !strings.Contains(inputErr.Reason, `key "k"`) {
		t.Errorf("reason = %q, want it to name key %q", inputErr.Reason, "k")
	}
	if !strings.Contains(inputErr.Reason, "[1200,2200)") {
		t.Errorf("reason = %q, want it to name the overflowing window [1200,2200)", inputErr.Reason)
	}
	if strings.Contains(inputErr.Reason, "[600,1600)") {
		t.Errorf("reason must not blame the safe window [600,1600): %q", inputErr.Reason)
	}
	if !strings.Contains(inputErr.Reason, fmt.Sprintf("%d + %d", M, 1)) {
		t.Errorf("reason = %q, want it to show the overflowing addition %d + 1", inputErr.Reason, M)
	}
	// Only the z window closed before the failure; the post-error cancel and
	// watermark never run, so nothing further is emitted.
	want := `{"key":"z","start":0,"end":1000,"count":1,"sum":9}` + "\n"
	if stdout != want {
		t.Fatalf("existing output must be retained with nothing added:\n got: %q\nwant: %q", stdout, want)
	}
	if stderr != "" {
		t.Fatalf("unexpected stderr: %q", stderr)
	}
}

// Crossing the negative boundary is equally fatal at the triggering line:
// math.MinInt64 plus -1 has no representable result. The window closed before
// the failure keeps its exact math.MinInt64 sum in the retained output, the
// error names the window that actually underflowed (not the later containing
// window that would have been safe), and the following positive event that
// could have brought the sum back into range is never processed.
func TestAggregateSlidingSumUnderflowNamesWindow(t *testing.T) {
	const m = math.MinInt64
	input := strings.Join([]string{
		fmt.Sprintf(`{"type":"event","key":"k","time":700,"value":%d}`, m), // [0,1000) and [600,1600)
		`{"type":"watermark","time":1000}`,                                 // closes [0,1000) with sum exactly m
		`{"type":"event","key":"k","time":1100,"value":-1}`,                // underflows [600,1600); [1200,2200) never reached
		`{"type":"event","key":"k","time":1100,"value":10}`,                // would cancel; never processed
		`{"type":"watermark","time":2200}`,                                 // never processed
	}, "\n")
	stdout, _, err := runSliding(t, input, 1000, 600)
	if err == nil {
		t.Fatal("expected cumulative sum overflow, got nil")
	}
	inputErr, ok := err.(*InputError)
	if !ok {
		t.Fatalf("expected *InputError, got %T: %v", err, err)
	}
	if inputErr.Line != 3 {
		t.Errorf("line = %d, want 3", inputErr.Line)
	}
	if !strings.Contains(inputErr.Reason, "cumulative sum overflow") {
		t.Fatalf("reason = %q, want cumulative sum overflow", inputErr.Reason)
	}
	if !strings.Contains(inputErr.Reason, "[600,1600)") {
		t.Errorf("reason = %q, want it to name the underflowing window [600,1600)", inputErr.Reason)
	}
	if strings.Contains(inputErr.Reason, "[1200,2200)") {
		t.Errorf("reason must not blame the unreached window [1200,2200): %q", inputErr.Reason)
	}
	if !strings.Contains(inputErr.Reason, fmt.Sprintf("%d + %d", m, -1)) {
		t.Errorf("reason = %q, want it to show the underflowing addition %d + -1", inputErr.Reason, m)
	}
	want := fmt.Sprintf(`{"key":"k","start":0,"end":1000,"count":1,"sum":%d}`+"\n", m)
	if stdout != want {
		t.Fatalf("existing output must be retained with nothing added:\n got: %q\nwant: %q", stdout, want)
	}
}

// A late event is skipped by the late-event rule alone, even when its value
// would have overflowed the still-open window it falls into: it produces the
// usual notice, changes no cumulative sum, and never becomes a sum error.
// Processing then continues normally for on-time events.
func TestAggregateSlidingLateEventWithOverflowValueSkipped(t *testing.T) {
	const M = math.MaxInt64
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":700,"value":5}`,
		`{"type":"watermark","time":1000}`,                                 // closes [0,1000); [600,1600) stays open with sum 5
		fmt.Sprintf(`{"type":"event","key":"k","time":900,"value":%d}`, M), // line 3: late; 5+M would overflow
		`{"type":"event","key":"k","time":1000,"value":2}`,                 // boundary valid: [600,1600) sum 7
		`{"type":"watermark","time":1600}`,
	}, "\n")
	stdout, stderr, err := runSliding(t, input, 1000, 600)
	if err != nil {
		t.Fatalf("late event must not become a sum error, got: %v", err)
	}
	wantErr := "line 3: late event time=900 below current watermark 1000, skipped\n"
	if stderr != wantErr {
		t.Fatalf("stderr = %q, want %q", stderr, wantErr)
	}
	// [600,1600) closes with sum 7, proving the skipped MaxInt64 never
	// touched the cumulative sum.
	want := strings.Join([]string{
		`{"key":"k","start":0,"end":1000,"count":1,"sum":5}`,
		`{"key":"k","start":600,"end":1600,"count":2,"sum":7}`,
		``,
	}, "\n")
	if stdout != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, want)
	}
}

// Overlapping windows of different keys accumulate from their own event sets:
// an event joins exactly the windows that contain its time, for its own key
// only, and the closed rows reflect those distinct sets exactly.
func TestAggregateSlidingSumIndependentPerKeyWindows(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"a","time":100,"value":7}`, // a[0,1000) only
		`{"type":"event","key":"b","time":700,"value":2}`, // b[0,1000) and b[600,1600)
		`{"type":"event","key":"a","time":700,"value":3}`, // a[0,1000) and a[600,1600)
		`{"type":"watermark","time":1600}`,
	}, "\n")
	stdout, stderr, err := runSliding(t, input, 1000, 600)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stderr != "" {
		t.Fatalf("unexpected stderr: %q", stderr)
	}
	want := strings.Join([]string{
		`{"key":"a","start":0,"end":1000,"count":2,"sum":10}`,
		`{"key":"b","start":0,"end":1000,"count":1,"sum":2}`,
		`{"key":"a","start":600,"end":1600,"count":1,"sum":3}`,
		`{"key":"b","start":600,"end":1600,"count":1,"sum":2}`,
		``,
	}, "\n")
	if stdout != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, want)
	}
}
