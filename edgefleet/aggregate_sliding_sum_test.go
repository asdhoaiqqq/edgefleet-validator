package edgefleet

import (
	"fmt"
	"math"
	"strings"
	"testing"
)

// Regression safeguards for the cumulative sum of sliding windows at the
// signed 64-bit value boundaries. Each containing window keeps its own
// independent sum: a valid event's full value is added to every window that
// contains it (never split across the overlap), sums landing exactly on
// math.MaxInt64 or math.MinInt64 are legal and keep full integer precision
// in the JSON output, and only an addition that would cross a boundary is
// fatal — at the input line that triggers it, naming the window that truly
// overflows. Scope is the legacy single-watermark RunAggregateSliding entry
// point.

// The full value lands in every containing window; nothing is divided across
// the overlap. With window 1000, slide 600 the event at 700 belongs to
// [0,1000) and [600,1600), the event at 1200 to [600,1600) and [1200,2200),
// so each window's sum reflects exactly its own event set.
func TestAggregateSlidingSumFullValueEachWindow(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":700,"value":7}`,
		`{"type":"event","key":"k","time":1200,"value":11}`,
		`{"type":"watermark","time":2200}`,
	}, "\n")
	stdout, stderr, err := runSliding(t, input, 1000, 600)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stderr != "" {
		t.Fatalf("unexpected stderr: %q", stderr)
	}
	want := strings.Join([]string{
		`{"key":"k","start":0,"end":1000,"count":1,"sum":7}`,     // 700 only
		`{"key":"k","start":600,"end":1600,"count":2,"sum":18}`,  // 700 and 1200
		`{"key":"k","start":1200,"end":2200,"count":1,"sum":11}`, // 1200 only
		``,
	}, "\n")
	if stdout != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, want)
	}
}

// Sums that land exactly on the signed 64-bit limits are legal and must
// close successfully. The expected output is compared as literal text so a
// sum rounded through a float (9223372036854775807 would print as
// 9223372036854775808) fails the test.
func TestAggregateSlidingSumExactInt64Limits(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"hi","time":100,"value":9223372036854775804}`,
		`{"type":"event","key":"hi","time":200,"value":3}`, // sum = MaxInt64 exactly
		`{"type":"event","key":"lo","time":100,"value":-9223372036854775803}`,
		`{"type":"event","key":"lo","time":200,"value":-5}`, // sum = MinInt64 exactly
		`{"type":"watermark","time":1000}`,
	}, "\n")
	stdout, stderr, err := runSliding(t, input, 1000, 600)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stderr != "" {
		t.Fatalf("unexpected stderr: %q", stderr)
	}
	want := strings.Join([]string{
		`{"key":"hi","start":0,"end":1000,"count":2,"sum":9223372036854775807}`,
		`{"key":"lo","start":0,"end":1000,"count":2,"sum":-9223372036854775808}`,
		``,
	}, "\n")
	if stdout != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, want)
	}
	// Parse the sums back: they must survive as the exact limit values.
	got := decodeAggregateResults(t, stdout)
	wantRows := []AggregateResult{
		{Key: "hi", Start: 0, End: 1000, Count: 2, Sum: math.MaxInt64},
		{Key: "lo", Start: 0, End: 1000, Count: 2, Sum: math.MinInt64},
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

// Legal values above 2^53 must not lose precision: 2^53+1 is not exactly
// representable as a float64, so any float round-trip in the sum path would
// print 9007199254740992 and 18014398509481984 instead of the true values.
func TestAggregateSlidingSumPrecisionAbove2To53(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":100,"value":9007199254740993}`, // 2^53+1
		`{"type":"event","key":"k","time":700,"value":9007199254740993}`, // shared by both windows
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
		`{"key":"k","start":0,"end":1000,"count":2,"sum":18014398509481986}`, // 2*(2^53+1)
		`{"key":"k","start":600,"end":1600,"count":1,"sum":9007199254740993}`,
		``,
	}, "\n")
	if stdout != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, want)
	}
}

// The same legal boundary sum can coexist in overlapping windows with
// different event sets: the event at 999 (value MaxInt64) belongs to both
// [0,1000) and [600,1600), while the event at 1000 (value -1) joins only
// [600,1600). Both windows close with their own exact sums.
func TestAggregateSlidingSumBoundarySharedAcrossOverlap(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":999,"value":9223372036854775807}`,
		`{"type":"event","key":"k","time":1000,"value":-1}`,
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
		`{"key":"k","start":0,"end":1000,"count":1,"sum":9223372036854775807}`,
		`{"key":"k","start":600,"end":1600,"count":2,"sum":9223372036854775806}`,
		``,
	}, "\n")
	if stdout != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, want)
	}
}

// Positive and negative values may cancel inside a window; the count keeps
// tracking the actually accepted events, and a non-empty window whose sum
// returned to zero is still emitted — it is not an empty window. Key "edge"
// cancels through the full signed range (MaxInt64 + MinInt64 + 1 = 0).
func TestAggregateSlidingSumCancellationKeepsCount(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":700,"value":5}`,
		`{"type":"event","key":"k","time":800,"value":-5}`,
		`{"type":"event","key":"k","time":1000,"value":9}`, // only [600,1600)
		`{"type":"event","key":"edge","time":100,"value":9223372036854775807}`,
		`{"type":"event","key":"edge","time":200,"value":-9223372036854775808}`,
		`{"type":"event","key":"edge","time":300,"value":1}`,
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
		`{"key":"edge","start":0,"end":1000,"count":3,"sum":0}`,
		`{"key":"k","start":0,"end":1000,"count":2,"sum":0}`,
		`{"key":"k","start":600,"end":1600,"count":3,"sum":9}`,
		``,
	}, "\n")
	if stdout != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, want)
	}
}

// Per-window sums are independent: the same event can be safe in an earlier
// window while it overflows a later one that earlier events already pushed
// to the limit. Window 1000, slide 300: the event at 1000 belongs to
// [300,1300) (empty, accepts it fine), [600,1600) and [900,1900) (both
// already at MaxInt64 from the event at 1300). The run must fail at that
// input line naming [600,1600) — the first window that truly overflows —
// not the safe [300,1300). Output closed before the failure is retained and
// the watermark after it is never processed.
func TestAggregateSlidingSumOverflowNamesOverflowingWindow(t *testing.T) {
	input := strings.Join([]string{
		``, // line 1 blank: physical line numbers still count it
		`{"type":"event","key":"k","time":100,"value":7}`,
		`{"type":"watermark","time":1000}`, // line 3 closes [0,1000)
		`{"type":"event","key":"k","time":1300,"value":9223372036854775807}`,
		`{"type":"event","key":"k","time":1000,"value":1}`, // line 5 fatal
		`{"type":"watermark","time":2200}`,                 // line 6 must never be processed
	}, "\n")
	stdout, _, err := runSliding(t, input, 1000, 300)
	if err == nil {
		t.Fatal("expected cumulative sum overflow, got nil")
	}
	inputErr, ok := err.(*InputError)
	if !ok {
		t.Fatalf("expected *InputError, got %T: %v", err, err)
	}
	if inputErr.Line != 5 {
		t.Errorf("line = %d, want 5 (blank lines must count)", inputErr.Line)
	}
	if !strings.Contains(inputErr.Reason, "cumulative sum overflow") {
		t.Fatalf("reason = %q, want cumulative sum overflow", inputErr.Reason)
	}
	if !strings.Contains(inputErr.Reason, `for key "k"`) {
		t.Fatalf("reason = %q, want it to name the key", inputErr.Reason)
	}
	if !strings.Contains(inputErr.Reason, "window [600,1600)") {
		t.Fatalf("reason = %q, want it to name the overflowing window [600,1600)", inputErr.Reason)
	}
	if !strings.Contains(inputErr.Reason, "9223372036854775807 + 1") {
		t.Fatalf("reason = %q, want it to show the overflowing addition", inputErr.Reason)
	}
	// [300,1300) accepted this same event safely; it is not the offender.
	if strings.Contains(inputErr.Reason, "[300,1300)") {
		t.Errorf("reason must not blame the safe window [300,1300): %q", inputErr.Reason)
	}
	// The result closed before the failure stays; the post-error watermark
	// never runs, so no further window is emitted.
	want := `{"key":"k","start":0,"end":1000,"count":1,"sum":7}` + "\n"
	if stdout != want {
		t.Fatalf("existing output must be retained with nothing added:\n got: %q\nwant: %q", stdout, want)
	}
}

// Mirror image at the negative boundary: the later window already sits at
// MinInt64, so the same event that the earlier window accepts drives it
// below the limit. The error names the truly overflowing window and shows
// the exact addition.
func TestAggregateSlidingSumUnderflowNamesOverflowingWindow(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":1300,"value":-9223372036854775808}`,
		`{"type":"event","key":"k","time":1000,"value":-1}`, // line 2 fatal
		`{"type":"watermark","time":2200}`,                  // never processed
	}, "\n")
	stdout, _, err := runSliding(t, input, 1000, 300)
	if err == nil {
		t.Fatal("expected cumulative sum overflow, got nil")
	}
	inputErr, ok := err.(*InputError)
	if !ok {
		t.Fatalf("expected *InputError, got %T: %v", err, err)
	}
	if inputErr.Line != 2 {
		t.Errorf("line = %d, want 2", inputErr.Line)
	}
	if !strings.Contains(inputErr.Reason, "cumulative sum overflow") {
		t.Fatalf("reason = %q, want cumulative sum overflow", inputErr.Reason)
	}
	if !strings.Contains(inputErr.Reason, "window [600,1600)") {
		t.Fatalf("reason = %q, want it to name the overflowing window [600,1600)", inputErr.Reason)
	}
	if !strings.Contains(inputErr.Reason, "-9223372036854775808 + -1") {
		t.Fatalf("reason = %q, want it to show the overflowing addition", inputErr.Reason)
	}
	if strings.Contains(inputErr.Reason, "[300,1300)") {
		t.Errorf("reason must not blame the safe window [300,1300): %q", inputErr.Reason)
	}
	if stdout != "" {
		t.Fatalf("no window closed before the failure, got %q", stdout)
	}
}

// Overflow is judged at the moment of the addition, not from the final
// total: MaxInt64 + 1 overflows at line 2 even though the value -1 on line 3
// would bring the sum back into range. The cancelling event and the closing
// watermark after the failure are never processed.
func TestAggregateSlidingSumOverflowNotMaskedByLaterEvents(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":700,"value":9223372036854775807}`,
		`{"type":"event","key":"k","time":800,"value":1}`, // line 2: MaxInt64 + 1 overflows
		`{"type":"event","key":"k","time":900,"value":-1}`,
		`{"type":"watermark","time":1600}`,
	}, "\n")
	stdout, _, err := runSliding(t, input, 1000, 600)
	if err == nil {
		t.Fatal("expected cumulative sum overflow, got nil")
	}
	inputErr, ok := err.(*InputError)
	if !ok {
		t.Fatalf("expected *InputError, got %T: %v", err, err)
	}
	if inputErr.Line != 2 {
		t.Errorf("line = %d, want 2", inputErr.Line)
	}
	if !strings.Contains(inputErr.Reason, "sum overflow") {
		t.Fatalf("reason = %q, want sum overflow", inputErr.Reason)
	}
	if stdout != "" {
		t.Fatalf("nothing may be emitted after the mid-stream overflow, got %q", stdout)
	}
}

// A late event is only skipped with a notice, even when its value would
// overflow the still-open window it falls into: it changes no sum and never
// becomes a cumulative sum error. The window keeps accumulating later valid
// events and closes with the exact boundary sum.
func TestAggregateSlidingLateEventOverflowValueSkipped(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":700,"value":9223372036854775807}`,
		`{"type":"watermark","time":1000}`, // closes [0,1000); [600,1600) stays open
		`{"type":"event","key":"k","time":999,"value":9223372036854775807}`, // line 3: late; would overflow [600,1600)
		`{"type":"event","key":"k","time":1000,"value":-5}`,                 // valid: joins [600,1600)
		`{"type":"watermark","time":1600}`,
	}, "\n")
	stdout, stderr, err := runSliding(t, input, 1000, 600)
	if err != nil {
		t.Fatalf("late event must not become a sum error, got: %v", err)
	}
	wantErr := "line 3: late event time=999 below current watermark 1000, skipped\n"
	if stderr != wantErr {
		t.Fatalf("stderr = %q, want %q", stderr, wantErr)
	}
	want := strings.Join([]string{
		`{"key":"k","start":0,"end":1000,"count":1,"sum":9223372036854775807}`,
		// The skipped late event contributed nothing: MaxInt64 - 5.
		`{"key":"k","start":600,"end":1600,"count":2,"sum":9223372036854775802}`,
		``,
	}, "\n")
	if stdout != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, want)
	}
}

// The overflow error identifies the exact window and addition for every
// containing-window grid, not just one slide: with window 1200, slide 400
// the event at 1000 belongs to [0,1200),[400,1600),[800,1800) and only the
// windows already holding MaxInt64 may be named.
func TestAggregateSlidingSumOverflowGridVariants(t *testing.T) {
	const M = math.MaxInt64
	input := strings.Join([]string{
		fmt.Sprintf(`{"type":"event","key":"k","time":1400,"value":%d}`, M),
		`{"type":"event","key":"k","time":1000,"value":1}`,
	}, "\n")
	// t=1400 belongs to [400,1600),[800,1800),[1200,2200); t=1000 belongs to
	// [0,1200),[400,1600),[800,1800). The shared windows [400,1600) and
	// [800,1800) hold MaxInt64; [0,1200) is empty and accepts the event.
	_, _, err := runSliding(t, input, 1200, 400)
	if err == nil {
		t.Fatal("expected cumulative sum overflow, got nil")
	}
	inputErr, ok := err.(*InputError)
	if !ok {
		t.Fatalf("expected *InputError, got %T: %v", err, err)
	}
	if inputErr.Line != 2 {
		t.Errorf("line = %d, want 2", inputErr.Line)
	}
	if !strings.Contains(inputErr.Reason, "window [400,1600)") {
		t.Fatalf("reason = %q, want it to name the first overflowing window [400,1600)", inputErr.Reason)
	}
	if strings.Contains(inputErr.Reason, "[0,1200)") {
		t.Errorf("reason must not blame the safe window [0,1200): %q", inputErr.Reason)
	}
}
