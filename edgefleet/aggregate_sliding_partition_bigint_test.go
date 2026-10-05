package edgefleet

import (
	"fmt"
	"math"
	"strings"
	"testing"
)

// Regression safeguards for cumulative sums in sliding windows when the same
// key's events arrive from different partitions. Each partition contributes
// only legal signed 64-bit integers on its own; the boundary is reached (or
// crossed) only after the partitions' contributions merge into one shared
// overlapping window. The check therefore has to run against the merged
// per-window state, never against a partition in isolation, and only the
// effective (minimum) watermark may publish a result. Scope is the public
// RunAggregatePartitionedSliding entry point.

// With a window length the slide does not divide (1000/600), an event from
// each partition contributes its full value once to every window containing
// it. The partition-0 event at 700 is in [0,1000) and [600,1600); the
// partition-1 event at 1200 is in [600,1600) and [1200,2200). The two
// partitions merge into one count and sum per window, and no partition field
// may appear in the output.
func TestAggregatePartitionedSlidingSumFullValueEachWindow(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":700,"value":7,"partition":0}`,
		`{"type":"event","key":"k","time":1200,"value":11,"partition":1}`,
		`{"type":"watermark","time":2200,"partition":0}`,
		`{"type":"watermark","time":2200,"partition":1}`,
	}, "\n")
	stdout, stderr, err := runPartitionedSliding(t, input, 1000, 600, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stderr != "" {
		t.Fatalf("unexpected stderr: %q", stderr)
	}
	want := strings.Join([]string{
		`{"key":"k","start":0,"end":1000,"count":1,"sum":7}`,     // p0 only
		`{"key":"k","start":600,"end":1600,"count":2,"sum":18}`,  // both partitions merged
		`{"key":"k","start":1200,"end":2200,"count":1,"sum":11}`, // p1 only
		``,
	}, "\n")
	if stdout != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, want)
	}
	// The partition field never reaches the result stream.
	if strings.Contains(stdout, "partition") {
		t.Fatalf("output must not carry a partition field: %q", stdout)
	}
}

// Values above 2^53 keep full integer precision after merging across
// partitions: 2^53+1 is not exactly representable as a float64, so any float
// round-trip in the sum path would print 18014398509481984 instead of the
// true merged value 18014398509481986.
func TestAggregatePartitionedSlidingSumPrecisionAbove2To53(t *testing.T) {
	const v int64 = 9007199254740993 // 2^53+1
	input := strings.Join([]string{
		fmt.Sprintf(`{"type":"event","key":"k","time":700,"value":%d,"partition":0}`, v),
		fmt.Sprintf(`{"type":"event","key":"k","time":800,"value":%d,"partition":1}`, v),
		`{"type":"watermark","time":1600,"partition":0}`,
		`{"type":"watermark","time":1600,"partition":1}`,
	}, "\n")
	stdout, stderr, err := runPartitionedSliding(t, input, 1000, 600, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stderr != "" {
		t.Fatalf("unexpected stderr: %q", stderr)
	}
	// Both events sit in [0,1000) and [600,1600), so each window gets the
	// merged 2*(2^53+1).
	want := strings.Join([]string{
		`{"key":"k","start":0,"end":1000,"count":2,"sum":18014398509481986}`,
		`{"key":"k","start":600,"end":1600,"count":2,"sum":18014398509481986}`,
		``,
	}, "\n")
	if stdout != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, want)
	}
	got := decodeAggregateResults(t, stdout)
	for i, r := range got {
		if r.Count != 2 || r.Sum != 2*v {
			t.Errorf("row %d = %+v, want count 2 sum %d", i, r, 2*v)
		}
	}
}

// A merged cumulative sum landing exactly on math.MaxInt64 or math.MinInt64
// is legal: each partition's value is legal on its own and the boundary is
// reached only when both contributions share a window. Literal-text
// comparison makes a float-rounded MaxInt64 (which would print as
// 9223372036854775808) fail.
func TestAggregatePartitionedSlidingSumExactInt64Limits(t *testing.T) {
	input := strings.Join([]string{
		// Shared window [0,1000): hi reaches MaxInt64, lo reaches MinInt64.
		`{"type":"event","key":"hi","time":100,"value":9223372036854775804,"partition":0}`,
		`{"type":"event","key":"hi","time":200,"value":3,"partition":1}`,
		`{"type":"event","key":"lo","time":100,"value":-9223372036854775803,"partition":0}`,
		`{"type":"event","key":"lo","time":200,"value":-5,"partition":1}`,
		`{"type":"watermark","time":1000,"partition":0}`,
		`{"type":"watermark","time":1000,"partition":1}`,
	}, "\n")
	stdout, stderr, err := runPartitionedSliding(t, input, 1000, 600, 2)
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

// A merged result may appear only once the effective (minimum) watermark
// reaches the window end. Both partitions report at 1000, partition 0 then
// races to 2200 while partition 1 is still at 1000: [0,1000) closes but the
// later shared [600,1600) must not be published by p0 alone. Partition 1's
// event arriving afterwards still joins that open window, and advancing p1
// emits the merged row; under a max-watermark implementation the shared
// window would have closed early with p0's contribution only and the p1
// event would land in a reopened empty window.
func TestAggregatePartitionedSlidingSumWaitsForEffectiveWatermark(t *testing.T) {
	const big int64 = 4000000000000000000 // two of these merge to 8e18, still below MaxInt64
	prefix := strings.Join([]string{
		fmt.Sprintf(`{"type":"event","key":"k","time":700,"value":%d,"partition":0}`, big),
		`{"type":"watermark","time":1000,"partition":1}`, // p1 reports first: effective still unknown
		`{"type":"watermark","time":1000,"partition":0}`, // min = 1000: closes [0,1000)
		`{"type":"watermark","time":2200,"partition":0}`, // p0 races ahead; min stays 1000
	}, "\n")
	// Only [0,1000) may close here: [600,1600) shares p0's event but p1 still
	// holds the effective watermark at 1000, so it must not be emitted early.
	early, earlyErr, err := runPartitionedSliding(t, prefix, 1000, 600, 2)
	if err != nil {
		t.Fatalf("prefix run error: %v", err)
	}
	wantEarly := fmt.Sprintf(`{"key":"k","start":0,"end":1000,"count":1,"sum":%d}`+"\n", big)
	if early != wantEarly {
		t.Fatalf("fast partition must not close the shared window early:\n got: %q\nwant: %q", early, wantEarly)
	}
	if earlyErr != "" {
		t.Fatalf("unexpected stderr: %q", earlyErr)
	}

	input := prefix + "\n" + strings.Join([]string{
		// Arrives while [600,1600) is still open: must merge with p0's event.
		fmt.Sprintf(`{"type":"event","key":"k","time":1300,"value":%d,"partition":1}`, big),
		`{"type":"watermark","time":2200,"partition":1}`, // min = 2200: shared windows close
	}, "\n")
	stdout, stderr, err := runPartitionedSliding(t, input, 1000, 600, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stderr != "" {
		t.Fatalf("unexpected stderr: %q", stderr)
	}
	want := strings.Join([]string{
		fmt.Sprintf(`{"key":"k","start":0,"end":1000,"count":1,"sum":%d}`, big), // p0 only, closed earlier
		fmt.Sprintf(`{"key":"k","start":600,"end":1600,"count":2,"sum":%d}`, 2*big),
		fmt.Sprintf(`{"key":"k","start":1200,"end":2200,"count":1,"sum":%d}`, big), // p1 only
		``,
	}, "\n")
	if stdout != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, want)
	}
}

// Positive boundary across partitions: [0,1000) has already been closed with
// a legal merged sum when the other partition's event arrives. That event is
// legal in the earlier [300,1300) but drives the shared [600,1600) -- which
// partition 0 already pushed to MaxInt64 -- over the upper bound, and
// [900,1900) would overflow too. Containing windows are updated in ascending
// start order, so the run fails at the event's physical line naming the
// earliest overflowing window, showing its accumulated value and the new
// one; closed and still-legal windows are not blamed. Blank lines count, the
// previously closed result stays exactly as published, and every later
// record (including the trailing watermarks) is dead.
func TestAggregatePartitionedSlidingSumOverflowNamesOverflowingWindow(t *testing.T) {
	const M = math.MaxInt64
	input := strings.Join([]string{
		``, // line 1 blank: physical line numbers still count it
		`{"type":"event","key":"k","time":100,"value":7,"partition":0}`,
		`{"type":"watermark","time":1000,"partition":0}`,
		`{"type":"watermark","time":1000,"partition":1}`, // line 4: min 1000 closes [0,1000)
		``, // line 5 blank
		// p0 saturates [600,1600), [900,1900), [1200,2200), the windows of t=1300.
		fmt.Sprintf(`{"type":"event","key":"k","time":1300,"value":%d,"partition":0}`, M),
		// p1 event at t=1000 (== watermark, so not late) is in [300,1300),
		// [600,1600), [900,1900): safe in the first, overflows [600,1600).
		`{"type":"event","key":"k","time":1000,"value":1,"partition":1}`, // line 7 fatal
		`{"type":"watermark","time":5000,"partition":1}`,
		`{"type":"watermark","time":5000,"partition":0}`,
	}, "\n")
	stdout, stderr, err := runPartitionedSliding(t, input, 1000, 300, 2)
	if err == nil {
		t.Fatal("expected cumulative sum overflow, got nil")
	}
	inputErr, ok := err.(*InputError)
	if !ok {
		t.Fatalf("expected *InputError, got %T: %v", err, err)
	}
	if inputErr.Line != 7 {
		t.Errorf("line = %d, want 7 (blank lines must count)", inputErr.Line)
	}
	if !strings.Contains(inputErr.Reason, "cumulative sum overflow") {
		t.Fatalf("reason = %q, want cumulative sum overflow", inputErr.Reason)
	}
	if !strings.Contains(inputErr.Reason, `for key "k"`) {
		t.Fatalf("reason = %q, want it to name the key", inputErr.Reason)
	}
	if !strings.Contains(inputErr.Reason, "window [600,1600)") {
		t.Fatalf("reason = %q, want it to name the earliest overflowing window [600,1600)", inputErr.Reason)
	}
	if !strings.Contains(inputErr.Reason, "9223372036854775807 + 1") {
		t.Fatalf("reason = %q, want it to show accumulated value and new value", inputErr.Reason)
	}
	// This same event was legal in the earlier window, [600,1600)'s result was
	// never published and [900,1900) overflows only later: none may be blamed.
	for _, notCause := range []string{"[300,1300)", "[900,1900)", "[1200,2200)"} {
		if strings.Contains(inputErr.Reason, notCause) {
			t.Errorf("reason must not list the non-(first-)overflowing window %s: %q", notCause, inputErr.Reason)
		}
	}
	// The already closed [0,1000) result stays exactly as published; after the
	// failure no further event or watermark takes effect.
	want := `{"key":"k","start":0,"end":1000,"count":1,"sum":7}` + "\n"
	if stdout != want {
		t.Fatalf("closed output must be retained with nothing added:\n got: %q\nwant: %q", stdout, want)
	}
	if stderr != "" {
		t.Fatalf("every event time is >= the effective watermark; late log must stay empty, got %q", stderr)
	}
}

// Mirror image at the negative boundary, with partitions swapped: partition
// 1 pushes the shared windows to MinInt64, and partition 0's event is legal
// in the earlier [300,1300) yet drives [600,1600) below the lower bound.
func TestAggregatePartitionedSlidingSumUnderflowNamesOverflowingWindow(t *testing.T) {
	input := strings.Join([]string{
		fmt.Sprintf(`{"type":"event","key":"k","time":1300,"value":%d,"partition":1}`, math.MinInt64),
		`{"type":"event","key":"k","time":1000,"value":-1,"partition":0}`, // line 2 fatal
		`{"type":"watermark","time":5000,"partition":0}`,
		`{"type":"watermark","time":5000,"partition":1}`,
	}, "\n")
	stdout, stderr, err := runPartitionedSliding(t, input, 1000, 300, 2)
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
	if !strings.Contains(inputErr.Reason, `for key "k"`) {
		t.Fatalf("reason = %q, want it to name the key", inputErr.Reason)
	}
	if !strings.Contains(inputErr.Reason, "window [600,1600)") {
		t.Fatalf("reason = %q, want it to name the earliest overflowing window [600,1600)", inputErr.Reason)
	}
	if !strings.Contains(inputErr.Reason, "-9223372036854775808 + -1") {
		t.Fatalf("reason = %q, want it to show accumulated value and new value", inputErr.Reason)
	}
	if strings.Contains(inputErr.Reason, "[300,1300)") {
		t.Errorf("reason must not blame the safe window [300,1300): %q", inputErr.Reason)
	}
	if stdout != "" || stderr != "" {
		t.Fatalf("nothing may be emitted or logged after the immediate failure, got stdout=%q stderr=%q", stdout, stderr)
	}
}

// Overflow is judged at the addition that crosses the boundary, not from the
// eventual mathematical total: a later opposite-sign event from the other
// partition would bring every window sum back into range, but it can never
// erase the overflow that already happened. Neither that event nor any
// following watermark is processed, so no open window is published.
func TestAggregatePartitionedSlidingSumOverflowNotMaskedByLaterEvents(t *testing.T) {
	input := strings.Join([]string{
		fmt.Sprintf(`{"type":"event","key":"k","time":700,"value":%d,"partition":0}`, math.MaxInt64),
		`{"type":"event","key":"k","time":800,"value":1,"partition":1}`,  // line 2: merged MaxInt64 + 1
		`{"type":"event","key":"k","time":900,"value":-1,"partition":0}`, // would cancel it: never processed
		`{"type":"watermark","time":1600,"partition":0}`,
		`{"type":"watermark","time":1600,"partition":1}`,
	}, "\n")
	stdout, stderr, err := runPartitionedSliding(t, input, 1000, 600, 2)
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
	// t=800 is in [0,1000) and [600,1600), updated in ascending start order,
	// so the first crossing -- and the named window -- is [0,1000).
	if !strings.Contains(inputErr.Reason, "window [0,1000)") {
		t.Fatalf("reason = %q, want it to name the first overflowing window [0,1000)", inputErr.Reason)
	}
	if !strings.Contains(inputErr.Reason, "9223372036854775807 + 1") {
		t.Fatalf("reason = %q, want it to show accumulated value and new value", inputErr.Reason)
	}
	if stdout != "" {
		t.Fatalf("no window had closed before the failure; nothing may be emitted, got %q", stdout)
	}
	if stderr != "" {
		t.Fatalf("late log must stay empty for non-late events, got %q", stderr)
	}
}
