package edgefleet

import (
	"math"
	"strings"
	"testing"
)

// Regression safeguards for the signed 64-bit cumulative sum when events for
// the same key arrive from different partitions and merge into the same
// overlapping sliding windows. Scope is the public partitioned sliding entry
// point RunAggregatePartitionedSliding; results, late notices and the returned
// error are all observed through it.
//
// The scenarios here combine every dimension at once:
//
//   - window length 10 with slide 6 (6 does not divide 10), so an event is
//     counted once with its full value in each window that contains it:
//     t=8 -> [0,10),[6,16); t=9 -> [0,10),[6,16); t=12,t=13 ->
//     [6,16),[12,22);
//   - two partitions whose per-key contributions merge into one count and sum
//     per window, with the partition field never reaching the output;
//   - the effective (minimum) watermark is the only thing that may close a
//     window, so a fast partition can never emit ahead of the slow one.

// Each event contributes its full value once per containing window, and the
// two partitions merge into one count and sum per window. The slide does not
// divide the window length, output carries no partition field, and only the
// effective minimum watermark releases each window, never the fast
// partition's larger one.
func TestAggregatePartitionedSlidingMergedSumFullValuePerWindow(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":8,"value":100,"partition":0}`,
		`{"type":"event","key":"k","time":9,"value":200,"partition":1}`,
		`{"type":"watermark","time":16,"partition":0}`,  // p0 ahead: p1 has not reported, no effective watermark yet
		`{"type":"watermark","time":10,"partition":1}`,  // min(16,10)=10: closes only [0,10)
		`{"type":"watermark","time":100,"partition":0}`, // p0 runs ahead; min stays 10: nothing new
		`{"type":"event","key":"k","time":12,"value":400,"partition":0}`,
		`{"type":"watermark","time":16,"partition":1}`, // min(100,16)=16: closes [6,16)
		`{"type":"watermark","time":22,"partition":1}`, // min(100,22)=22: closes [12,22)
	}, "\n")
	stdout, stderr, err := runPartitionedSliding(t, input, 10, 6, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stderr != "" {
		t.Fatalf("unexpected late notices: %q", stderr)
	}
	if strings.Contains(stdout, "partition") {
		t.Fatalf("output must not carry a partition field: %q", stdout)
	}
	want := strings.Join([]string{
		`{"key":"k","start":0,"end":10,"count":2,"sum":300}`,  // 100 (p0) + 200 (p1)
		`{"key":"k","start":6,"end":16,"count":3,"sum":700}`,  // 100 + 200 + 400
		`{"key":"k","start":12,"end":22,"count":1,"sum":400}`, // 400 only
		``,
	}, "\n")
	if stdout != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, want)
	}
	got := decodeAggregateResults(t, stdout)
	wantRows := []AggregateResult{
		{Key: "k", Start: 0, End: 10, Count: 2, Sum: 300},
		{Key: "k", Start: 6, End: 16, Count: 3, Sum: 700},
		{Key: "k", Start: 12, End: 22, Count: 1, Sum: 400},
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

// A single fast partition's watermark must not close anything while the other
// partition has never reported, even when it is far past every window end;
// events the slow partition sent beforehand stay buffered and nothing is
// emitted at end of input either.
func TestAggregatePartitionedSlidingFastPartitionCannotEmitEarly(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":8,"value":100,"partition":0}`,
		`{"type":"watermark","time":100,"partition":0}`, // p0 alone: effective watermark still unknown
		`{"type":"event","key":"k","time":9,"value":200,"partition":1}`,
	}, "\n")
	stdout, stderr, err := runPartitionedSliding(t, input, 10, 6, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stderr != "" {
		t.Fatalf("unexpected late notices: %q", stderr)
	}
	if stdout != "" {
		t.Fatalf("one partition's watermark must not close windows ahead of the other: %q", stdout)
	}
}

// Merged sums that land exactly on math.MaxInt64 or math.MinInt64 only after
// the two partitions combine are legal and must close with exact precision.
// The boundary is crossed by neither partition alone: hi is M-3 then +3,
// lo is MinInt64+5 then -5.
func TestAggregatePartitionedSlidingMergedSumExactlyAtInt64Limits(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"hi","time":1,"value":9223372036854775804,"partition":0}`,
		`{"type":"event","key":"hi","time":2,"value":3,"partition":1}`, // merged sum = MaxInt64
		`{"type":"event","key":"lo","time":1,"value":-9223372036854775803,"partition":0}`,
		`{"type":"event","key":"lo","time":2,"value":-5,"partition":1}`, // merged sum = MinInt64
		`{"type":"watermark","time":10,"partition":0}`,
		`{"type":"watermark","time":10,"partition":1}`, // both at 10: [0,10) closes
	}, "\n")
	stdout, stderr, err := runPartitionedSliding(t, input, 10, 6, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stderr != "" {
		t.Fatalf("unexpected late notices: %q", stderr)
	}
	// Literal comparison: rounding through float64 would print
	// 9223372036854775808 instead of the true max.
	want := strings.Join([]string{
		`{"key":"hi","start":0,"end":10,"count":2,"sum":9223372036854775807}`,
		`{"key":"lo","start":0,"end":10,"count":2,"sum":-9223372036854775808}`,
		``,
	}, "\n")
	if stdout != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, want)
	}
	got := decodeAggregateResults(t, stdout)
	wantRows := []AggregateResult{
		{Key: "hi", Start: 0, End: 10, Count: 2, Sum: math.MaxInt64},
		{Key: "lo", Start: 0, End: 10, Count: 2, Sum: math.MinInt64},
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

// Values larger than 2^53 merged across partitions must keep full integer
// precision in every overlapping window. 2^53+1 is not exactly representable
// as float64, so a float round-trip in the sum path would print
// 9007199254740992 and 18014398509481984 instead of the true values.
func TestAggregatePartitionedSlidingMergedSumPrecisionAbove2To53(t *testing.T) {
	const v int64 = 9007199254740993 // 2^53+1
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":8,"value":9007199254740993,"partition":0}`,  // [0,10),[6,16)
		`{"type":"event","key":"k","time":12,"value":9007199254740993,"partition":1}`, // [6,16),[12,22)
		`{"type":"watermark","time":22,"partition":0}`,
		`{"type":"watermark","time":22,"partition":1}`,
	}, "\n")
	stdout, stderr, err := runPartitionedSliding(t, input, 10, 6, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stderr != "" {
		t.Fatalf("unexpected late notices: %q", stderr)
	}
	want := strings.Join([]string{
		`{"key":"k","start":0,"end":10,"count":1,"sum":9007199254740993}`,
		`{"key":"k","start":6,"end":16,"count":2,"sum":18014398509481986}`,
		`{"key":"k","start":12,"end":22,"count":1,"sum":9007199254740993}`,
		``,
	}, "\n")
	if stdout != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, want)
	}
	got := decodeAggregateResults(t, stdout)
	wantRows := []AggregateResult{
		{Key: "k", Start: 0, End: 10, Count: 1, Sum: v},
		{Key: "k", Start: 6, End: 16, Count: 2, Sum: 2 * v},
		{Key: "k", Start: 12, End: 22, Count: 1, Sum: v},
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

// A partition-1 event that an earlier window still accepts drives a later
// shared window already sitting at MaxInt64 over the upper limit. The run
// fails immediately at that physical input line (blank lines count) with the
// existing *InputError: it names the cumulative sum overflow, the key, the
// earliest window that actually overflows [6,16) and the exact addition; the
// also-overflowing [12,22) must not be reported. The [0,10) window closed by
// the effective watermark just before is retained exactly (its merged sum is
// exactly MaxInt64), no late notice is produced, and neither the later event
// nor the later watermark takes effect, so still-open windows are never
// emitted.
func TestAggregatePartitionedSlidingMergedSumOverflowNamesEarliestWindow(t *testing.T) {
	input := strings.Join([]string{
		``, // line 1 blank: physical line numbers still count it
		`{"type":"event","key":"k","time":8,"value":9223372036854775807,"partition":0}`,
		``, // line 3 blank
		`{"type":"watermark","time":10,"partition":0}`,
		`{"type":"watermark","time":10,"partition":1}`,                  // line 5: effective 10 closes [0,10) at MaxInt64
		`{"type":"event","key":"k","time":12,"value":1,"partition":1}`,  // line 6 fatal: [6,16) and [12,22) overflow
		`{"type":"event","key":"k","time":13,"value":-1,"partition":0}`, // line 7 never processed
		`{"type":"watermark","time":100,"partition":0}`,                 // line 8 never processed
	}, "\n")
	stdout, stderr, err := runPartitionedSliding(t, input, 10, 6, 2)
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
	if !strings.Contains(inputErr.Reason, `for key "k"`) {
		t.Fatalf("reason = %q, want it to name the key", inputErr.Reason)
	}
	if !strings.Contains(inputErr.Reason, "window [6,16)") {
		t.Fatalf("reason = %q, want it to name the earliest overflowing window [6,16)", inputErr.Reason)
	}
	if !strings.Contains(inputErr.Reason, "9223372036854775807 + 1") {
		t.Fatalf("reason = %q, want it to show the cumulative and added values", inputErr.Reason)
	}
	// The same event also overflows [12,22); only the earliest window may be
	// named, and the still-legal-at-that-point addition must not be misblamed.
	if strings.Contains(inputErr.Reason, "[12,22)") {
		t.Errorf("reason must not name the later overflowing window [12,22): %q", inputErr.Reason)
	}
	if strings.Contains(inputErr.Reason, "[0,10)") {
		t.Errorf("reason must not blame the already-closed in-range window [0,10): %q", inputErr.Reason)
	}
	if stderr != "" {
		t.Errorf("the crossing event is on time, no late notice expected, got %q", stderr)
	}
	// The window closed before the failure is retained exactly; nothing after
	// the fatal line runs, so [6,16) and [12,22) are never emitted even
	// though line 8's watermark would close them.
	want := `{"key":"k","start":0,"end":10,"count":1,"sum":9223372036854775807}` + "\n"
	if stdout != want {
		t.Fatalf("closed output must be retained with nothing added:\n got: %q\nwant: %q", stdout, want)
	}
}

// Mirror image at the lower limit: partition 1's -1 is accepted into an
// earlier window but pushes a shared window already at MinInt64 below it. The
// error names the truly underflowing window [6,16) with the exact addition,
// not the also-overflowing [12,22), and the previously closed result stays.
func TestAggregatePartitionedSlidingMergedSumUnderflowNamesEarliestWindow(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":8,"value":-9223372036854775808,"partition":0}`,
		``, // line 2 blank
		`{"type":"watermark","time":10,"partition":0}`,
		`{"type":"watermark","time":10,"partition":1}`,                  // line 4: closes [0,10) at MinInt64
		`{"type":"event","key":"k","time":13,"value":-1,"partition":1}`, // line 5 fatal
		`{"type":"watermark","time":100,"partition":1}`,                 // line 6 never processed
	}, "\n")
	stdout, stderr, err := runPartitionedSliding(t, input, 10, 6, 2)
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
	if !strings.Contains(inputErr.Reason, "window [6,16)") {
		t.Fatalf("reason = %q, want it to name the earliest overflowing window [6,16)", inputErr.Reason)
	}
	if !strings.Contains(inputErr.Reason, "-9223372036854775808 + -1") {
		t.Fatalf("reason = %q, want it to show the cumulative and added values", inputErr.Reason)
	}
	if strings.Contains(inputErr.Reason, "[12,22)") {
		t.Errorf("reason must not name the later overflowing window [12,22): %q", inputErr.Reason)
	}
	if stderr != "" {
		t.Errorf("the crossing event is on time, no late notice expected, got %q", stderr)
	}
	want := `{"key":"k","start":0,"end":10,"count":1,"sum":-9223372036854775808}` + "\n"
	if stdout != want {
		t.Fatalf("closed output must be retained with nothing added:\n got: %q\nwant: %q", stdout, want)
	}
}

// Overflow is judged at the addition that crosses, not from the final
// mathematical total: the fatal +1 arrives from partition 1 while no
// effective watermark exists yet, and later records (including opposite-sign
// values from either partition that would bring every total back into range,
// and the watermarks that would close the windows) are never processed and so
// cannot erase the error.
func TestAggregatePartitionedSlidingMergedSumOverflowNotMaskedByLaterEvents(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":8,"value":9223372036854775807,"partition":0}`,
		`{"type":"watermark","time":16,"partition":0}`,                                   // p0 alone: effective watermark still unknown
		`{"type":"event","key":"k","time":12,"value":1,"partition":1}`,                   // line 3: MaxInt64 + 1 crosses
		`{"type":"event","key":"k","time":13,"value":-1,"partition":1}`,                  // would cancel in [6,16)
		`{"type":"event","key":"k","time":8,"value":-9223372036854775807,"partition":0}`, // would cancel everywhere
		`{"type":"watermark","time":100,"partition":1}`,
		`{"type":"watermark","time":100,"partition":0}`,
	}, "\n")
	stdout, stderr, err := runPartitionedSliding(t, input, 10, 6, 2)
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
	if !strings.Contains(inputErr.Reason, "window [6,16)") {
		t.Fatalf("reason = %q, want it to name [6,16)", inputErr.Reason)
	}
	if stderr != "" {
		t.Errorf("no late notice expected: every event time is on time, got %q", stderr)
	}
	if stdout != "" {
		t.Fatalf("nothing was closed before the failure and nothing may follow, got %q", stdout)
	}
}
