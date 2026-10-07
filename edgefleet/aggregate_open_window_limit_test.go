package edgefleet

import (
	"errors"
	"strings"
	"testing"
)

// These tests pin the optional open-window limit: one slot per (decoded key,
// window interval) combination currently open, charged only for combinations
// an accepted event would create for the first time, released only when a
// window closes under the existing watermark rules and its result is fully
// written. RunAggregatePartitionedSlidingMaxOpenWindows with a non-positive
// limit is the existing unlimited behavior, so the older entry points (which
// delegate with 0) are covered by their own tests.

func runAggregateMaxOpen(t *testing.T, input string, window, slide, partitions, maxOpen int64) (string, string, error) {
	t.Helper()
	var stdout, stderr strings.Builder
	err := RunAggregatePartitionedSlidingMaxOpenWindows(strings.NewReader(input), window, slide, partitions, maxOpen, &stdout, &stderr)
	return stdout.String(), stderr.String(), err
}

// A non-positive limit is the library's unlimited selector: distinct keys
// pile up without bound, exactly as through the older entry points.
func TestAggregateMaxOpenWindowsNonPositiveMeansUnlimited(t *testing.T) {
	var lines []string
	for _, key := range []string{"a", "b", "c", "d", "e"} {
		lines = append(lines, `{"type":"event","key":"`+key+`","time":100,"value":1}`)
	}
	lines = append(lines, `{"type":"watermark","time":1000}`)
	input := strings.Join(lines, "\n") + "\n"
	for _, maxOpen := range []int64{0, -1, -1 << 63} {
		stdout, stderr, err := runAggregateMaxOpen(t, input, 1000, 1000, 0, maxOpen)
		if err != nil {
			t.Fatalf("maxOpenWindows=%d: unexpected error: %v", maxOpen, err)
		}
		if stderr != "" {
			t.Fatalf("maxOpenWindows=%d: unexpected stderr: %q", maxOpen, stderr)
		}
		if got := strings.Count(stdout, "\n"); got != 5 {
			t.Fatalf("maxOpenWindows=%d: got %d result lines, want 5:\n%s", maxOpen, got, stdout)
		}
	}
}

// Reaching the limit exactly is accepted: two distinct keys in one window
// fill both slots of a limit of two, and the window closes and outputs
// normally.
func TestAggregateMaxOpenWindowsExactLimitAccepted(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"a","time":100,"value":1}`,
		`{"type":"event","key":"b","time":200,"value":2}`,
		`{"type":"watermark","time":1000}`,
	}, "\n") + "\n"
	stdout, stderr, err := runAggregateMaxOpen(t, input, 1000, 1000, 0, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stderr != "" {
		t.Fatalf("unexpected stderr: %q", stderr)
	}
	want := strings.Join([]string{
		`{"key":"a","start":0,"end":1000,"count":1,"sum":1}`,
		`{"key":"b","start":0,"end":1000,"count":1,"sum":2}`,
		``,
	}, "\n")
	if stdout != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, want)
	}
}

// Events for a combination that is already open are always accepted, even
// when every slot is taken: they only add to that window's count and sum.
func TestAggregateMaxOpenWindowsExistingCombinationAlwaysAccepted(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"a","time":100,"value":1}`,
		`{"type":"event","key":"a","time":200,"value":2}`,
		`{"type":"event","key":"a","time":300,"value":3}`,
		`{"type":"watermark","time":1000}`,
	}, "\n") + "\n"
	stdout, stderr, err := runAggregateMaxOpen(t, input, 1000, 1000, 0, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stderr != "" {
		t.Fatalf("unexpected stderr: %q", stderr)
	}
	want := `{"key":"a","start":0,"end":1000,"count":3,"sum":6}` + "\n"
	if stdout != want {
		t.Fatalf("stdout = %q, want %q", stdout, want)
	}
}

// A new combination beyond the limit fails the whole physical input line:
// the error names the line, the key, the slots in use, the new slots needed
// and the configured limit; earlier output is retained and later records
// (including the watermark that would have closed the open window) are never
// processed.
func TestAggregateMaxOpenWindowsExceededFailsWholeLine(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"a","time":100,"value":1}`,  // line 1: fills the only slot
		`{"type":"watermark","time":1000}`,                 // line 2: closes [0,1000), frees the slot
		`{"type":"event","key":"b","time":1100,"value":2}`, // line 3: window [1000,2000), slot taken again
		`{"type":"event","key":"b","time":1200,"value":3}`, // line 4: same (key, window), no new slot
		`{"type":"event","key":"d","time":1300,"value":4}`, // line 5: needs a 2nd slot, limit is 1 -> fatal
		`{"type":"watermark","time":2000}`,                 // line 6: never read
	}, "\n") + "\n"
	stdout, stderr, err := runAggregateMaxOpen(t, input, 1000, 1000, 0, 1)
	if err == nil {
		t.Fatal("expected open window limit error, got nil")
	}
	var inputErr *InputError
	if !errors.As(err, &inputErr) {
		t.Fatalf("error = %T, want *InputError: %v", err, err)
	}
	if inputErr.Line != 5 {
		t.Fatalf("error line = %d, want 5", inputErr.Line)
	}
	for _, want := range []string{`"d"`, "1 window slot(s) currently open", "needs 1 new slot(s)", "limit is 1"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error = %q, want it to mention %q", err.Error(), want)
		}
	}
	// Only the window closed before the fatal line was output; line 6's
	// watermark never ran, so b's window is not flushed.
	want := `{"key":"a","start":0,"end":1000,"count":1,"sum":1}` + "\n"
	if stdout != want {
		t.Fatalf("stdout = %q, want %q", stdout, want)
	}
	if stderr != "" {
		t.Fatalf("unexpected late-event notices: %q", stderr)
	}
}

// Under sliding windows an event is charged only for the containing
// combinations it would create for the first time: with window 1000 and
// slide 600, the time-700 event opens [0,1000) and [600,1600) (two slots),
// and the time-1000 event joins the existing [600,1600) for free.
func TestAggregateMaxOpenWindowsSlidingChargesOnlyNewCombinations(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":700,"value":2}`,  // opens [0,1000) and [600,1600): 2 slots
		`{"type":"event","key":"k","time":1000,"value":3}`, // joins [600,1600): no new slot
		`{"type":"watermark","time":1600}`,
	}, "\n") + "\n"
	stdout, stderr, err := runAggregateMaxOpen(t, input, 1000, 600, 0, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stderr != "" {
		t.Fatalf("unexpected stderr: %q", stderr)
	}
	want := strings.Join([]string{
		`{"key":"k","start":0,"end":1000,"count":1,"sum":2}`,
		`{"key":"k","start":600,"end":1600,"count":2,"sum":5}`,
		``,
	}, "\n")
	if stdout != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, want)
	}
}

// The sliding limit error reports how many new combinations the one
// triggering event would have needed, not just one: a second key's event
// belongs to two not-yet-open windows, so it needs two new slots at once.
func TestAggregateMaxOpenWindowsSlidingCountsAllNewCombinations(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"a","time":700,"value":1}`, // opens [0,1000) and [600,1600): 2 slots
		`{"type":"event","key":"b","time":700,"value":1}`, // needs 2 new slots, limit is 3 -> fatal
	}, "\n") + "\n"
	_, _, err := runAggregateMaxOpen(t, input, 1000, 600, 0, 3)
	if err == nil {
		t.Fatal("expected open window limit error, got nil")
	}
	for _, want := range []string{"line 2", `"b"`, "2 window slot(s) currently open", "needs 2 new slot(s)", "limit is 3"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error = %q, want it to mention %q", err.Error(), want)
		}
	}
}

// A slot is released only when its window closes and its result is fully
// written: with a limit of one, the first key's window must close before a
// different key's event is accepted.
func TestAggregateMaxOpenWindowsSlotReleasedOnClose(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"a","time":100,"value":1}`,
		`{"type":"watermark","time":1000}`, // closes [0,1000), freeing the only slot
		`{"type":"event","key":"b","time":1100,"value":2}`,
		`{"type":"watermark","time":2000}`,
	}, "\n") + "\n"
	stdout, stderr, err := runAggregateMaxOpen(t, input, 1000, 1000, 0, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stderr != "" {
		t.Fatalf("unexpected stderr: %q", stderr)
	}
	want := strings.Join([]string{
		`{"key":"a","start":0,"end":1000,"count":1,"sum":1}`,
		`{"key":"b","start":1000,"end":2000,"count":1,"sum":2}`,
		``,
	}, "\n")
	if stdout != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, want)
	}
}

// A late event is skipped before the limit is consulted: even with every
// slot taken it produces the ordinary late notice, never a limit error, and
// consumes no slot.
func TestAggregateMaxOpenWindowsLateEventNeverHitsLimit(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"a","time":100,"value":1}`, // fills the only slot
		`{"type":"watermark","time":500}`,                 // watermark 500, nothing closes
		`{"type":"event","key":"b","time":400,"value":2}`, // late: skipped, not a limit error
		`{"type":"event","key":"a","time":600,"value":3}`, // joins a's open window for free
		`{"type":"watermark","time":1000}`,
	}, "\n") + "\n"
	stdout, stderr, err := runAggregateMaxOpen(t, input, 1000, 1000, 0, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	wantNotice := "line 3: late event time=400 below current watermark 500, skipped\n"
	if stderr != wantNotice {
		t.Fatalf("stderr = %q, want %q", stderr, wantNotice)
	}
	want := `{"key":"a","start":0,"end":1000,"count":2,"sum":4}` + "\n"
	if stdout != want {
		t.Fatalf("stdout = %q, want %q", stdout, want)
	}
}

// Contributions from different partitions to the same key and window merge
// into one slot; they are never charged per partition.
func TestAggregateMaxOpenWindowsPartitionsShareOneSlot(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":100,"value":1,"partition":0}`,
		`{"type":"event","key":"k","time":200,"value":2,"partition":1}`,
		`{"type":"watermark","time":1000,"partition":0}`,
		`{"type":"watermark","time":1000,"partition":1}`,
	}, "\n") + "\n"
	stdout, stderr, err := runAggregateMaxOpen(t, input, 1000, 1000, 2, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stderr != "" {
		t.Fatalf("unexpected stderr: %q", stderr)
	}
	want := `{"key":"k","start":0,"end":1000,"count":2,"sum":3}` + "\n"
	if stdout != want {
		t.Fatalf("stdout = %q, want %q", stdout, want)
	}
}

// A partition going idle frees nothing by itself: only the window closures
// its watermark advance triggers release slots. Here the idle declaration
// raises the effective watermark only to 500, the open window stays open and
// still holds the single slot, so a second key's event fails.
func TestAggregateMaxOpenWindowsIdleAloneFreesNoSlot(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"a","time":100,"value":1,"partition":0}`, // fills the only slot
		`{"type":"watermark","time":500,"partition":0}`,
		`{"type":"idle","partition":1}`,                                 // effective watermark 500: nothing closes
		`{"type":"event","key":"b","time":600,"value":2,"partition":0}`, // needs a 2nd slot -> fatal
	}, "\n") + "\n"
	_, _, err := runAggregateMaxOpen(t, input, 1000, 1000, 2, 1)
	if err == nil {
		t.Fatal("expected open window limit error, got nil")
	}
	for _, want := range []string{"line 4", `"b"`, "1 window slot(s) currently open", "needs 1 new slot(s)", "limit is 1"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error = %q, want it to mention %q", err.Error(), want)
		}
	}
}

// The existing field, idle-partition and overflow rules still run before the
// limit: a damaged record on a full house keeps its original error instead
// of being re-reported as a limit overflow.
func TestAggregateMaxOpenWindowsFieldErrorKeepsOriginalReason(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"a","time":100,"value":1}`, // fills the only slot
		`{"type":"event","key":"b","time":200,"value":"x"}`,
	}, "\n") + "\n"
	_, _, err := runAggregateMaxOpen(t, input, 1000, 1000, 0, 1)
	if err == nil {
		t.Fatal("expected field error, got nil")
	}
	if !strings.Contains(err.Error(), `"value" must be a signed 64-bit integer`) {
		t.Fatalf("error = %q, want the original field error, not a limit overflow", err)
	}
}
