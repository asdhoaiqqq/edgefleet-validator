package edgefleet

import (
	"bytes"
	"encoding/json"
	"math"
	"strings"
	"testing"
)

func runSliding(t *testing.T, input string, window, slide int64) (string, string, error) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	err := RunAggregateSliding(strings.NewReader(input), window, slide, &stdout, &stderr)
	return stdout.String(), stderr.String(), err
}

// The worked example from the spec: length 1000, interval 600; events at 700
// (value 2) and 1000 (value 3). Watermark 1000 emits only [0,1000) with one
// event and sum 2; watermark 1600 emits [600,1600) with both events and sum
// 5; the time-1000 event is outside the first window.
func TestAggregateSlidingSpecExample(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":700,"value":2}`,
		`{"type":"event","key":"k","time":1000,"value":3}`,
		`{"type":"watermark","time":1000}`,
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
		`{"key":"k","start":0,"end":1000,"count":1,"sum":2}`,
		`{"key":"k","start":600,"end":1600,"count":2,"sum":5}`,
		``,
	}, "\n")
	if stdout != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, want)
	}
}

// Every window-start boundary in the grid shifts membership: with
// window=1000, slide=600 the windows containing t=1200 are [600,1600) and
// [1200,2200), while t=1600 belongs to neither of those.
func TestAggregateSlidingMembershipBoundaries(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"a","time":599,"value":1}`,
		`{"type":"event","key":"a","time":600,"value":1}`,
		`{"type":"event","key":"a","time":999,"value":1}`,
		`{"type":"event","key":"a","time":1000,"value":1}`,
		`{"type":"event","key":"a","time":1199,"value":1}`,
		`{"type":"event","key":"a","time":1200,"value":1}`,
		`{"type":"watermark","time":2200}`,
	}, "\n")
	stdout, _, err := runSliding(t, input, 1000, 600)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := strings.Join([]string{
		`{"key":"a","start":0,"end":1000,"count":3,"sum":3}`,    // 599,600,999
		`{"key":"a","start":600,"end":1600,"count":5,"sum":5}`,  // 600,999,1000,1199,1200
		`{"key":"a","start":1200,"end":2200,"count":1,"sum":1}`, // 1200
		``,
	}, "\n")
	if stdout != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, want)
	}
}

// A non-dividing interval keeps the "event at end is outside" boundary for
// every window; length 1000, slide 300 gives at most 4 covering windows and
// windows with no events are not emitted.
func TestAggregateSlidingNonDividingInterval(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":0,"value":1}`,
		`{"type":"event","key":"k","time":700,"value":1}`,
		`{"type":"watermark","time":2000}`,
	}, "\n")
	stdout, _, err := runSliding(t, input, 1000, 300)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := strings.Join([]string{
		`{"key":"k","start":0,"end":1000,"count":2,"sum":2}`,   // 0 and 700
		`{"key":"k","start":300,"end":1300,"count":1,"sum":1}`, // 700
		`{"key":"k","start":600,"end":1600,"count":1,"sum":1}`, // 700
		// [900,1900) has no events and is not emitted; [1200,2200) stays open.
		``,
	}, "\n")
	if stdout != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, want)
	}
}

// When one watermark advance closes several windows, output is ordered by
// end ascending and then by key in UTF-8 byte order.
func TestAggregateSlidingClosureOrder(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"中","time":700,"value":1}`,
		`{"type":"event","key":"A","time":700,"value":1}`,
		`{"type":"event","key":"é","time":100,"value":1}`,
		`{"type":"event","key":"A","time":1100,"value":1}`,
		`{"type":"watermark","time":1600}`,
	}, "\n")
	stdout, _, err := runSliding(t, input, 1000, 600)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	lines := strings.Split(strings.TrimSuffix(stdout, "\n"), "\n")
	type row struct {
		key        string
		start, end int64
	}
	var got []row
	for _, line := range lines {
		var r struct {
			Key   string `json:"key"`
			Start int64  `json:"start"`
			End   int64  `json:"end"`
		}
		if err := json.Unmarshal([]byte(line), &r); err != nil {
			t.Fatalf("invalid output line %q: %v", line, err)
		}
		got = append(got, row{r.Key, r.Start, r.End})
	}
	want := []row{
		{"A", 0, 1000},
		{"é", 0, 1000},
		{"中", 0, 1000},
		{"A", 600, 1600},
		{"中", 600, 1600},
	}
	if len(got) != len(want) {
		t.Fatalf("got %d rows %v, want %d rows %v", len(got), got, len(want), want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("row %d = %v, want %v\nfull output:\n%s", i, got[i], want[i], stdout)
		}
	}
}

// Empty windows never produce output, and still-open windows are not flushed
// at end of input.
func TestAggregateSlidingNoEmptyWindowsAndNoEOFFlush(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":100,"value":1}`,
		`{"type":"watermark","time":999}`, // closes nothing
	}, "\n")
	stdout, _, err := runSliding(t, input, 1000, 300)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stdout != "" {
		t.Fatalf("nothing should close at watermark 999, got %q", stdout)
	}
}

// Late events are skipped wholesale even when their time still falls inside
// an unclosed overlapping window; the note keeps the physical line number.
// An event exactly at the watermark remains valid.
func TestAggregateSlidingLateEvenInsideOverlap(t *testing.T) {
	input := strings.Join([]string{
		``, // line 1 blank
		`{"type":"event","key":"k","time":900,"value":7}`,
		`{"type":"watermark","time":1000}`,                 // closes [0,1000); [600,1600) stays open
		`{"type":"event","key":"k","time":999,"value":1}`,  // line 4: late, even though [600,1600) is still open
		`{"type":"event","key":"k","time":1000,"value":3}`, // line 5: boundary valid -> [600,1600)
		`{"type":"watermark","time":1600}`,
	}, "\n")
	stdout, stderr, err := runSliding(t, input, 1000, 600)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// The pre-watermark time-900 event and the boundary time-1000 event both
	// accumulated into [600,1600); the late time-999 event must not.
	want := strings.Join([]string{
		`{"key":"k","start":0,"end":1000,"count":1,"sum":7}`,
		`{"key":"k","start":600,"end":1600,"count":2,"sum":10}`,
		``,
	}, "\n")
	if stdout != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, want)
	}
	wantErr := "line 4: late event time=999 below current watermark 1000, skipped\n"
	if stderr != wantErr {
		t.Fatalf("stderr = %q, want %q", stderr, wantErr)
	}
}

// Out-of-order arrival does not change window assignment; duplicate input
// lines count as separate events in every containing window.
func TestAggregateSlidingOutOfOrderAndDuplicates(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"watermark","time":1000}`,
		`{"type":"event","key":"k","time":700,"value":2}`,  // late: 700 < 1000
		`{"type":"event","key":"k","time":1000,"value":3}`, // boundary valid
		`{"type":"event","key":"k","time":1200,"value":4}`, // valid
		`{"type":"event","key":"k","time":1000,"value":3}`, // duplicate of line 3
		`{"type":"watermark","time":1600}`,
	}, "\n")
	stdout, stderr, err := runSliding(t, input, 1000, 600)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// [600,1600) gets both time-1000 duplicates (3+3=6) plus time-1200 (4).
	want := `{"key":"k","start":600,"end":1600,"count":3,"sum":10}` + "\n"
	if stdout != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, want)
	}
	if !strings.Contains(stderr, "line 2: late event time=700") {
		t.Fatalf("expected late note for line 2, got %q", stderr)
	}
}

// Invalid slide arguments are rejected before input is read.
func TestAggregateSlidingInvalidArguments(t *testing.T) {
	cases := []struct {
		name   string
		w, sl  int64
		wantIn string
	}{
		{"zero slide", 1000, 0, "positive"},
		{"negative slide", 1000, -600, "positive"},
		{"min slide", 1000, math.MinInt64, "positive"},
		{"slide exceeds window", 1000, 1001, "must not exceed"},
		{"zero window", 0, 0, "window length"},
		{"negative window", -1, -1, "window length"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := RunAggregateSliding(strings.NewReader("not consumed\n"), tc.w, tc.sl, &bytes.Buffer{}, &bytes.Buffer{})
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !strings.Contains(err.Error(), tc.wantIn) {
				t.Fatalf("error = %q, want substring %q", err.Error(), tc.wantIn)
			}
		})
	}
}

// slide == window is exactly the fixed-window behavior.
func TestAggregateSlidingEqualsWindowMatchesFixed(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"b","time":800,"value":-2}`,
		`{"type":"event","key":"a","time":1200,"value":5}`,
		`{"type":"event","key":"a","time":1500,"value":7}`,
		`{"type":"watermark","time":2000}`,
	}, "\n")
	slidingOut, _, err := runSliding(t, input, 1000, 1000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var fixed bytes.Buffer
	if err := RunAggregate(strings.NewReader(input), 1000, &fixed, &bytes.Buffer{}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if slidingOut != fixed.String() {
		t.Fatalf("slide==window output %q != fixed output %q", slidingOut, fixed.String())
	}
}

// A window end leaving the signed 64-bit range stops processing and reports
// the input line and offending window; prior output remains and later lines
// are not read.
func TestAggregateSlidingWindowEndOverflow(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":0,"value":1}`,
		`{"type":"watermark","time":1000}`,
		`{"type":"event","key":"k","time":9223372036854775807,"value":1}`,
		`{"type":"event","key":"k","time":1,"value":1}`,
	}, "\n")
	// window=1000, slide=600: containing windows for MaxInt64 start near
	// MaxInt64 and their ends overflow.
	stdout, _, err := runSliding(t, input, 1000, 600)
	if err == nil {
		t.Fatal("expected overflow error")
	}
	inputErr, ok := err.(*InputError)
	if !ok {
		t.Fatalf("expected *InputError, got %T: %v", err, err)
	}
	if inputErr.Line != 3 {
		t.Errorf("line = %d, want 3", inputErr.Line)
	}
	if !strings.Contains(inputErr.Reason, "window end overflow") {
		t.Fatalf("reason = %q, want window end overflow", inputErr.Reason)
	}
	if !strings.Contains(inputErr.Reason, "window starting at") {
		t.Fatalf("reason = %q, want it to name the offending window start", inputErr.Reason)
	}
	if got := stdout; got != `{"key":"k","start":0,"end":1000,"count":1,"sum":1}`+"\n" {
		t.Fatalf("earlier output must be retained, got %q", got)
	}
}

// A cumulative sum overflow reached through overlapping windows is fatal for
// that input line and names the offending window.
func TestAggregateSlidingSumOverflow(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":700,"value":9223372036854775807}`,
		`{"type":"event","key":"k","time":800,"value":1}`, // adds to both windows covering 700..800
	}, "\n")
	_, _, err := runSliding(t, input, 1000, 600)
	if err == nil {
		t.Fatal("expected cumulative sum overflow")
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
	if !strings.Contains(inputErr.Reason, "[0,1000)") {
		t.Fatalf("overflow should name the offending window [0,1000), got %q", inputErr.Reason)
	}
}

// Overflow in a later containing window must not leave earlier windows of the
// same line partially updated: the failing event counts in no window.
func TestAggregateSlidingOverflowLeavesNoPartialUpdate(t *testing.T) {
	// First event lands in [0,1000) and [600,1600); second positive overflow
	// event is rejected, so a later watermark still shows count 1.
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":700,"value":1}`,
		`{"type":"event","key":"k","time":800,"value":9223372036854775807}`,
		`{"type":"watermark","time":1600}`,
	}, "\n")
	stdout, _, err := runSliding(t, input, 1000, 600)
	if err == nil {
		t.Fatal("expected overflow error")
	}
	// Nothing closes before the fatal line, so no window should carry the
	// rejected event; output is empty because the only watermark runs after
	// the failure and is never reached.
	if stdout != "" {
		t.Fatalf("failing line must not update any window, got %q", stdout)
	}
}
