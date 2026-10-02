package edgefleet

import (
	"bytes"
	"errors"
	"math"
	"strings"
	"testing"
)

func runSliding(t *testing.T, input string, window, slide int64, partitions ...int64) (string, string, error) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	p := int64(0)
	if len(partitions) > 0 {
		p = partitions[0]
	}
	err := RunAggregateSliding(strings.NewReader(input), window, slide, p, &stdout, &stderr)
	return stdout.String(), stderr.String(), err
}

// The worked example from the spec: window 1000, slide 600, same key at times
// 700 and 1000 with values 2 and 3. Watermark 1000 closes only [0,1000) with
// count 1 sum 2; watermark 1600 closes [600,1600) with count 2 sum 5. The
// event at 1000 is not in [0,1000).
func TestAggregateSlideSpecExample(t *testing.T) {
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

// An event at a window boundary belongs to two windows; an event at time zero
// belongs only to the first window.
func TestAggregateSlideEventInMultipleWindows(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":0,"value":1}`,
		`{"type":"event","key":"k","time":1000,"value":5}`,
		`{"type":"watermark","time":2000}`,
	}, "\n")
	stdout, _, err := runSliding(t, input, 1000, 500)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := strings.Join([]string{
		`{"key":"k","start":0,"end":1000,"count":1,"sum":1}`,
		`{"key":"k","start":500,"end":1500,"count":1,"sum":5}`,
		`{"key":"k","start":1000,"end":2000,"count":1,"sum":5}`,
		``,
	}, "\n")
	if stdout != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, want)
	}
}

// A late event is skipped whole: it must not be added even to an open
// overlapping window whose end is still beyond the watermark.
func TestAggregateSlideLateEventSkipped(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"watermark","time":1000}`,
		`{"type":"event","key":"k","time":999,"value":1}`,  // late: skipped everywhere
		`{"type":"event","key":"k","time":1000,"value":5}`, // valid: belongs to [600,1600)
		`{"type":"watermark","time":1600}`,
	}, "\n")
	stdout, stderr, err := runSliding(t, input, 1000, 600)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	wantOut := `{"key":"k","start":600,"end":1600,"count":1,"sum":5}` + "\n"
	if stdout != wantOut {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, wantOut)
	}
	wantErr := "line 2: late event time=999 below current watermark 1000, skipped\n"
	if stderr != wantErr {
		t.Fatalf("stderr = %q, want %q", stderr, wantErr)
	}
}

// Omitting --slide-ms (or passing an interval equal to the window length)
// gives exactly the fixed-window output.
func TestAggregateSlideEqualsFixed(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"b","time":800,"value":-2}`,
		`{"type":"event","key":"a","time":1200,"value":5}`,
		`{"type":"event","key":"a","time":1500,"value":7}`,
		`{"type":"watermark","time":1000}`,
		`{"type":"watermark","time":2000}`,
	}, "\n")
	fixed, _, err := runAggregate(t, input, 1000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got, _, err := runSliding(t, input, 1000, 1000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != fixed {
		t.Fatalf("slide equal to window differs from fixed:\n got: %q\nwant: %q", got, fixed)
	}
}

// The interval need not divide the window length: window 1000, slide 300.
// Event at 500 is in [0,1000) and [300,1300); event at 1000 is in
// [300,1300), [600,1600) and [900,1900).
func TestAggregateSlideNoDivide(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":500,"value":1}`,
		`{"type":"event","key":"k","time":1000,"value":1}`,
		`{"type":"watermark","time":1900}`,
	}, "\n")
	stdout, _, err := runSliding(t, input, 1000, 300)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := strings.Join([]string{
		`{"key":"k","start":0,"end":1000,"count":1,"sum":1}`,
		`{"key":"k","start":300,"end":1300,"count":2,"sum":2}`,
		`{"key":"k","start":600,"end":1600,"count":1,"sum":1}`,
		`{"key":"k","start":900,"end":1900,"count":1,"sum":1}`,
		``,
	}, "\n")
	if stdout != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, want)
	}
}

// Windows never start before time zero: an event at time zero with slide 600
// belongs only to [0,1000).
func TestAggregateSlideNoNegativeStarts(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":0,"value":1}`,
		`{"type":"watermark","time":1000}`,
	}, "\n")
	stdout, _, err := runSliding(t, input, 1000, 600)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := `{"key":"k","start":0,"end":1000,"count":1,"sum":1}` + "\n"
	if stdout != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, want)
	}
}

// Repeated input records count as multiple events.
func TestAggregateSlideRepeatedEventsCounted(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":700,"value":2}`,
		`{"type":"event","key":"k","time":700,"value":2}`,
		`{"type":"watermark","time":1600}`,
	}, "\n")
	stdout, _, err := runSliding(t, input, 1000, 600)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := strings.Join([]string{
		`{"key":"k","start":0,"end":1000,"count":2,"sum":4}`,
		`{"key":"k","start":600,"end":1600,"count":2,"sum":4}`,
		``,
	}, "\n")
	if stdout != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, want)
	}
}

// Sliding windows work with partitions: events from different partitions for
// the same key and window merge, and closure uses the min effective
// watermark.
func TestAggregateSlidePartitions(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":700,"value":2,"partition":0}`,
		`{"type":"event","key":"k","time":1000,"value":3,"partition":1}`,
		`{"type":"watermark","time":1000,"partition":0}`,
		`{"type":"watermark","time":1000,"partition":1}`, // min = 1000, closes [0,1000)
		`{"type":"watermark","time":1600,"partition":0}`,
		`{"type":"watermark","time":1600,"partition":1}`, // closes [600,1600)
	}, "\n")
	stdout, stderr, err := runSliding(t, input, 1000, 600, 2)
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

// With partitions, a late event is skipped whole even when it falls in an open
// overlapping window.
func TestAggregateSlidePartitionsLateEvent(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"watermark","time":1000,"partition":0}`,
		`{"type":"watermark","time":1000,"partition":1}`,
		`{"type":"event","key":"k","time":999,"value":1,"partition":0}`,  // late
		`{"type":"event","key":"k","time":1000,"value":5,"partition":1}`, // valid
		`{"type":"watermark","time":1600,"partition":0}`,
		`{"type":"watermark","time":1600,"partition":1}`,
	}, "\n")
	stdout, stderr, err := runSliding(t, input, 1000, 600, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	wantOut := `{"key":"k","start":600,"end":1600,"count":1,"sum":5}` + "\n"
	if stdout != wantOut {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, wantOut)
	}
	wantErr := "line 3: late event time=999 below current watermark 1000, skipped\n"
	if stderr != wantErr {
		t.Fatalf("stderr = %q, want %q", stderr, wantErr)
	}
}

// Idle declarations keep their existing meaning with sliding windows: the
// idle record itself advances the effective watermark and closes every
// overlapping window whose end is reached.
func TestAggregateSlideIdleClosesOverlappingWindows(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"watermark","time":2000,"partition":0}`,
		`{"type":"event","key":"k","time":800,"value":2,"partition":1}`,
		`{"type":"event","key":"k","time":800,"value":3,"partition":1}`,
		`{"type":"idle","partition":1}`, // effective jumps to 2000: closes both [0,1000) and [600,1600)
	}, "\n")
	stdout, _, err := runSliding(t, input, 1000, 600, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := strings.Join([]string{
		`{"key":"k","start":0,"end":1000,"count":2,"sum":5}`,
		`{"key":"k","start":600,"end":1600,"count":2,"sum":5}`,
		``,
	}, "\n")
	if stdout != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, want)
	}
}

// Closure ordering (end ascending, then key in UTF-8 byte order) still applies
// when several overlapping windows close at once.
func TestAggregateSlideClosureOrder(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"z","time":100,"value":1}`,
		`{"type":"event","key":"a","time":100,"value":1}`,
		`{"type":"event","key":"a","time":1000,"value":1}`,
		`{"type":"watermark","time":2000}`,
	}, "\n")
	stdout, _, err := runSliding(t, input, 1000, 500)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := strings.Join([]string{
		`{"key":"a","start":0,"end":1000,"count":1,"sum":1}`,
		`{"key":"z","start":0,"end":1000,"count":1,"sum":1}`,
		`{"key":"a","start":500,"end":1500,"count":1,"sum":1}`,
		`{"key":"a","start":1000,"end":2000,"count":1,"sum":1}`,
		``,
	}, "\n")
	if stdout != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, want)
	}
}

// No window with events is emitted before its end is reached, and open
// windows are not flushed at end of input.
func TestAggregateSlideNoFlushAtEOF(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":700,"value":1}`,
		`{"type":"watermark","time":1000}`,
		`{"type":"event","key":"k","time":1500,"value":1}`,
	}, "\n")
	stdout, _, err := runSliding(t, input, 1000, 600)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := `{"key":"k","start":0,"end":1000,"count":1,"sum":1}` + "\n"
	if stdout != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, want)
	}
}

// A cumulative-sum overflow in one of the windows an event belongs to stops
// processing and names the input line and the offending window.
func TestAggregateSlideSumOverflow(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":700,"value":9223372036854775807}`,
		`{"type":"event","key":"k","time":1000,"value":1}`, // overflows [600,1600), line 2
	}, "\n")
	_, _, err := runSliding(t, input, 1000, 600)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	inputErr, ok := err.(*InputError)
	if !ok {
		t.Fatalf("expected *InputError, got %T: %v", err, err)
	}
	if inputErr.Line != 2 {
		t.Errorf("line = %d, want 2", inputErr.Line)
	}
	if !strings.Contains(inputErr.Reason, "sum overflow") {
		t.Errorf("reason = %q, want sum overflow", inputErr.Reason)
	}
	if !strings.Contains(inputErr.Reason, "[600,1600)") {
		t.Errorf("reason = %q, want it to name window [600,1600)", inputErr.Reason)
	}
}

// A window-end overflow in one of the windows an event belongs to stops
// processing and names the offending window.
func TestAggregateSlideEndOverflow(t *testing.T) {
	input := `{"type":"event","key":"k","time":300,"value":1}` + "\n"
	window := int64(math.MaxInt64 - 100)
	_, _, err := runSliding(t, input, window, 100)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	inputErr, ok := err.(*InputError)
	if !ok {
		t.Fatalf("expected *InputError, got %T: %v", err, err)
	}
	if inputErr.Line != 1 {
		t.Errorf("line = %d, want 1", inputErr.Line)
	}
	if !strings.Contains(inputErr.Reason, "window end overflow") {
		t.Errorf("reason = %q, want window end overflow", inputErr.Reason)
	}
	if !strings.Contains(inputErr.Reason, "window starting at 200") {
		t.Errorf("reason = %q, want it to name the window starting at 200", inputErr.Reason)
	}
}

// Results already written stay written when a later record fails.
func TestAggregateSlideOutputRetainedOnError(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":700,"value":1}`,
		`{"type":"watermark","time":1000}`, // closes [0,1000)
		`not json`,
	}, "\n")
	stdout, _, err := runSliding(t, input, 1000, 600)
	if err == nil {
		t.Fatal("expected error")
	}
	want := `{"key":"k","start":0,"end":1000,"count":1,"sum":1}` + "\n"
	if stdout != want {
		t.Fatalf("earlier output must be retained, got %q", stdout)
	}
}

// Determinism: repeated sliding runs are identical.
func TestAggregateSlideDeterministic(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"zeta","time":200,"value":1}`,
		`{"type":"event","key":"alpha","time":100,"value":-1}`,
		`{"type":"event","key":"alpha","time":1200,"value":2}`,
		`{"type":"event","key":"zeta","time":1100,"value":3}`,
		`{"type":"watermark","time":3000}`,
	}, "\n")
	first, _, err := runSliding(t, input, 1000, 600)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	second, _, err := runSliding(t, input, 1000, 600)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if first != second {
		t.Fatalf("repeated runs must be identical:\n%s\nvs\n%s", first, second)
	}
}

// Invalid --slide-ms arguments are rejected before any input is read.
func TestAggregateSlideInvalidArguments(t *testing.T) {
	readErr := errors.New("input was read")
	cases := []struct {
		name   string
		window int64
		slide  int64
	}{
		{"zero slide", 1000, 0},
		{"negative slide", 1000, -1},
		{"slide above window", 1000, 1001},
		{"min int64 slide", 1000, math.MinInt64},
		{"invalid window with slide", 0, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := RunAggregateSliding(errReader{}, tc.window, tc.slide, 0, &bytes.Buffer{}, &bytes.Buffer{})
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if errors.Is(err, readErr) {
				t.Fatalf("input must not be read on invalid arguments, got %v", err)
			}
		})
	}
}

type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, errors.New("input was read") }
