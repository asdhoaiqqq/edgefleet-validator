package edgefleet

import (
	"bytes"
	"fmt"
	"math"
	"strings"
	"testing"
)

// Big-integer regression coverage for sliding windows, all through the
// legacy single-watermark entry RunAggregateSliding. Times, window length
// and slide interval stay signed 64-bit integers: legal values near
// math.MaxInt64 must keep full precision (no float rounding, no negative
// starts, no false overflow), and only a window end genuinely past
// math.MaxInt64 may fail.

// Legal times far beyond the millisecond examples: window 7, slide 4 (the
// interval does not divide the length). Events at M-5 (value 2) and M-4
// (value 3), where M = math.MaxInt64. The M-4 event sits exactly on the
// right edge of [M-11,M-4) and belongs only to [M-7,M). A watermark below a
// window end emits nothing; equality closes it. The window ending exactly at
// M is legal and must succeed with exact integer bounds.
func TestAggregateSlidingBigIntLegalMembershipAndClosure(t *testing.T) {
	const M = int64(math.MaxInt64) // 9223372036854775807
	input := strings.Join([]string{
		fmt.Sprintf(`{"type":"event","key":"k","time":%d,"value":2}`, M-5),
		fmt.Sprintf(`{"type":"event","key":"k","time":%d,"value":3}`, M-4),
		fmt.Sprintf(`{"type":"watermark","time":%d}`, M-5), // below both ends: closes nothing
		fmt.Sprintf(`{"type":"watermark","time":%d}`, M-4), // equals end of [M-11,M-4)
		fmt.Sprintf(`{"type":"watermark","time":%d}`, M),   // equals end of [M-7,M)
	}, "\n")
	stdout, stderr, err := runSliding(t, input, 7, 4)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stderr != "" {
		t.Fatalf("unexpected stderr: %q", stderr)
	}
	want := strings.Join([]string{
		// [M-11,M-4): only the M-5 event; M-4 is on the right edge, outside.
		`{"key":"k","start":9223372036854775796,"end":9223372036854775803,"count":1,"sum":2}`,
		// [M-7,M): both events; end exactly M is representable and legal.
		`{"key":"k","start":9223372036854775800,"end":9223372036854775807,"count":2,"sum":5}`,
		``,
	}, "\n")
	if stdout != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, want)
	}
}

// A huge window length with event times near zero: only windows with
// non-negative starts exist, so the events land in the single window
// starting at zero — no window before time zero is created, and the
// zero-start window is not lost. Its end M-3 is still representable.
func TestAggregateSlidingBigIntHugeWindowNearZero(t *testing.T) {
	const M = int64(math.MaxInt64)
	window := M - 3 // 9223372036854775804
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":0,"value":2}`,
		`{"type":"event","key":"k","time":3,"value":5}`,
		fmt.Sprintf(`{"type":"watermark","time":%d}`, M-4), // one below the end: closes nothing
		fmt.Sprintf(`{"type":"watermark","time":%d}`, M-3), // equals the end
	}, "\n")
	stdout, stderr, err := runSliding(t, input, window, 4)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stderr != "" {
		t.Fatalf("unexpected stderr: %q", stderr)
	}
	want := `{"key":"k","start":0,"end":9223372036854775804,"count":2,"sum":7}` + "\n"
	if stdout != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, want)
	}
}

// Same parameters (window 7, slide 4), event at M-3 while the watermark is
// still well below it: the event falls into both [M-7,M), whose end is
// representable, and [M-3,M+4), whose end is not. The whole line fails
// fatally, naming the physical line (blank lines count), the window end
// overflow and the earliest offending start M-3. Output already written is
// retained and the later watermark never produces results.
func TestAggregateSlidingBigIntWindowEndOverflow(t *testing.T) {
	const M = int64(math.MaxInt64)
	input := strings.Join([]string{
		``, // line 1 blank, still counted in line numbers
		`{"type":"event","key":"k","time":0,"value":9}`,
		`{"type":"watermark","time":7}`, // closes [0,7)
		fmt.Sprintf(`{"type":"event","key":"k","time":%d,"value":1}`, M-3), // line 4: fatal
		fmt.Sprintf(`{"type":"watermark","time":%d}`, M),                   // line 5: never read
	}, "\n")
	stdout, _, err := runSliding(t, input, 7, 4)
	if err == nil {
		t.Fatal("expected window end overflow error")
	}
	inputErr, ok := err.(*InputError)
	if !ok {
		t.Fatalf("expected *InputError, got %T: %v", err, err)
	}
	if inputErr.Line != 4 {
		t.Errorf("line = %d, want 4", inputErr.Line)
	}
	if !strings.Contains(inputErr.Reason, "window end overflow") {
		t.Fatalf("reason = %q, want window end overflow", inputErr.Reason)
	}
	// The earliest containing window whose end overflows starts at M-3.
	if !strings.Contains(inputErr.Reason, "9223372036854775804") {
		t.Fatalf("reason = %q, want it to name the earliest offending start M-3", inputErr.Reason)
	}
	// The [0,7) result closed before the failure stays written; the trailing
	// watermark is never processed, so nothing else appears.
	if got := stdout; got != `{"key":"k","start":0,"end":7,"count":1,"sum":9}`+"\n" {
		t.Fatalf("earlier output must be retained and nothing else emitted, got %q", got)
	}
}

// With slide == window the big-integer behavior is exactly the fixed-window
// behavior: the same input near M through RunAggregateSliding must match
// RunAggregate byte for byte.
func TestAggregateSlidingBigIntSlideEqualsWindowMatchesFixed(t *testing.T) {
	const M = int64(math.MaxInt64)
	input := strings.Join([]string{
		fmt.Sprintf(`{"type":"event","key":"k","time":%d,"value":2}`, M-5),
		fmt.Sprintf(`{"type":"event","key":"k","time":%d,"value":3}`, M-4),
		fmt.Sprintf(`{"type":"watermark","time":%d}`, M-4), // closes nothing: end is M
		fmt.Sprintf(`{"type":"watermark","time":%d}`, M),
	}, "\n")
	slidingOut, _, err := runSliding(t, input, 7, 7)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var fixed bytes.Buffer
	if err := RunAggregate(strings.NewReader(input), 7, &fixed, &bytes.Buffer{}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if slidingOut != fixed.String() {
		t.Fatalf("slide==window output %q != fixed output %q", slidingOut, fixed.String())
	}
	// Both M-5 and M-4 land in the single fixed window [M-7,M).
	want := `{"key":"k","start":9223372036854775800,"end":9223372036854775807,"count":2,"sum":5}` + "\n"
	if slidingOut != want {
		t.Fatalf("output mismatch:\n got: %q\nwant: %q", slidingOut, want)
	}
}
