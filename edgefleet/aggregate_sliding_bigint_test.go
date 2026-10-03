package edgefleet

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"testing"
)

// Regression safeguards for sliding windows at the signed 64-bit time
// boundaries. Times, window length and slide interval stay signed int64 all
// the way through: legal values close to math.MaxInt64 must keep full integer
// precision (no negative starts, no false overflow), and only a window end
// strictly above math.MaxInt64 is reported as an overflow. Scope is the legacy
// single-watermark RunAggregateSliding entry point.

// decodeAggregateResults parses emitted lines back into AggregateResult so the
// assertions observe the exact signed 64-bit start/end/count/sum values.
func decodeAggregateResults(t *testing.T, stdout string) []AggregateResult {
	t.Helper()
	if stdout == "" {
		return nil
	}
	var got []AggregateResult
	for i, line := range strings.Split(strings.TrimSuffix(stdout, "\n"), "\n") {
		var r AggregateResult
		if err := json.Unmarshal([]byte(line), &r); err != nil {
			t.Fatalf("invalid output line %d %q: %v", i+1, line, err)
		}
		got = append(got, r)
	}
	return got
}

// The spec's large-number example: M=math.MaxInt64, window length 7, slide 4
// (not a divisor). Events at M-5 (value 2) and M-4 (value 3). Each event is
// counted once in every window that contains it, with the full value added;
// the second event sits exactly on the right edge of [M-11,M-4) and therefore
// enters only the later [M-7,M) window. A watermark strictly below a window
// end closes nothing; equality is what emits. An end exactly equal to M is a
// legal, representable window end and must not be reported as overflow.
func TestAggregateSlidingBigTimeWindowEndAtMaxInt64(t *testing.T) {
	const M = math.MaxInt64
	const w, sl int64 = 7, 4

	events := strings.Join([]string{
		fmt.Sprintf(`{"type":"event","key":"k","time":%d,"value":2}`, M-5),
		fmt.Sprintf(`{"type":"event","key":"k","time":%d,"value":3}`, M-4),
	}, "\n")

	// Watermark M-5 is still strictly below the earliest window end M-4.
	stdout, stderr, err := runSliding(t, events+"\n"+
		fmt.Sprintf(`{"type":"watermark","time":%d}`, M-5), w, sl)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stderr != "" {
		t.Fatalf("unexpected stderr: %q", stderr)
	}
	if stdout != "" {
		t.Fatalf("watermark below the window end must close nothing, got %q", stdout)
	}

	// Watermark M-4 closes [M-11,M-4); watermark M closes [M-7,M), whose end
	// is exactly the largest representable signed 64-bit integer.
	input := events + "\n" + strings.Join([]string{
		fmt.Sprintf(`{"type":"watermark","time":%d}`, M-5),
		fmt.Sprintf(`{"type":"watermark","time":%d}`, M-4),
		fmt.Sprintf(`{"type":"watermark","time":%d}`, M),
	}, "\n")
	stdout, stderr, err = runSliding(t, input, w, sl)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stderr != "" {
		t.Fatalf("unexpected stderr: %q", stderr)
	}
	want := fmt.Sprintf(
		`{"key":"k","start":%d,"end":%d,"count":1,"sum":2}`+"\n"+
			`{"key":"k","start":%d,"end":%d,"count":2,"sum":5}`+"\n",
		M-11, M-4, M-7, M)
	if stdout != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, want)
	}

	// Parse the integer fields back: both window bounds must survive as exact
	// signed 64-bit values with no precision loss or sign flip.
	got := decodeAggregateResults(t, stdout)
	wantRows := []AggregateResult{
		{Key: "k", Start: M - 11, End: M - 4, Count: 1, Sum: 2},
		{Key: "k", Start: M - 7, End: M, Count: 2, Sum: 5},
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

// With a window length of math.MaxInt64 and a short slide, events near time
// zero belong only to the window starting at zero. No negative-start window
// may be generated, and the [0,MaxInt64) window must not be dropped; its end
// is exactly MaxInt64 and closes at a watermark equal to it.
func TestAggregateSlidingBigWindowLengthNoNegativeStarts(t *testing.T) {
	const M = math.MaxInt64
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":0,"value":5}`,
		`{"type":"event","key":"k","time":3,"value":7}`,
		fmt.Sprintf(`{"type":"watermark","time":%d}`, M),
	}, "\n")
	stdout, stderr, err := runSliding(t, input, M, 4)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stderr != "" {
		t.Fatalf("unexpected stderr: %q", stderr)
	}
	// Exactly one row proves the clamp at start zero: without it, grids such
	// as [-4,MaxInt64-4) would also contain these events and close here,
	// producing extra rows and double counting.
	want := fmt.Sprintf(`{"key":"k","start":0,"end":%d,"count":2,"sum":12}`+"\n", M)
	if stdout != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, want)
	}
	got := decodeAggregateResults(t, stdout)
	if len(got) != 1 || got[0] != (AggregateResult{Key: "k", Start: 0, End: M, Count: 2, Sum: 12}) {
		t.Fatalf("decoded rows = %v", got)
	}
}

// While the watermark is still below M-3, an event at M-3 (window length 7,
// slide 4) falls into both a representable window [M-7,M) and one whose end
// overflows [M-3,M+4). The whole input line must fail fatally: the error
// carries the physical line number (blank lines still count), says "window
// end overflow" and names the earliest offending start M-3. Output already
// written stays, the rejected event updates no window, and a later watermark
// is never processed.
func TestAggregateSlidingBigTimeWindowEndOverflowFatal(t *testing.T) {
	const M = math.MaxInt64
	const w, sl int64 = 7, 4
	input := strings.Join([]string{
		``, // line 1 blank: physical line numbers still count it
		fmt.Sprintf(`{"type":"event","key":"k","time":%d,"value":2}`, M-5),
		fmt.Sprintf(`{"type":"event","key":"k","time":%d,"value":3}`, M-4),
		fmt.Sprintf(`{"type":"watermark","time":%d}`, M-4), // line 4 closes [M-11,M-4)
		``, // line 5 blank
		fmt.Sprintf(`{"type":"event","key":"k","time":%d,"value":9}`, M-3), // line 6 fatal
		fmt.Sprintf(`{"type":"watermark","time":%d}`, M),                   // line 7 must never be processed
	}, "\n")
	stdout, _, err := runSliding(t, input, w, sl)
	if err == nil {
		t.Fatal("expected window end overflow error, got nil")
	}
	inputErr, ok := err.(*InputError)
	if !ok {
		t.Fatalf("expected *InputError, got %T: %v", err, err)
	}
	if inputErr.Line != 6 {
		t.Errorf("line = %d, want 6 (blank lines must count)", inputErr.Line)
	}
	if !strings.Contains(inputErr.Reason, "window end overflow") {
		t.Fatalf("reason = %q, want window end overflow", inputErr.Reason)
	}
	wantStart := fmt.Sprintf("window starting at %d with length %d", M-3, w)
	if !strings.Contains(inputErr.Reason, wantStart) {
		t.Fatalf("reason = %q, want it to name %s", inputErr.Reason, wantStart)
	}
	// The still-fitting window [M-7,M) is not the offender.
	if strings.Contains(inputErr.Reason, fmt.Sprintf("starting at %d", M-7)) {
		t.Errorf("reason must not blame the fitting window start %d: %q", M-7, inputErr.Reason)
	}
	// Prior output is retained exactly; the post-error watermark never runs,
	// so [M-7,M) must not appear even though M would close it.
	want := fmt.Sprintf(`{"key":"k","start":%d,"end":%d,"count":1,"sum":2}`+"\n", M-11, M-4)
	if stdout != want {
		t.Fatalf("existing output must be retained with nothing added:\n got: %q\nwant: %q", stdout, want)
	}
}

// At the big boundary, slide equal to the window length must stay exactly
// identical to RunAggregate's fixed-window behavior: a legal end at
// math.MaxInt64 succeeds through both entry points, and a window end past
// math.MaxInt64 is rejected with the same error from both.
func TestAggregateSlidingBigTimeEqualsFixedWindow(t *testing.T) {
	const M = math.MaxInt64

	okInput := strings.Join([]string{
		`{"type":"event","key":"k","time":0,"value":11}`,
		fmt.Sprintf(`{"type":"watermark","time":%d}`, M),
	}, "\n")
	slidingOut, _, err := runSliding(t, okInput, M, M)
	if err != nil {
		t.Fatalf("sliding: unexpected error: %v", err)
	}
	var fixedOut bytes.Buffer
	if err := RunAggregate(strings.NewReader(okInput), M, &fixedOut, &bytes.Buffer{}); err != nil {
		t.Fatalf("fixed: unexpected error: %v", err)
	}
	if slidingOut != fixedOut.String() {
		t.Fatalf("slide==window output %q != fixed output %q", slidingOut, fixedOut.String())
	}
	want := fmt.Sprintf(`{"key":"k","start":0,"end":%d,"count":1,"sum":11}`+"\n", M)
	if slidingOut != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", slidingOut, want)
	}

	// Window length 7 (M is divisible by 7): time M belongs to the fixed
	// window [M,M+7), whose end overflows. Both entry points must report the
	// same fatal line and offending start.
	badInput := fmt.Sprintf(`{"type":"event","key":"k","time":%d,"value":1}`+"\n", M)
	_, _, slidingErr := runSliding(t, badInput, 7, 7)
	fixedErr := RunAggregate(strings.NewReader(badInput), 7, &bytes.Buffer{}, &bytes.Buffer{})
	if slidingErr == nil || fixedErr == nil {
		t.Fatalf("expected overflow from sliding=%v fixed=%v", slidingErr, fixedErr)
	}
	if slidingErr.Error() != fixedErr.Error() {
		t.Fatalf("sliding error %q != fixed error %q", slidingErr, fixedErr)
	}
	if !strings.Contains(slidingErr.Error(), "window end overflow") {
		t.Fatalf("error = %q, want window end overflow", slidingErr.Error())
	}
	if !strings.Contains(slidingErr.Error(), fmt.Sprintf("starting at %d", M)) {
		t.Fatalf("error = %q, want offending start %d", slidingErr.Error(), M)
	}
}
