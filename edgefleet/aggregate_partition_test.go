package edgefleet

import (
	"bytes"
	"math"
	"strings"
	"testing"
)

func runPartitioned(t *testing.T, input string, window, partitions int64) (string, string, error) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	err := RunAggregatePartitioned(strings.NewReader(input), window, partitions, &stdout, &stderr)
	return stdout.String(), stderr.String(), err
}

// The worked example from the spec: partition 0's early watermark must not
// close the window; partition 1's watermark completes the pair and the min
// closes [0,1000) with both events merged; a repeated watermark does not
// emit the window again.
func TestAggregatePartitionedSpecExample(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":100,"value":2,"partition":0}`,
		`{"type":"watermark","time":2000,"partition":0}`,
		`{"type":"event","key":"k","time":800,"value":3,"partition":1}`,
		`{"type":"watermark","time":1000,"partition":1}`,
		`{"type":"watermark","time":2000,"partition":0}`, // repeat: no duplicate
	}, "\n")
	stdout, stderr, err := runPartitioned(t, input, 1000, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stderr != "" {
		t.Fatalf("unexpected stderr: %q", stderr)
	}
	want := `{"key":"k","start":0,"end":1000,"count":2,"sum":5}` + "\n"
	if stdout != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, want)
	}
}

// Before every partition has reported at least once there is no effective
// watermark: nothing closes and no event is judged late.
func TestAggregatePartitionedNoWatermarkUntilAllReport(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"watermark","time":5000,"partition":0}`,
		`{"type":"event","key":"k","time":100,"value":1,"partition":1}`,
		`{"type":"event","key":"k","time":999,"value":1,"partition":0}`,
	}, "\n")
	stdout, stderr, err := runPartitioned(t, input, 1000, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stdout != "" {
		t.Fatalf("no window may close before all partitions report, got %q", stdout)
	}
	if stderr != "" {
		t.Fatalf("no late judgment before all partitions report, got %q", stderr)
	}
}

// A single partition's larger watermark must not close windows or drop
// events; only the min does.
func TestAggregatePartitionedMinWatermarkClosure(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":150,"value":1,"partition":0}`,
		`{"type":"event","key":"k","time":250,"value":1,"partition":1}`,
		`{"type":"watermark","time":5000,"partition":0}`, // p0 reports first: no effective watermark yet
		`{"type":"watermark","time":100,"partition":1}`,  // min = 100, window [0,1000) stays open
		`{"type":"watermark","time":5000,"partition":0}`, // p0 repeats, min stays 100, still no close
		`{"type":"watermark","time":1000,"partition":1}`, // min = 1000, closes [0,1000)
	}, "\n")
	stdout, stderr, err := runPartitioned(t, input, 1000, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stderr != "" {
		t.Fatalf("unexpected stderr: %q", stderr)
	}
	want := `{"key":"k","start":0,"end":1000,"count":2,"sum":2}` + "\n"
	if stdout != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, want)
	}
}

// Late-event checks use the min: an event below the min is late, an event
// exactly at the min is valid.
func TestAggregatePartitionedLateAndBoundary(t *testing.T) {
	input := strings.Join([]string{
		``, // line 1 blank
		`{"type":"watermark","time":2000,"partition":0}`,
		``, // line 3 blank
		`{"type":"watermark","time":1000,"partition":1}`,                 // line 4, min = 1000
		`{"type":"event","key":"k","time":999,"value":1,"partition":0}`,  // line 5 late
		`{"type":"event","key":"k","time":1000,"value":5,"partition":1}`, // line 6 boundary
	}, "\n")
	stdout, stderr, err := runPartitioned(t, input, 1000, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stdout != "" {
		t.Fatalf("window [1000,2000) must stay open, got %q", stdout)
	}
	wantErr := "line 5: late event time=999 below current watermark 1000, skipped\n"
	if stderr != wantErr {
		t.Fatalf("stderr = %q, want %q", stderr, wantErr)
	}
}

// Events from different partitions for the same key and window merge into
// one count and sum; the partition field never reaches output.
func TestAggregatePartitionedMergesAcrossPartitions(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"a","time":100,"value":2,"partition":0}`,
		`{"type":"event","key":"a","time":200,"value":3,"partition":1}`,
		`{"type":"event","key":"a","time":300,"value":-1,"partition":2}`,
		`{"type":"event","key":"b","time":400,"value":7,"partition":0}`,
		`{"type":"watermark","time":1000,"partition":0}`,
		`{"type":"watermark","time":1000,"partition":1}`,
		`{"type":"watermark","time":1000,"partition":2}`,
	}, "\n")
	stdout, _, err := runPartitioned(t, input, 1000, 3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := strings.Join([]string{
		`{"key":"a","start":0,"end":1000,"count":3,"sum":4}`,
		`{"key":"b","start":0,"end":1000,"count":1,"sum":7}`,
		``,
	}, "\n")
	if stdout != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, want)
	}
}

// Closure ordering (end ascending, then key) still applies in partition mode.
func TestAggregatePartitionedClosureOrder(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"z","time":1500,"value":1,"partition":0}`,
		`{"type":"event","key":"a","time":500,"value":1,"partition":1}`,
		`{"type":"event","key":"a","time":1200,"value":1,"partition":0}`,
		`{"type":"watermark","time":2000,"partition":0}`,
		`{"type":"watermark","time":2000,"partition":1}`,
	}, "\n")
	stdout, _, err := runPartitioned(t, input, 1000, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := strings.Join([]string{
		`{"key":"a","start":0,"end":1000,"count":1,"sum":1}`,
		`{"key":"a","start":1000,"end":2000,"count":1,"sum":1}`,
		`{"key":"z","start":1000,"end":2000,"count":1,"sum":1}`,
		``,
	}, "\n")
	if stdout != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, want)
	}
}

// Per-partition watermarks may repeat or jump forward, but never move
// backwards — even when the regression would not change the overall min.
func TestAggregatePartitionedWatermarkRegression(t *testing.T) {
	cases := []struct {
		name      string
		input     string
		wantLine  int
		wantInMsg string
	}{
		{
			"regression changes min",
			strings.Join([]string{
				`{"type":"watermark","time":100,"partition":0}`,
				`{"type":"watermark","time":100,"partition":1}`,
				`{"type":"watermark","time":50,"partition":0}`,
			}, "\n"),
			3, "partition 0",
		},
		{
			"regression hidden behind min",
			strings.Join([]string{
				`{"type":"watermark","time":100,"partition":0}`,
				`{"type":"watermark","time":100,"partition":1}`,
				`{"type":"watermark","time":200,"partition":1}`, // p1 jumps, min stays 100
				`{"type":"watermark","time":150,"partition":1}`, // p1 regresses 200 -> 150
			}, "\n"),
			4, "partition 1",
		},
		{
			"regression before all report",
			strings.Join([]string{
				`{"type":"watermark","time":100,"partition":0}`,
				`{"type":"watermark","time":90,"partition":0}`,
			}, "\n"),
			2, "partition 0",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := runPartitioned(t, tc.input, 1000, 2)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			inputErr, ok := err.(*InputError)
			if !ok {
				t.Fatalf("expected *InputError, got %T: %v", err, err)
			}
			if inputErr.Line != tc.wantLine {
				t.Errorf("line = %d, want %d", inputErr.Line, tc.wantLine)
			}
			if !strings.Contains(inputErr.Reason, tc.wantInMsg) {
				t.Errorf("reason = %q, want substring %q", inputErr.Reason, tc.wantInMsg)
			}
		})
	}
}

// Missing, mistyped and out-of-range partition fields are fatal with the
// physical line number.
func TestAggregatePartitionedPartitionFieldErrors(t *testing.T) {
	cases := []struct {
		name      string
		input     string
		wantLine  int
		wantInMsg string
	}{
		{"event missing partition", `{"type":"event","key":"k","time":1,"value":1}` + "\n", 1, `"partition"`},
		{"watermark missing partition", `{"type":"watermark","time":100}` + "\n", 1, `"partition"`},
		{"string partition", `{"type":"event","key":"k","time":1,"value":1,"partition":"0"}` + "\n", 1, `"partition"`},
		{"float partition", `{"type":"event","key":"k","time":1,"value":1,"partition":0.5}` + "\n", 1, `"partition"`},
		{"boolean partition", `{"type":"event","key":"k","time":1,"value":1,"partition":true}` + "\n", 1, `"partition"`},
		{"null partition", `{"type":"watermark","time":100,"partition":null}` + "\n", 1, `"partition"`},
		{"partition equals count", `{"type":"event","key":"k","time":1,"value":1,"partition":2}` + "\n", 1, "[0,2)"},
		{"negative partition", `{"type":"event","key":"k","time":1,"value":1,"partition":-1}` + "\n", 1, "[0,2)"},
		{"partition out of range later line", "\n" + `{"type":"watermark","time":100,"partition":0}` + "\n" + `{"type":"event","key":"k","time":1,"value":1,"partition":9}` + "\n", 3, "[0,2)"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := runPartitioned(t, tc.input, 1000, 2)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			inputErr, ok := err.(*InputError)
			if !ok {
				t.Fatalf("expected *InputError, got %T: %v", err, err)
			}
			if inputErr.Line != tc.wantLine {
				t.Errorf("line = %d, want %d", inputErr.Line, tc.wantLine)
			}
			if !strings.Contains(inputErr.Reason, tc.wantInMsg) {
				t.Errorf("reason = %q, want substring %q", inputErr.Reason, tc.wantInMsg)
			}
		})
	}
}

// Explicit --partitions 1 is partition mode: every record must carry
// partition 0, and the effective watermark tracks it.
func TestAggregatePartitionedOnePartition(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":100,"value":2,"partition":0}`,
		`{"type":"watermark","time":1000,"partition":0}`,
	}, "\n")
	stdout, _, err := runPartitioned(t, input, 1000, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := `{"key":"k","start":0,"end":1000,"count":1,"sum":2}` + "\n"
	if stdout != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, want)
	}

	// Missing partition is still an error with a single partition.
	noPartition := `{"type":"event","key":"k","time":100,"value":2}` + "\n"
	_, _, err = runPartitioned(t, noPartition, 1000, 1)
	if err == nil {
		t.Fatal("expected error for missing partition with count 1")
	}
}

// Legacy mode (partitions == 0) keeps the single-watermark behavior and
// ignores extra fields, including a stray partition field.
func TestAggregateLegacyIgnoresPartitionField(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":100,"value":2,"partition":7}`,
		`{"type":"watermark","time":1000,"partition":9}`,
	}, "\n")
	stdout, _, err := runPartitioned(t, input, 1000, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := `{"key":"k","start":0,"end":1000,"count":1,"sum":2}` + "\n"
	if stdout != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, want)
	}
}

// Results already written stay written when a later record fails, and no
// further records are processed.
func TestAggregatePartitionedOutputRetainedOnError(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":0,"value":1,"partition":0}`,
		`{"type":"watermark","time":1000,"partition":0}`,
		`{"type":"watermark","time":1000,"partition":1}`, // closes [0,1000)
		`{"type":"watermark","time":900,"partition":1}`,  // regression, line 4
		`{"type":"event","key":"k","time":1,"value":9,"partition":0}`,
	}, "\n")
	stdout, _, err := runPartitioned(t, input, 1000, 2)
	if err == nil {
		t.Fatal("expected error")
	}
	if got := stdout; got != `{"key":"k","start":0,"end":1000,"count":1,"sum":1}`+"\n" {
		t.Fatalf("earlier output must be retained, got %q", got)
	}
}

// Open windows are not flushed at end of input in partition mode either.
func TestAggregatePartitionedNoFlushAtEOF(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"watermark","time":1000,"partition":0}`,
		`{"type":"event","key":"k","time":1500,"value":1,"partition":1}`,
	}, "\n")
	stdout, _, err := runPartitioned(t, input, 1000, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stdout != "" {
		t.Fatalf("open windows must not flush at EOF, got %q", stdout)
	}
}

// Existing record validation still applies in partition mode.
func TestAggregatePartitionedRecordValidation(t *testing.T) {
	cases := []struct {
		name      string
		input     string
		wantLine  int
		wantInMsg string
	}{
		{"negative time", `{"type":"event","key":"k","time":-1,"value":1,"partition":0}` + "\n", 1, "non-negative"},
		{"missing value", `{"type":"event","key":"k","time":1,"partition":0}` + "\n", 1, `"value"`},
		{"empty key", `{"type":"event","key":"","time":1,"value":1,"partition":0}` + "\n", 1, "non-empty"},
		{"watermark negative", `{"type":"watermark","time":-1,"partition":0}` + "\n", 1, "non-negative"},
		{"watermark time string", `{"type":"watermark","time":"100","partition":0}` + "\n", 1, `"time"`},
		{"unknown record type", `{"type":"heartbeat","partition":0}` + "\n", 1, "unknown record type"},
		{"malformed json", `{not json` + "\n", 1, "invalid JSON"},
		{
			"sum overflow",
			`{"type":"event","key":"k","time":1,"value":9223372036854775807,"partition":0}` + "\n" +
				`{"type":"event","key":"k","time":2,"value":1,"partition":1}` + "\n",
			2, "sum overflow",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := runPartitioned(t, tc.input, 3, 2)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			inputErr, ok := err.(*InputError)
			if !ok {
				t.Fatalf("expected *InputError, got %T: %v", err, err)
			}
			if inputErr.Line != tc.wantLine {
				t.Errorf("line = %d, want %d", inputErr.Line, tc.wantLine)
			}
			if !strings.Contains(inputErr.Reason, tc.wantInMsg) {
				t.Errorf("reason = %q, want substring %q", inputErr.Reason, tc.wantInMsg)
			}
		})
	}
}

// Invalid library arguments are rejected before any input is read.
func TestAggregatePartitionedInvalidArguments(t *testing.T) {
	for _, w := range []int64{0, -1, math.MinInt64} {
		if err := RunAggregatePartitioned(strings.NewReader(""), w, 2, &bytes.Buffer{}, &bytes.Buffer{}); err == nil {
			t.Fatalf("expected error for window %d", w)
		}
	}
	for _, p := range []int64{-1, math.MinInt64} {
		if err := RunAggregatePartitioned(strings.NewReader(""), 1000, p, &bytes.Buffer{}, &bytes.Buffer{}); err == nil {
			t.Fatalf("expected error for partition count %d", p)
		}
	}
}

// Determinism: repeated partitioned runs are identical.
func TestAggregatePartitionedDeterministic(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"zeta","time":200,"value":1,"partition":0}`,
		`{"type":"event","key":"alpha","time":100,"value":-1,"partition":1}`,
		`{"type":"event","key":"alpha","time":1200,"value":2,"partition":0}`,
		`{"type":"watermark","time":3000,"partition":0}`,
		`{"type":"watermark","time":2000,"partition":1}`,
	}, "\n")
	first, _, err := runPartitioned(t, input, 1000, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	second, _, err := runPartitioned(t, input, 1000, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if first != second {
		t.Fatalf("repeated runs must be identical:\n%s\nvs\n%s", first, second)
	}
}

// The worked example from the spec: partition 0 has watermark 2000,
// partition 1 only 100, key k accumulated two events at time 800 summing to
// 5. Declaring partition 1 idle must immediately emit k's [0,1000) window
// with count 2 and sum 5. Resuming partition 1 with 1999 must fail, with
// 2000 must succeed, and the window must not appear again.
func TestAggregatePartitionedIdleSpecExample(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":800,"value":2,"partition":0}`,
		`{"type":"event","key":"k","time":800,"value":3,"partition":1}`,
		`{"type":"watermark","time":2000,"partition":0}`,
		`{"type":"watermark","time":100,"partition":1}`,
		`{"type":"idle","partition":1}`,
	}, "\n")
	want := `{"key":"k","start":0,"end":1000,"count":2,"sum":5}` + "\n"

	stdout, stderr, err := runPartitioned(t, input, 1000, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stderr != "" {
		t.Fatalf("unexpected stderr: %q", stderr)
	}
	if stdout != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, want)
	}

	// Resume with 1999: below the overall watermark 2000, must fail.
	bad := strings.Join([]string{
		`{"type":"event","key":"k","time":800,"value":2,"partition":0}`,
		`{"type":"event","key":"k","time":800,"value":3,"partition":1}`,
		`{"type":"watermark","time":2000,"partition":0}`,
		`{"type":"watermark","time":100,"partition":1}`,
		`{"type":"idle","partition":1}`,
		`{"type":"watermark","time":1999,"partition":1}`,
	}, "\n")
	_, _, err = runPartitioned(t, bad, 1000, 2)
	if err == nil {
		t.Fatal("expected error for resume at 1999")
	}
	inputErr, ok := err.(*InputError)
	if !ok {
		t.Fatalf("expected *InputError, got %T: %v", err, err)
	}
	if inputErr.Line != 6 {
		t.Errorf("line = %d, want 6", inputErr.Line)
	}
	if !strings.Contains(inputErr.Reason, "partition 1") || !strings.Contains(inputErr.Reason, "2000") {
		t.Errorf("reason = %q, want partition 1 and bound 2000", inputErr.Reason)
	}

	// Resume with 2000: equal to the overall watermark, must succeed and
	// must not re-emit the already closed window.
	good := strings.Join([]string{
		`{"type":"event","key":"k","time":800,"value":2,"partition":0}`,
		`{"type":"event","key":"k","time":800,"value":3,"partition":1}`,
		`{"type":"watermark","time":2000,"partition":0}`,
		`{"type":"watermark","time":100,"partition":1}`,
		`{"type":"idle","partition":1}`,
		`{"type":"watermark","time":2000,"partition":1}`,
	}, "\n")
	stdout2, _, err := runPartitioned(t, good, 1000, 2)
	if err != nil {
		t.Fatalf("unexpected error for resume at 2000: %v", err)
	}
	if stdout2 != want {
		t.Fatalf("stdout mismatch after resume:\n got: %q\nwant: %q", stdout2, want)
	}
}

// A partition that has never reported a watermark can be declared idle;
// the idle record itself lets the other partition's watermark drive closure.
func TestAggregatePartitionedIdleBeforeAnyWatermark(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":800,"value":5,"partition":0}`,
		`{"type":"watermark","time":2000,"partition":0}`,
		`{"type":"idle","partition":1}`,
	}, "\n")
	stdout, _, err := runPartitioned(t, input, 1000, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := `{"key":"k","start":0,"end":1000,"count":1,"sum":5}` + "\n"
	if stdout != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, want)
	}
}

// If a non-idle partition has not reported, idle must not close windows
// early. Once it reports, the watermark advances and closes.
func TestAggregatePartitionedIdleDoesNotCloseWhenOtherUnreported(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":800,"value":5,"partition":0}`,
		`{"type":"watermark","time":2000,"partition":0}`,
		`{"type":"idle","partition":0}`,
	}, "\n")
	stdout, _, err := runPartitioned(t, input, 1000, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stdout != "" {
		t.Fatalf("no window may close while a non-idle partition is unreported, got %q", stdout)
	}

	again := strings.Join([]string{
		`{"type":"event","key":"k","time":800,"value":5,"partition":0}`,
		`{"type":"watermark","time":2000,"partition":0}`,
		`{"type":"idle","partition":0}`,
		`{"type":"watermark","time":2000,"partition":1}`,
	}, "\n")
	stdout2, _, err := runPartitioned(t, again, 1000, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := `{"key":"k","start":0,"end":1000,"count":1,"sum":5}` + "\n"
	if stdout2 != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout2, want)
	}
}

// All partitions idle with no prior watermark must keep the overall
// watermark unknown, not treat idle as infinite and flush open windows.
func TestAggregatePartitionedIdleAllIdleNoWatermarkStaysUnknown(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":800,"value":5,"partition":0}`,
		`{"type":"event","key":"k","time":900,"value":2,"partition":1}`,
		`{"type":"idle","partition":0}`,
		`{"type":"idle","partition":1}`,
	}, "\n")
	stdout, _, err := runPartitioned(t, input, 1000, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stdout != "" {
		t.Fatalf("all-idle with no prior watermark must stay unknown and not flush windows, got %q", stdout)
	}
}

// Repeated idle records for the same partition succeed without producing
// duplicate window output.
func TestAggregatePartitionedIdleRepeatedNoDuplicate(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":800,"value":5,"partition":0}`,
		`{"type":"watermark","time":2000,"partition":0}`,
		`{"type":"watermark","time":100,"partition":1}`,
		`{"type":"idle","partition":1}`,
		`{"type":"idle","partition":1}`,
		`{"type":"idle","partition":1}`,
	}, "\n")
	stdout, _, err := runPartitioned(t, input, 1000, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := `{"key":"k","start":0,"end":1000,"count":1,"sum":5}` + "\n"
	if stdout != want {
		t.Fatalf("repeated idle must not duplicate output:\n got: %q\nwant: %q", stdout, want)
	}
}

// An event received for an idle partition is fatal: it must not silently
// resume the partition or be skipped as an ordinary late event. Output
// already produced by the idle record is retained.
func TestAggregatePartitionedIdleEventWhileIdleFatal(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":800,"value":5,"partition":0}`,
		`{"type":"watermark","time":2000,"partition":0}`,
		`{"type":"watermark","time":100,"partition":1}`,
		`{"type":"idle","partition":1}`,
		`{"type":"event","key":"k","time":900,"value":1,"partition":1}`,
	}, "\n")
	stdout, _, err := runPartitioned(t, input, 1000, 2)
	if err == nil {
		t.Fatal("expected error for event while partition idle")
	}
	inputErr, ok := err.(*InputError)
	if !ok {
		t.Fatalf("expected *InputError, got %T: %v", err, err)
	}
	if inputErr.Line != 5 {
		t.Errorf("line = %d, want 5", inputErr.Line)
	}
	if !strings.Contains(inputErr.Reason, "partition 1") || !strings.Contains(inputErr.Reason, "idle") {
		t.Errorf("reason = %q, want partition 1 and idle", inputErr.Reason)
	}
	want := `{"key":"k","start":0,"end":1000,"count":1,"sum":5}` + "\n"
	if stdout != want {
		t.Fatalf("earlier output must be retained, got %q", stdout)
	}
}

// Resuming an idle partition must satisfy both the partition's previous
// watermark and the current effective watermark; a violation reports the
// physical line number, partition and the bound.
func TestAggregatePartitionedIdleResumeBounds(t *testing.T) {
	cases := []struct {
		name      string
		records   []string
		wantLine  int
		wantInMsg string
	}{
		{
			"resume below previous watermark",
			[]string{
				`{"type":"watermark","time":2000,"partition":0}`,
				`{"type":"watermark","time":1000,"partition":1}`,
				`{"type":"idle","partition":1}`,
				`{"type":"watermark","time":900,"partition":1}`,
			},
			4, "partition 1",
		},
		{
			"resume below overall watermark",
			[]string{
				`{"type":"watermark","time":2000,"partition":0}`,
				`{"type":"watermark","time":100,"partition":1}`,
				`{"type":"idle","partition":1}`,
				`{"type":"watermark","time":1999,"partition":1}`,
			},
			4, "2000",
		},
		{
			"resume below overall watermark with no prior watermark",
			[]string{
				`{"type":"watermark","time":2000,"partition":0}`,
				`{"type":"idle","partition":1}`,
				`{"type":"watermark","time":1999,"partition":1}`,
			},
			3, "2000",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := runPartitioned(t, strings.Join(tc.records, "\n"), 1000, 2)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			inputErr, ok := err.(*InputError)
			if !ok {
				t.Fatalf("expected *InputError, got %T: %v", err, err)
			}
			if inputErr.Line != tc.wantLine {
				t.Errorf("line = %d, want %d", inputErr.Line, tc.wantLine)
			}
			if !strings.Contains(inputErr.Reason, tc.wantInMsg) {
				t.Errorf("reason = %q, want substring %q", inputErr.Reason, tc.wantInMsg)
			}
		})
	}
}

// Resuming exactly at the bounds (equal to the previous watermark and to
// the current effective watermark) is allowed.
func TestAggregatePartitionedIdleResumeAtBounds(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"watermark","time":2000,"partition":0}`,
		`{"type":"watermark","time":2000,"partition":1}`,
		`{"type":"idle","partition":1}`,
		`{"type":"watermark","time":2000,"partition":1}`,
	}, "\n")
	_, _, err := runPartitioned(t, input, 1000, 2)
	if err != nil {
		t.Fatalf("resume at exact bounds must succeed: %v", err)
	}
}

// After resume the partition's watermark is tracked, so a later regression
// is caught with the usual message.
func TestAggregatePartitionedIdleResumeInstallsWatermark(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"watermark","time":2000,"partition":0}`,
		`{"type":"watermark","time":100,"partition":1}`,
		`{"type":"idle","partition":1}`,
		`{"type":"watermark","time":2000,"partition":1}`,
		`{"type":"watermark","time":1500,"partition":1}`,
	}, "\n")
	_, _, err := runPartitioned(t, input, 1000, 2)
	if err == nil {
		t.Fatal("expected error for regression after resume")
	}
	inputErr, ok := err.(*InputError)
	if !ok {
		t.Fatalf("expected *InputError, got %T: %v", err, err)
	}
	if inputErr.Line != 5 {
		t.Errorf("line = %d, want 5", inputErr.Line)
	}
	if !strings.Contains(inputErr.Reason, "partition 1") || !strings.Contains(inputErr.Reason, "backwards") {
		t.Errorf("reason = %q, want partition 1 and backwards", inputErr.Reason)
	}
}

// Idle records use the same integer partition range rules as events and
// watermarks: missing, mistyped or out-of-range is fatal with the physical
// line number.
func TestAggregatePartitionedIdleFieldErrors(t *testing.T) {
	cases := []struct {
		name      string
		input     string
		wantLine  int
		wantInMsg string
	}{
		{"missing partition", `{"type":"idle"}` + "\n", 1, `"partition"`},
		{"string partition", `{"type":"idle","partition":"1"}` + "\n", 1, `"partition"`},
		{"float partition", `{"type":"idle","partition":1.5}` + "\n", 1, `"partition"`},
		{"boolean partition", `{"type":"idle","partition":true}` + "\n", 1, `"partition"`},
		{"null partition", `{"type":"idle","partition":null}` + "\n", 1, `"partition"`},
		{"partition equals count", `{"type":"idle","partition":2}` + "\n", 1, "[0,2)"},
		{"negative partition", `{"type":"idle","partition":-1}` + "\n", 1, "[0,2)"},
		{"out of range later line", "\n" + `{"type":"idle","partition":9}` + "\n", 2, "[0,2)"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := runPartitioned(t, tc.input, 1000, 2)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			inputErr, ok := err.(*InputError)
			if !ok {
				t.Fatalf("expected *InputError, got %T: %v", err, err)
			}
			if inputErr.Line != tc.wantLine {
				t.Errorf("line = %d, want %d", inputErr.Line, tc.wantLine)
			}
			if !strings.Contains(inputErr.Reason, tc.wantInMsg) {
				t.Errorf("reason = %q, want substring %q", inputErr.Reason, tc.wantInMsg)
			}
		})
	}
}

// In legacy single-watermark mode idle is still an unknown record type.
func TestAggregateLegacyIdleUnknownType(t *testing.T) {
	input := `{"type":"idle","partition":1}` + "\n"
	_, _, err := runPartitioned(t, input, 1000, 0)
	if err == nil {
		t.Fatal("expected error for idle in legacy mode")
	}
	inputErr, ok := err.(*InputError)
	if !ok {
		t.Fatalf("expected *InputError, got %T: %v", err, err)
	}
	if inputErr.Line != 1 {
		t.Errorf("line = %d, want 1", inputErr.Line)
	}
	if !strings.Contains(inputErr.Reason, "unknown record type") {
		t.Errorf("reason = %q, want unknown record type", inputErr.Reason)
	}
}

// Inputs without idle records produce identical results to before.
func TestAggregatePartitionedIdleAbsentUnchanged(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":800,"value":2,"partition":0}`,
		`{"type":"event","key":"k","time":800,"value":3,"partition":1}`,
		`{"type":"watermark","time":2000,"partition":0}`,
		`{"type":"watermark","time":1000,"partition":1}`,
	}, "\n")
	stdout, _, err := runPartitioned(t, input, 1000, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := `{"key":"k","start":0,"end":1000,"count":2,"sum":5}` + "\n"
	if stdout != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, want)
	}
}
