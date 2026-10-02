package edgefleet

import (
	"strings"
	"testing"
)

// The worked example from the idle-partition spec: with window 1000,
// partition 0 at watermark 2000 and partition 1 only holding two events at
// time 800 (sum 5), declaring partition 1 idle immediately closes k's
// [0,1000) window. Resuming partition 1 at 1999 must fail; resuming at 2000
// succeeds and the closed window is not emitted again.
func TestAggregateIdleSpecExample(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"watermark","time":2000,"partition":0}`,
		`{"type":"event","key":"k","time":800,"value":2,"partition":1}`,
		`{"type":"event","key":"k","time":800,"value":3,"partition":1}`,
		`{"type":"idle","partition":1}`, // line 4: closes [0,1000) at once
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

	// Resume below the current effective watermark (2000) fails, and the
	// already produced output survives.
	failInput := input + "\n" + `{"type":"watermark","time":1999,"partition":1}` + "\n"
	got, _, err := runPartitioned(t, failInput, 1000, 2)
	if err == nil {
		t.Fatal("expected resume at 1999 to fail")
	}
	inputErr, ok := err.(*InputError)
	if !ok {
		t.Fatalf("expected *InputError, got %T: %v", err, err)
	}
	if inputErr.Line != 5 {
		t.Errorf("line = %d, want 5", inputErr.Line)
	}
	if !strings.Contains(inputErr.Reason, "partition 1") || !strings.Contains(inputErr.Reason, "2000") {
		t.Errorf("reason = %q, want it to name partition 1 and boundary 2000", inputErr.Reason)
	}
	if got != want {
		t.Fatalf("prior output must be retained on failed resume, got %q", got)
	}

	// Resume exactly at the boundary succeeds and emits nothing new.
	okInput := input + "\n" + `{"type":"watermark","time":2000,"partition":1}` + "\n"
	got, _, err = runPartitioned(t, okInput, 1000, 2)
	if err != nil {
		t.Fatalf("resume at 2000 must succeed, got %v", err)
	}
	if got != want {
		t.Fatalf("resume must not re-emit the closed window, got %q", got)
	}
}

// An idle declaration does nothing for closure while another non-idle
// partition has never reported a watermark.
func TestAggregateIdleWaitsForOtherReporters(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":100,"value":1,"partition":0}`,
		`{"type":"watermark","time":5000,"partition":0}`,
		`{"type":"idle","partition":1}`, // partition 2 (of 3) never reported either
	}, "\n")
	stdout, _, err := runPartitioned(t, input, 1000, 3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stdout != "" {
		t.Fatalf("no effective watermark while a non-idle partition is silent, got %q", stdout)
	}
}

// Every non-idle partition having reported: the idle record itself advances
// the effective watermark to the min of the rest and closes windows.
func TestAggregateIdleClosesImmediately(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":100,"value":1,"partition":0}`,
		`{"type":"watermark","time":5000,"partition":0}`,
		`{"type":"watermark","time":2000,"partition":1}`, // min would be 2000, p2 unreported
		`{"type":"idle","partition":2}`,                  // exclude p2: min = 2000, closes [0,1000)
	}, "\n")
	stdout, _, err := runPartitioned(t, input, 1000, 3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := `{"key":"k","start":0,"end":1000,"count":1,"sum":1}` + "\n"
	if stdout != want {
		t.Fatalf("stdout = %q, want %q", stdout, want)
	}
}

// A partition that has never reported a watermark may be declared idle
// before any effective watermark exists.
func TestAggregateIdleBeforeAnyWatermark(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"idle","partition":1}`,
		`{"type":"event","key":"k","time":100,"value":4,"partition":0}`,
		`{"type":"watermark","time":1000,"partition":0}`, // only p0 active: effective = 1000
	}, "\n")
	stdout, _, err := runPartitioned(t, input, 1000, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := `{"key":"k","start":0,"end":1000,"count":1,"sum":4}` + "\n"
	if stdout != want {
		t.Fatalf("stdout = %q, want %q", stdout, want)
	}
}

// Repeated idle declarations succeed and never produce duplicate output.
func TestAggregateIdleRepeatedIsNoop(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":100,"value":1,"partition":0}`,
		`{"type":"watermark","time":1000,"partition":0}`,
		`{"type":"idle","partition":1}`, // closes [0,1000)
		`{"type":"idle","partition":1}`,
		`{"type":"idle","partition":1}`,
	}, "\n")
	stdout, _, err := runPartitioned(t, input, 1000, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := `{"key":"k","start":0,"end":1000,"count":1,"sum":1}` + "\n"
	if stdout != want {
		t.Fatalf("stdout = %q, want %q", stdout, want)
	}
}

// When all partitions go idle after an effective watermark existed, the last
// effective watermark is retained; after a partition resumes at the boundary,
// events below the retained watermark are still judged late.
func TestAggregateAllIdleRetainsWatermark(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":100,"value":1,"partition":0}`,
		`{"type":"watermark","time":1000,"partition":0}`,
		`{"type":"watermark","time":1000,"partition":1}`,                // effective = 1000, closes [0,1000)
		`{"type":"idle","partition":0}`,                                 // only p1 active, effective stays 1000
		`{"type":"idle","partition":1}`,                                 // all idle: retained 1000, not infinite
		`{"type":"watermark","time":1000,"partition":0}`,                // resume at boundary; [1000,2000) stays open
		`{"type":"event","key":"k","time":999,"value":1,"partition":0}`, // late vs retained 1000
	}, "\n")
	stdout, stderr, err := runPartitioned(t, input, 1000, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	wantOut := `{"key":"k","start":0,"end":1000,"count":1,"sum":1}` + "\n"
	if stdout != wantOut {
		t.Fatalf("stdout = %q, want %q", stdout, wantOut)
	}
	wantErr := "line 7: late event time=999 below current watermark 1000, skipped\n"
	if stderr != wantErr {
		t.Fatalf("stderr = %q, want %q", stderr, wantErr)
	}
}

// All idle before any effective watermark was ever produced: it stays
// unknown rather than becoming an infinite watermark. When a partition
// resumes alone the effective watermark is its value (the min over the
// remaining active set).
func TestAggregateAllIdleNeverProducedStaysUnknown(t *testing.T) {
	// Internal state: every partition idle with no watermark ever reported
	// yields no effective watermark.
	s := &aggregateState{
		windowMillis:  1000,
		windows:       make(map[windowID]*windowState),
		partitions:    2,
		partWatermark: make(map[int64]*int64),
		idle:          map[int64]bool{0: true, 1: true},
	}
	if s.effectiveWatermark() != nil {
		t.Fatal("effective watermark must stay unknown when all idle and none ever reported")
	}

	// Through the public API: resuming one partition sets the effective
	// watermark to its value; watermark 1000 closes no window.
	input := strings.Join([]string{
		`{"type":"idle","partition":0}`,
		`{"type":"idle","partition":1}`,
		`{"type":"watermark","time":1000,"partition":0}`, // resume: active set {p0} -> 1000
	}, "\n")
	stdout, _, err := runPartitioned(t, input, 1000, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stdout != "" {
		t.Fatalf("watermark 1000 closes no window, got %q", stdout)
	}
}

// An event for an idle partition is fatal; it is never accepted as a late
// skip and never resumes the partition.
func TestAggregateEventWhileIdleFatal(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":100,"value":1,"partition":0}`,
		`{"type":"watermark","time":1000,"partition":0}`,
		`{"type":"idle","partition":1}`,                                  // closes [0,1000)
		`{"type":"event","key":"k","time":1500,"value":1,"partition":1}`, // line 4 fatal
	}, "\n")
	stdout, stderr, err := runPartitioned(t, input, 1000, 2)
	if err == nil {
		t.Fatal("expected error for event on idle partition")
	}
	inputErr, ok := err.(*InputError)
	if !ok {
		t.Fatalf("expected *InputError, got %T: %v", err, err)
	}
	if inputErr.Line != 4 {
		t.Errorf("line = %d, want 4", inputErr.Line)
	}
	if !strings.Contains(inputErr.Reason, "idle partition 1") {
		t.Errorf("reason = %q, want it to name idle partition 1", inputErr.Reason)
	}
	if stderr != "" {
		t.Fatalf("idle event must be fatal, not a late skip, got stderr %q", stderr)
	}
	want := `{"key":"k","start":0,"end":1000,"count":1,"sum":1}` + "\n"
	if stdout != want {
		t.Fatalf("earlier output retained, got %q want %q", stdout, want)
	}
}

// Resume validation: the resume time must be at least the partition's own
// previous watermark and the current effective watermark; equality resumes.
func TestAggregateIdleResumeBoundaries(t *testing.T) {
	cases := []struct {
		name      string
		lines     []string
		resume    string
		wantError bool
		wantInMsg string
	}{
		{
			"below own previous",
			[]string{
				`{"type":"watermark","time":5000,"partition":0}`,
				`{"type":"watermark","time":2000,"partition":1}`,
				`{"type":"idle","partition":1}`, // effective jumps to 5000
			},
			`{"type":"watermark","time":1999,"partition":1}`, // < own previous 2000
			true, "previous watermark",
		},
		{
			"above own previous but below effective",
			[]string{
				`{"type":"watermark","time":5000,"partition":0}`,
				`{"type":"watermark","time":2000,"partition":1}`,
				`{"type":"idle","partition":1}`, // effective jumps to 5000
			},
			`{"type":"watermark","time":3000,"partition":1}`, // >= 2000 own, < 5000 effective
			true, "effective watermark",
		},
		{
			"equal to own previous and effective",
			[]string{
				`{"type":"watermark","time":2000,"partition":0}`,
				`{"type":"watermark","time":2000,"partition":1}`,
				`{"type":"idle","partition":1}`, // effective stays 2000
			},
			`{"type":"watermark","time":2000,"partition":1}`, // equal to both
			false, "",
		},
		{
			"equal to advanced effective",
			[]string{
				`{"type":"watermark","time":5000,"partition":0}`,
				`{"type":"watermark","time":2000,"partition":1}`,
				`{"type":"idle","partition":1}`,
			},
			`{"type":"watermark","time":5000,"partition":1}`,
			false, "",
		},
		{
			"above both",
			[]string{
				`{"type":"watermark","time":5000,"partition":0}`,
				`{"type":"watermark","time":2000,"partition":1}`,
				`{"type":"idle","partition":1}`,
			},
			`{"type":"watermark","time":6000,"partition":1}`,
			false, "",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			input := strings.Join(append(tc.lines, tc.resume), "\n") + "\n"
			_, _, err := runPartitioned(t, input, 1000, 2)
			if tc.wantError && err == nil {
				t.Fatal("expected error, got nil")
			}
			if !tc.wantError && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.wantError {
				inputErr, ok := err.(*InputError)
				if !ok {
					t.Fatalf("expected *InputError, got %T: %v", err, err)
				}
				if inputErr.Line != len(tc.lines)+1 {
					t.Errorf("line = %d, want %d", inputErr.Line, len(tc.lines)+1)
				}
				if !strings.Contains(inputErr.Reason, tc.wantInMsg) {
					t.Errorf("reason = %q, want substring %q", inputErr.Reason, tc.wantInMsg)
				}
			}
		})
	}
}

// A partition that goes idle before ever reporting has no previous watermark:
// its resume only has to reach the current effective watermark. The resume
// recomputes the effective watermark as the min and never re-emits windows
// already closed while the partition was idle.
func TestAggregateIdleNeverReportedResume(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":100,"value":2,"partition":0}`,
		`{"type":"event","key":"k","time":1100,"value":3,"partition":0}`,
		`{"type":"idle","partition":1}`,                  // p1 never reported; effective still unknown
		`{"type":"watermark","time":9000,"partition":0}`, // active set {p0}: closes both windows
		`{"type":"watermark","time":9000,"partition":1}`, // resume never-reported p1: min = 9000
	}, "\n")
	stdout, _, err := runPartitioned(t, input, 1000, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := strings.Join([]string{
		`{"key":"k","start":0,"end":1000,"count":1,"sum":2}`,
		`{"key":"k","start":1000,"end":2000,"count":1,"sum":3}`,
		``,
	}, "\n")
	if stdout != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, want)
	}

	// Even without a previous watermark of its own, resuming below the
	// current effective watermark fails.
	below := strings.Join([]string{
		`{"type":"idle","partition":1}`,
		`{"type":"watermark","time":9000,"partition":0}`, // effective 9000 while p1 idle
		`{"type":"watermark","time":8999,"partition":1}`, // resume below effective: line 3 fatal
	}, "\n")
	_, _, err = runPartitioned(t, below, 1000, 2)
	if err == nil {
		t.Fatal("expected resume below effective watermark to fail")
	}
	inputErr := err.(*InputError)
	if inputErr.Line != 3 {
		t.Errorf("line = %d, want 3", inputErr.Line)
	}
	if !strings.Contains(inputErr.Reason, "effective watermark") {
		t.Errorf("reason = %q, want effective watermark bound", inputErr.Reason)
	}
}

// A resumed partition participates again: its lower watermark pulls the
// effective watermark back down to the min, and a regression afterwards is
// still fatal.
func TestAggregateIdleResumeRejoinsMinimum(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":100,"value":1,"partition":0}`,
		`{"type":"watermark","time":5000,"partition":0}`,
		`{"type":"idle","partition":1}`,                  // effective = 5000, [0,1000) closes
		`{"type":"watermark","time":5000,"partition":1}`, // resume: min = 5000
		`{"type":"watermark","time":6000,"partition":0}`, // min stays 5000
		`{"type":"watermark","time":4999,"partition":1}`, // regression: fatal line 6
	}, "\n")
	stdout, _, err := runPartitioned(t, input, 1000, 2)
	if err == nil {
		t.Fatal("expected watermark regression error after resume")
	}
	inputErr := err.(*InputError)
	if inputErr.Line != 6 {
		t.Errorf("line = %d, want 6", inputErr.Line)
	}
	want := `{"key":"k","start":0,"end":1000,"count":1,"sum":1}` + "\n"
	if stdout != want {
		t.Fatalf("stdout = %q, want %q", stdout, want)
	}
}

// idle uses the same integer partition requirements as other records:
// missing, mistyped and out-of-range partitions are fatal with line numbers.
func TestAggregateIdlePartitionFieldErrors(t *testing.T) {
	cases := []struct {
		name      string
		input     string
		wantLine  int
		wantInMsg string
	}{
		{"missing partition", `{"type":"idle"}` + "\n", 1, `"partition"`},
		{"string partition", `{"type":"idle","partition":"1"}` + "\n", 1, `"partition"`},
		{"float partition", `{"type":"idle","partition":1.5}` + "\n", 1, `"partition"`},
		{"boolean partition", `{"type":"idle","partition":false}` + "\n", 1, `"partition"`},
		{"null partition", `{"type":"idle","partition":null}` + "\n", 1, `"partition"`},
		{"partition equals count", `{"type":"idle","partition":2}` + "\n", 1, "[0,2)"},
		{"negative partition", `{"type":"idle","partition":-1}` + "\n", 1, "[0,2)"},
		{
			"out of range later line",
			"\n" + `{"type":"idle","partition":0}` + "\n" + `{"type":"idle","partition":9}` + "\n",
			3, "[0,2)",
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

// Single-watermark (legacy) mode treats idle as an unknown record type.
func TestAggregateIdleUnknownInLegacyMode(t *testing.T) {
	input := `{"type":"idle","partition":0}` + "\n"
	_, _, err := runAggregate(t, input, 1000)
	if err == nil {
		t.Fatal("expected unknown record type error in legacy mode")
	}
	inputErr, ok := err.(*InputError)
	if !ok {
		t.Fatalf("expected *InputError, got %T: %v", err, err)
	}
	if !strings.Contains(inputErr.Reason, "unknown record type") {
		t.Errorf("reason = %q, want unknown record type", inputErr.Reason)
	}
}
