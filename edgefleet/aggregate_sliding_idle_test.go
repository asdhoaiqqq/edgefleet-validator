package edgefleet

import (
	"strings"
	"testing"
)

// Full idle/resume cycle over sliding windows (window 1000, slide 600, two
// partitions): declaring a partition idle only removes it from the effective
// watermark, events both partitions contributed before the idle declaration
// stay in their still-open windows, and after resuming, the partition keeps
// feeding the overlapping windows it shares with the others.
func TestAggregateSlidingIdleResumeFullCycle(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":700,"value":2,"partition":0}`,
		`{"type":"event","key":"k","time":1000,"value":3,"partition":1}`,
		`{"type":"watermark","time":1000,"partition":0}`,
		`{"type":"watermark","time":600,"partition":1}`,                  // effective = 600: nothing closes
		`{"type":"idle","partition":1}`,                                  // effective = 1000: closes only [0,1000)
		`{"type":"watermark","time":1000,"partition":1}`,                 // resume at the effective watermark: no re-emit
		`{"type":"event","key":"k","time":1000,"value":4,"partition":1}`, // joins the still-open [600,1600)
		`{"type":"watermark","time":1600,"partition":0}`,                 // min stays 1000: [600,1600) still open
		`{"type":"watermark","time":1600,"partition":1}`,                 // min = 1600: closes [600,1600)
	}, "\n")
	stdout, stderr, err := runPartitionedSliding(t, input, 1000, 600, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stderr != "" {
		t.Fatalf("unexpected stderr: %q", stderr)
	}
	want := strings.Join([]string{
		// Only partition 0's event at 700 falls in [0,1000); the events at
		// 1000 belong to the next window and must not leak back.
		`{"key":"k","start":0,"end":1000,"count":1,"sum":2}`,
		// Pre-idle contributions (2 from p0, 3 from p1) survive the idle
		// period and merge with the post-resume event (4 from p1).
		`{"key":"k","start":600,"end":1600,"count":3,"sum":9}`,
		``,
	}, "\n")
	if stdout != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, want)
	}
}

// Resume validation with sliding windows: a resume watermark below the
// partition's own pre-idle watermark, or at/above it but below the current
// effective watermark, is fatal with the physical line number (blank lines
// count) and a reason naming the violated bound. Output produced before the
// error is retained and records after the failing line produce nothing.
func TestAggregateSlidingIdleResumeErrors(t *testing.T) {
	prefix := []string{
		`{"type":"event","key":"k","time":700,"value":2,"partition":0}`,
		``, // blank physical line: skipped but counted in line numbers
		`{"type":"watermark","time":1000,"partition":0}`,
		`{"type":"watermark","time":600,"partition":1}`,
		`{"type":"idle","partition":1}`, // line 5: effective = 1000, closes [0,1000)
	}
	closed := `{"key":"k","start":0,"end":1000,"count":1,"sum":2}` + "\n"

	cases := []struct {
		name      string
		resume    string
		wantInMsg []string
	}{
		{
			"below own previous watermark",
			`{"type":"watermark","time":500,"partition":1}`, // < p1's pre-idle 600
			[]string{"partition 1", "previous watermark", "600"},
		},
		{
			"above own previous but below effective",
			`{"type":"watermark","time":800,"partition":1}`, // >= 600 own, < 1000 effective
			[]string{"partition 1", "effective watermark", "1000"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			lines := append(append([]string{}, prefix...), tc.resume)
			// A record after the failing line that would close [600,1600)
			// if it were ever processed.
			lines = append(lines, `{"type":"watermark","time":5000,"partition":0}`)
			stdout, _, err := runPartitionedSliding(t, strings.Join(lines, "\n"), 1000, 600, 2)
			if err == nil {
				t.Fatal("expected resume error, got nil")
			}
			inputErr, ok := err.(*InputError)
			if !ok {
				t.Fatalf("expected *InputError, got %T: %v", err, err)
			}
			if inputErr.Line != 6 {
				t.Errorf("line = %d, want 6 (blank line 2 counts)", inputErr.Line)
			}
			for _, sub := range tc.wantInMsg {
				if !strings.Contains(inputErr.Reason, sub) {
					t.Errorf("reason = %q, want substring %q", inputErr.Reason, sub)
				}
			}
			if stdout != closed {
				t.Fatalf("output before the error must be retained and nothing after it emitted, got %q want %q", stdout, closed)
			}
		})
	}
}

// After a successful resume the effective watermark still governs lateness:
// an event below it is reported late and skipped even though it falls inside
// the still-open overlapping window, while an event exactly at the watermark
// is counted normally.
func TestAggregateSlidingResumeLateEventRules(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":700,"value":2,"partition":0}`,
		`{"type":"event","key":"k","time":1000,"value":3,"partition":1}`,
		`{"type":"watermark","time":1000,"partition":0}`,
		`{"type":"watermark","time":600,"partition":1}`,
		`{"type":"idle","partition":1}`,                                  // effective = 1000, closes [0,1000)
		`{"type":"watermark","time":1000,"partition":1}`,                 // resume at the boundary
		`{"type":"event","key":"k","time":999,"value":1,"partition":1}`,  // late vs 1000 despite open [600,1600)
		`{"type":"event","key":"k","time":1000,"value":4,"partition":1}`, // exactly at the watermark: counted
		`{"type":"watermark","time":1600,"partition":0}`,
		`{"type":"watermark","time":1600,"partition":1}`,
	}, "\n")
	stdout, stderr, err := runPartitionedSliding(t, input, 1000, 600, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := strings.Join([]string{
		`{"key":"k","start":0,"end":1000,"count":1,"sum":2}`,
		// The late event at 999 adds nothing; the at-watermark event at
		// 1000 joins the two pre-idle contributions.
		`{"key":"k","start":600,"end":1600,"count":3,"sum":9}`,
		``,
	}, "\n")
	if stdout != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, want)
	}
	wantErr := "line 7: late event time=999 below current watermark 1000, skipped\n"
	if stderr != wantErr {
		t.Fatalf("stderr = %q, want %q", stderr, wantErr)
	}
}
