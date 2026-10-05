package edgefleet

import (
	"strings"
	"testing"
)

// Regression: an idle partition's resume watermark must satisfy both lower
// bounds -- its own pre-idle watermark AND the current effective watermark --
// even when the partition's own bound is the higher one. Two partitions,
// window 1000, slide 600, key "sensor-a": partition 0 reported watermark 2000
// before going idle while partition 1 holds the effective watermark at 1000,
// so only [0,1000) has closed and [600,1600) is still open.
func TestAggregateSlidingIdleResumeAboveEffectiveBelowOwn(t *testing.T) {
	prefix := []string{
		`{"type":"event","key":"sensor-a","time":700,"value":2,"partition":0}`,
		`{"type":"event","key":"sensor-a","time":1000,"value":3,"partition":1}`,
		`{"type":"watermark","time":2000,"partition":0}`,
		`{"type":"watermark","time":1000,"partition":1}`, // effective = 1000: closes only [0,1000)
		``,                              // blank physical line: skipped but counted in line numbers
		`{"type":"idle","partition":0}`, // line 6: effective stays 1000 via partition 1
	}
	closed := `{"key":"sensor-a","start":0,"end":1000,"count":1,"sum":2}` + "\n"

	// Resuming at 1600 clears the effective watermark (1000) but not the
	// partition's own pre-idle watermark (2000): the resume is fatal with the
	// physical line number (blank line 5 counts) and a reason naming
	// partition 0 and the violated own-watermark bound. The already closed
	// [0,1000) output is retained, the failure is not degraded to a
	// late-event notice, and the watermark record after the failing line is
	// never processed, so it cannot close [600,1600).
	t.Run("resume below own previous watermark is fatal", func(t *testing.T) {
		lines := append(append([]string{}, prefix...),
			`{"type":"watermark","time":1600,"partition":0}`, // line 7: >= 1000 effective, < 2000 own
			`{"type":"watermark","time":5000,"partition":1}`, // would close [600,1600) if processed
		)
		stdout, stderr, err := runPartitionedSliding(t, strings.Join(lines, "\n"), 1000, 600, 2)
		if err == nil {
			t.Fatal("expected resume error, got nil")
		}
		inputErr, ok := err.(*InputError)
		if !ok {
			t.Fatalf("expected *InputError, got %T: %v", err, err)
		}
		if inputErr.Line != 7 {
			t.Errorf("line = %d, want 7 (blank line 5 counts)", inputErr.Line)
		}
		for _, sub := range []string{"partition 0", "previous watermark", "2000"} {
			if !strings.Contains(inputErr.Reason, sub) {
				t.Errorf("reason = %q, want substring %q", inputErr.Reason, sub)
			}
		}
		if stdout != closed {
			t.Fatalf("output before the error must be retained and nothing after it emitted, got %q want %q", stdout, closed)
		}
		if stderr != "" {
			t.Fatalf("failure must not degrade to a late-event notice, got stderr %q", stderr)
		}
	})

	// Resuming at exactly the partition's own pre-idle watermark (2000)
	// succeeds: equality on the own-watermark bound resumes. The effective
	// watermark stays at partition 1's 1000, so [600,1600) is not closed
	// early and the already closed [0,1000) is not re-emitted. A post-resume
	// event at time 1000 from partition 0 is counted normally -- it is
	// judged against the effective watermark 1000, not partition 0's own
	// 2000. Only when partition 1 advances to 1600 does [600,1600) close
	// with all three contributions merged, and no still-open window is
	// flushed at end of input.
	t.Run("resume at own previous watermark succeeds", func(t *testing.T) {
		lines := append(append([]string{},
			`{"type":"event","key":"sensor-a","time":700,"value":2,"partition":0}`,
			`{"type":"event","key":"sensor-a","time":1000,"value":3,"partition":1}`,
			`{"type":"watermark","time":2000,"partition":0}`,
			`{"type":"watermark","time":1000,"partition":1}`,                        // effective = 1000: closes only [0,1000)
			`{"type":"idle","partition":0}`,                                         // effective stays 1000
			`{"type":"watermark","time":2000,"partition":0}`,                        // resume at the own-watermark bound
			`{"type":"event","key":"sensor-a","time":1000,"value":4,"partition":0}`, // counted: not late vs effective 1000
			`{"type":"watermark","time":1600,"partition":1}`,                        // effective = 1600: closes [600,1600)
		))
		stdout, stderr, err := runPartitionedSliding(t, strings.Join(lines, "\n"), 1000, 600, 2)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if stderr != "" {
			t.Fatalf("unexpected stderr: %q", stderr)
		}
		want := strings.Join([]string{
			`{"key":"sensor-a","start":0,"end":1000,"count":1,"sum":2}`,
			// Pre-idle contributions (2 from p0, 3 from p1) survive the idle
			// period and merge with the post-resume event (4 from p0); the
			// result carries no partition field.
			`{"key":"sensor-a","start":600,"end":1600,"count":3,"sum":9}`,
			``,
		}, "\n")
		if stdout != want {
			t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, want)
		}
	})
}
