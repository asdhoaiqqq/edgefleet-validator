package edgefleet

import (
	"errors"
	"strings"
	"testing"
)

// Shared fixture for the idle-resume regression: window 1000, slide 600, two
// partitions, one key. Partition 0's event at 700 lands in both [0,1000) and
// [600,1600); partition 1's event at 1000 lands only in [600,1600). After
// partition 0 reports 2000 and partition 1 reports 1000 the effective
// watermark is 1000, so only [0,1000) closes (count 1, sum 2) while
// [600,1600) keeps both partitions' contributions. The blank physical lines
// 5 and 7 count toward line numbers but carry no record.
func slidingIdleResumeFixtureLines() []string {
	return []string{
		`{"type":"event","key":"sensor-a","time":700,"value":2,"partition":0}`,  // line 1
		`{"type":"event","key":"sensor-a","time":1000,"value":3,"partition":1}`, // line 2
		`{"type":"watermark","time":2000,"partition":0}`,                        // line 3: p0 ahead, effective still unknown
		`{"type":"watermark","time":1000,"partition":1}`,                        // line 4: effective = 1000, closes [0,1000)
		``, // line 5: blank, counts toward physical line numbers
		`{"type":"idle","partition":0}`, // line 6: p0 idle; p1 keeps effective at 1000
		``,                              // line 7: blank
	}
}

// A resume watermark that is above the current effective watermark but below
// the partition's own pre-idle watermark must still be rejected: the resume
// record has to clear both lower bounds. The error is the existing input
// error naming the physical line (blank lines count), partition 0 and the
// violated previous-watermark bound, not a late-event notice. The window
// already closed before the failed resume stays output, and watermarks after
// the failed record are never processed, so they cannot close [600,1600).
func TestAggregateSlidingIdleResumeBelowOwnOldWatermark(t *testing.T) {
	lines := append(slidingIdleResumeFixtureLines(),
		`{"type":"watermark","time":1600,"partition":0}`, // line 8: 1600 > effective 1000 but < p0's old 2000
		`{"type":"watermark","time":1600,"partition":1}`, // line 9: must never run (would close [600,1600))
	)
	stdout, stderr, err := runPartitionedSliding(t, strings.Join(lines, "\n"), 1000, 600, 2)

	var inputErr *InputError
	if err == nil {
		t.Fatal("expected an input error for the too-low resume record, got nil")
	}
	if !errors.As(err, &inputErr) {
		t.Fatalf("expected *InputError, got %T: %v", err, err)
	}
	if inputErr.Line != 8 {
		t.Errorf("Line = %d, want 8 (blank physical lines 5 and 7 count)", inputErr.Line)
	}
	// The violated bound is the partition's own previous watermark, even
	// though 1600 is already past the effective watermark 1000.
	for _, sub := range []string{
		"resume watermark 1600",
		"partition 0",
		"previous watermark 2000",
	} {
		if !strings.Contains(inputErr.Reason, sub) {
			t.Errorf("reason = %q, want substring %q", inputErr.Reason, sub)
		}
	}
	if strings.Contains(inputErr.Reason, "effective watermark") {
		t.Errorf("the effective-watermark bound must not be the reported one: %q", inputErr.Reason)
	}

	// The failure must not be downgraded to a late-event notice.
	if stderr != "" {
		t.Errorf("a rejected resume record must produce no late-event notice, got %q", stderr)
	}
	// The fully emitted window stays; the still-open window is neither
	// emitted by the failed record nor closed by the watermark after it.
	want := `{"key":"sensor-a","start":0,"end":1000,"count":1,"sum":2}` + "\n"
	if stdout != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, want)
	}
}

// Resuming exactly at the partition's own previous watermark is legal even
// though it sits far above the effective watermark: it must not by itself
// close [600,1600) or re-emit [0,1000), because partition 1 still pins the
// effective watermark at 1000. Events after the resume are judged against the
// effective watermark, so the event at 1000 is counted normally rather than
// skipped as late against partition 0's own 2000. The merged window closes
// only once partition 1 advances to 1600 (count 3, sum 9, no partition
// field), and windows still open at end of input are not flushed.
func TestAggregateSlidingIdleResumeAtOwnOldWatermark(t *testing.T) {
	lines := append(slidingIdleResumeFixtureLines(),
		`{"type":"watermark","time":2000,"partition":0}`,                        // line 8: resume at the old watermark; effective stays 1000
		`{"type":"event","key":"sensor-a","time":1000,"value":4,"partition":0}`, // line 9: not late (1000 >= effective 1000)
		`{"type":"watermark","time":1600,"partition":1}`,                        // line 10: effective = 1600, closes [600,1600)
		`{"type":"event","key":"sensor-a","time":2000,"value":5,"partition":0}`, // line 11: opens [1200,2200), [1800,2800); stays open
	)
	stdout, stderr, err := runPartitionedSliding(t, strings.Join(lines, "\n"), 1000, 600, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stderr != "" {
		t.Fatalf("unexpected stderr: %q", stderr)
	}
	want := strings.Join([]string{
		`{"key":"sensor-a","start":0,"end":1000,"count":1,"sum":2}`,
		// Resume at 2000 emits nothing; the window closes only when
		// partition 1 catches up, merging both pre-idle events with the
		// post-resume one. The result carries no partition field.
		`{"key":"sensor-a","start":600,"end":1600,"count":3,"sum":9}`,
		``,
	}, "\n")
	if stdout != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, want)
	}
}
