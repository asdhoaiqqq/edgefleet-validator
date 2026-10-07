package main

import (
	"strings"
	"testing"
)

// Command-line regression coverage for every partition going idle and then one
// resuming in partitioned sliding-window aggregation (window 1000, slide 600,
// two partitions, one key "k"). The library tests prove the state rules
// directly; these drive the real command through a child process so flag
// parsing, stdout/stderr separation and the exit code all participate.
//
// The shared history (physical line numbers include the blank line 8):
//
//	line 1: p0 event k time=700  value=2 -> [0,1000), [600,1600)
//	line 2: p0 event k time=1300 value=4 -> [600,1600), [1200,2200)
//	line 3: p1 event k time=1000 value=3 -> [600,1600)
//	line 4: p1 event k time=1300 value=5 -> [600,1600), [1200,2200)
//	line 5: p0 watermark 1000            -> effective still unknown
//	line 6: p1 watermark 500             -> effective 500, closes nothing
//	line 7: p1 idle                      -> effective 1000, closes [0,1000)
//	line 8: blank
//	line 9: p0 idle                      -> all idle: retain 1000, not infinity
//
// All-idle is neither end of input nor an infinite watermark: the only output
// is the already closed [0,1000). The resume continuation then lets the
// resumed partition alone close the remaining overlapping windows with both
// partitions' pre-idle contributions merged into count and sum.
func cliAllIdleResumeHistory() []string {
	return []string{
		`{"type":"event","key":"k","time":700,"value":2,"partition":0}`,
		`{"type":"event","key":"k","time":1300,"value":4,"partition":0}`,
		`{"type":"event","key":"k","time":1000,"value":3,"partition":1}`,
		`{"type":"event","key":"k","time":1300,"value":5,"partition":1}`,
		`{"type":"watermark","time":1000,"partition":0}`,
		`{"type":"watermark","time":500,"partition":1}`,
		`{"type":"idle","partition":1}`,
		``,
		`{"type":"idle","partition":0}`,
	}
}

// TestAggregateCLIPartitionedSlidingAllIdleResume runs the complete sequence
// through the command line: after all-idle retains watermark 1000, partition 1
// resumes at that boundary while partition 0 stays idle, contributes a new
// legal event to the open overlap, and advances alone to close the windows.
// The rows are ordered by window end, carry no partition field, merge the two
// idle partitions' pre-idle contributions with the post-resume event, and the
// final open window is not flushed on end of input; stderr stays empty and the
// process exits 0.
func TestAggregateCLIPartitionedSlidingAllIdleResume(t *testing.T) {
	lines := append(cliAllIdleResumeHistory(),
		`{"type":"watermark","time":1000,"partition":1}`,                 // line 10: resume at retained boundary
		`{"type":"event","key":"k","time":1300,"value":6,"partition":1}`, // line 11: legal, joins open windows
		`{"type":"watermark","time":1600,"partition":1}`,                 // line 12: p1 alone closes [600,1600)
		`{"type":"watermark","time":2200,"partition":1}`,                 // line 13: closes [1200,2200)
	)
	code, stdout, stderr := runAggregateCLI(t, strings.Join(lines, "\n")+"\n",
		"--window-ms", "1000", "--slide-ms", "600", "--partitions", "2")
	if code != 0 {
		t.Fatalf("exit code = %d, want 0; stderr: %s", code, stderr)
	}
	if stderr != "" {
		t.Fatalf("the normal all-idle/resume path must print nothing on stderr, got %q", stderr)
	}
	assertWindowLines(t, stdout, []windowLine{
		{Key: "k", Start: 0, End: 1000, Count: 1, Sum: 2},
		// 2+4 from p0 and 3+5 from p1 before idling, plus p1's new 6.
		{Key: "k", Start: 600, End: 1600, Count: 5, Sum: 20},
		// 4 from p0 and 5 from p1 before idling, plus p1's new time=1300
		// event (6), which belongs to this overlapping window as well.
		{Key: "k", Start: 1200, End: 2200, Count: 3, Sum: 15},
	})
}

// TestAggregateCLIPartitionedSlidingAllIdleResumeBelowRetainedWatermark checks
// the command-line failure surface once every partition is idle: a resume
// watermark that is above the partition's own old watermark (500) but below
// the retained overall watermark (1000) exits non-zero with the existing input
// error on stderr -- physical line 10 (blank line 8 counts), partition 1 and
// the violated overall lower bound -- while stdout keeps only the window
// closed before all-idle. The record after the failed resume is never
// processed, so it cannot close [600,1600). A resume exactly at the retained
// watermark exits 0 and adds nothing by itself.
func TestAggregateCLIPartitionedSlidingAllIdleResumeBelowRetainedWatermark(t *testing.T) {
	t.Run("resume below retained overall watermark fails", func(t *testing.T) {
		lines := append(cliAllIdleResumeHistory(),
			`{"type":"watermark","time":700,"partition":1}`,  // line 10: >= own 500, < retained 1000
			`{"type":"watermark","time":1600,"partition":1}`, // line 11: dead; would close [600,1600)
		)
		code, stdout, stderr := runAggregateCLI(t, strings.Join(lines, "\n")+"\n",
			"--window-ms", "1000", "--slide-ms", "600", "--partitions", "2")
		if code != 1 {
			t.Fatalf("exit code = %d, want 1; stderr: %s", code, stderr)
		}
		for _, want := range []string{
			"line 10",
			"resume watermark 700",
			"partition 1",
			"current effective watermark 1000",
		} {
			if !strings.Contains(stderr, want) {
				t.Fatalf("stderr = %q, want it to mention %q", stderr, want)
			}
		}
		if strings.Contains(stderr, "previous watermark") {
			t.Fatalf("the own-old bound (500) is satisfied; stderr must name the retained overall bound: %q", stderr)
		}
		// Only the window closed while a watermark existed is retained; the
		// dead line 11 must not close [600,1600) after the fatal record.
		assertWindowLines(t, stdout, []windowLine{
			{Key: "k", Start: 0, End: 1000, Count: 1, Sum: 2},
		})
	})

	t.Run("resume exactly at retained watermark succeeds", func(t *testing.T) {
		lines := append(cliAllIdleResumeHistory(),
			`{"type":"watermark","time":1000,"partition":1}`, // line 10: boundary resume, emits nothing
		)
		code, stdout, stderr := runAggregateCLI(t, strings.Join(lines, "\n")+"\n",
			"--window-ms", "1000", "--slide-ms", "600", "--partitions", "2")
		if code != 0 {
			t.Fatalf("exit code = %d, want 0; stderr: %s", code, stderr)
		}
		if stderr != "" {
			t.Fatalf("the boundary resume must print nothing on stderr, got %q", stderr)
		}
		assertWindowLines(t, stdout, []windowLine{
			{Key: "k", Start: 0, End: 1000, Count: 1, Sum: 2},
		})
	})
}
