package main

import (
	"strings"
	"testing"
)

// Command-line regression coverage for every partition being idle at once and
// the stream then resuming, with --window-ms, --slide-ms and --partitions all
// in play. All-idle is neither end of input nor an infinite watermark: the
// retained effective watermark and the still-open windows' contents survive,
// nothing is emitted while no active partition exists, and after one
// partition resumes, closure is driven by its watermarks alone. These tests
// drive the real main() in a child process (see main_test.go for the
// harness); the edgefleet package's all-idle tests cover the library
// behavior directly.

// All partitions idle after an effective watermark existed, then one resumes:
// the pre-idle events both partitions fed into the shared overlapping windows
// still determine the results, the resumed partition's new event joins the
// still-open windows, and the run exits 0 with empty standard error. Window
// length 1000, slide 600, two partitions, one key.
func TestAggregateCLISlidingAllIdleThenResume(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":700,"value":2,"partition":0}`,  // [0,1000) and [600,1600)
		`{"type":"event","key":"k","time":1000,"value":3,"partition":1}`, // [600,1600) only
		`{"type":"event","key":"k","time":1300,"value":5,"partition":1}`, // [600,1600) and [1200,2200)
		`{"type":"watermark","time":1600,"partition":0}`,                 // effective still unknown
		`{"type":"watermark","time":1000,"partition":1}`,                 // effective 1000, closes [0,1000)
		`{"type":"idle","partition":0}`,                                  // effective stays 1000
		`{"type":"idle","partition":1}`,                                  // all idle: retained 1000, no output
		`{"type":"watermark","time":1600,"partition":0}`,                 // resume; effective 1600, closes [600,1600)
		`{"type":"event","key":"k","time":1900,"value":7,"partition":0}`, // joins open [1200,2200) and [1800,2800)
		`{"type":"watermark","time":2200,"partition":0}`,                 // closes [1200,2200); p1 still idle
	}, "\n") + "\n"

	code, stdout, stderr := runAggregateCLI(t, input, "--window-ms", "1000", "--slide-ms", "600", "--partitions", "2")
	if code != 0 {
		t.Fatalf("exit code = %d, want 0; stderr: %s", code, stderr)
	}
	if stderr != "" {
		t.Fatalf("stderr = %q, want empty (no late events on the normal path)", stderr)
	}
	assertWindowLines(t, stdout, []windowLine{
		{Key: "k", Start: 0, End: 1000, Count: 1, Sum: 2},
		// Both partitions' pre-idle contributions survive the all-idle period.
		{Key: "k", Start: 600, End: 1600, Count: 3, Sum: 10},
		// Partition 1's pre-idle event at 1300 merges with the resumed
		// partition's post-resume event at 1900.
		{Key: "k", Start: 1200, End: 2200, Count: 2, Sum: 12},
	})
}

// A resume record that clears the partition's own pre-idle watermark but sits
// below the retained effective watermark is a record error, not an argument
// error: exit code 1, the physical input line (blank lines count), the
// partition and the violated effective-watermark bound on standard error, the
// windows closed before the failure retained on standard output, and records
// after the failing one never close the still-open window.
func TestAggregateCLISlidingAllIdleResumeBelowRetainedEffective(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":700,"value":2,"partition":0}`,  // line 1
		`{"type":"event","key":"k","time":1000,"value":3,"partition":1}`, // line 2
		`{"type":"event","key":"k","time":1300,"value":5,"partition":0}`, // line 3: opens [1200,2200)
		`{"type":"watermark","time":1000,"partition":0}`,                 // line 4
		`{"type":"watermark","time":1000,"partition":1}`,                 // line 5: effective 1000, closes [0,1000)
		``,                              // line 6: blank, still counted
		`{"type":"idle","partition":1}`, // line 7: p0 keeps effective at 1000
		`{"type":"watermark","time":2000,"partition":0}`, // line 8: effective 2000, closes [600,1600)
		`{"type":"idle","partition":0}`,                  // line 9: all idle, retained 2000
		`{"type":"watermark","time":1500,"partition":1}`, // line 10: >= own 1000 but < retained 2000, fatal
		`{"type":"watermark","time":2200,"partition":0}`, // line 11: must never be read
	}, "\n") + "\n"

	code, stdout, stderr := runAggregateCLI(t, input, "--window-ms", "1000", "--slide-ms", "600", "--partitions", "2")
	if code != 1 {
		t.Fatalf("exit code = %d, want 1; stderr: %s", code, stderr)
	}
	for _, want := range []string{"line 10", "partition 1", "effective watermark 2000"} {
		if !strings.Contains(stderr, want) {
			t.Fatalf("stderr = %q, want it to mention %q", stderr, want)
		}
	}
	if strings.Contains(stderr, "late event") {
		t.Fatalf("a rejected resume record must not be reported as an ordinary late event: stderr = %q", stderr)
	}
	// Exactly the two windows closed before the failure; line 11 would close
	// [1200,2200), so a third result would prove processing continued.
	assertWindowLines(t, stdout, []windowLine{
		{Key: "k", Start: 0, End: 1000, Count: 1, Sum: 2},
		{Key: "k", Start: 600, End: 1600, Count: 3, Sum: 10},
	})
}
