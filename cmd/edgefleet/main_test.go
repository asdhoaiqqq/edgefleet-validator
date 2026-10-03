package main

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"strings"
	"testing"
)

// These tests are command-line regression coverage for "aggregate" with
// --window-ms, --slide-ms and --partitions given at the same time. They drive
// the real main() entry point in a child process (the recompiled test binary)
// and feed line-delimited JSON on standard input, so flag parsing, standard
// output/error separation and the process exit code are all exercised
// end-to-end; the existing package tests cover the library behavior directly.

// aggregateCommand builds a child process that enters main() with
// ["aggregate", args...] and feeds input to its standard input. The child is
// the running test binary relaunched with TestHelperProcess, so no separate
// build step is needed.
func aggregateCommand(t *testing.T, input string, args ...string) *exec.Cmd {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("locate test binary: %v", err)
	}
	cmdArgs := append([]string{"-test.run=^TestHelperProcess$", "--", "aggregate"}, args...)
	cmd := exec.Command(exe, cmdArgs...)
	cmd.Stdin = strings.NewReader(input)
	cmd.Env = append(os.Environ(), "GO_WANT_HELPER_PROCESS=1")
	return cmd
}

// TestHelperProcess is the child-process entry point: it rewrites os.Args to
// the form main() expects ([binary, aggregate, flags...]) and hands control to
// main(), whose os.Exit calls become the child's real exit code.
func TestHelperProcess(t *testing.T) {
	if os.Getenv("GO_WANT_HELPER_PROCESS") != "1" {
		return
	}
	for i, a := range os.Args {
		if a == "--" {
			os.Args = append([]string{os.Args[0]}, os.Args[i+1:]...)
			break
		}
	}
	main()
}

// windowLine is the exact on-the-wire aggregate result record: the existing
// key, start, end, count and sum fields and nothing else.
type windowLine struct {
	Key   string `json:"key"`
	Start int64  `json:"start"`
	End   int64  `json:"end"`
	Count int64  `json:"count"`
	Sum   int64  `json:"sum"`
}

// assertWindowLines verifies that stdout is one complete JSON object per
// physical line, in order, with exactly the existing fields (no partition
// field), and nothing trailing after the last newline.
func assertWindowLines(t *testing.T, stdout string, want []windowLine) {
	t.Helper()
	physicalLines := strings.Split(strings.TrimSuffix(stdout, "\n"), "\n")
	if len(physicalLines) != len(want) {
		t.Fatalf("stdout has %d result line(s), want %d:\n%s", len(physicalLines), len(want), stdout)
	}
	for i, text := range physicalLines {
		var got windowLine
		if err := json.Unmarshal([]byte(text), &got); err != nil {
			t.Fatalf("stdout line %d is not a complete JSON object: %v (%q)", i+1, err, text)
		}
		if got != want[i] {
			t.Fatalf("stdout line %d = %+v, want %+v", i+1, got, want[i])
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal([]byte(text), &fields); err != nil {
			t.Fatalf("stdout line %d: %v", i+1, err)
		}
		if len(fields) != 5 {
			t.Fatalf("stdout line %d carries %d fields, want exactly key,start,end,count,sum: %q", i+1, len(fields), text)
		}
		if _, ok := fields["partition"]; ok {
			t.Fatalf("stdout line %d must not carry a partition field: %q", i+1, text)
		}
	}
}

// TestAggregateCLIPartitionedSlidingWindow drives the documented usage with
// all three flags at once: window length 1000ms, slide interval 600ms and two
// partitions. The observed output proves every flag is actually used:
//   - --window-ms 1000 and --slide-ms 600 produce the overlapping windows
//     [0,1000) and [600,1600), and the time-1000 event stays out of [0,1000)
//     (that window reports count 1, sum 2, not count 2, sum 5);
//   - --partitions 2 accepts partition-bearing records and the idle record
//     (an idle record is fatal in single-watermark mode) and merges both
//     partitions into one count/sum per window ([600,1600) reports 2 and 5).
//
// Partition 0's watermark of 1600 alone must not close anything; once
// partition 1 reports 600 the effective watermark is 600 and still nothing
// closes; only partition 1's idle declaration raises the effective
// watermark to 1600 and emits both windows. A normal input ending right after
// the idle declaration exits 0 with empty standard error.
func TestAggregateCLIPartitionedSlidingWindow(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":700,"value":2,"partition":0}`,  // line 1
		`{"type":"event","key":"k","time":1000,"value":3,"partition":1}`, // line 2
		`{"type":"watermark","time":1600,"partition":0}`,                 // line 3: effective still unknown
		`{"type":"watermark","time":600,"partition":1}`,                  // line 4: effective 600, nothing closes
		`{"type":"idle","partition":1}`,                                  // line 5: effective 1600, both windows close
	}, "\n") + "\n"

	cmd := aggregateCommand(t, input, "--window-ms", "1000", "--slide-ms", "600", "--partitions", "2")
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("aggregate failed: %v\nstderr: %s", err, stderr.String())
	}
	if code := cmd.ProcessState.ExitCode(); code != 0 {
		t.Fatalf("exit code = %d, want 0", code)
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}

	assertWindowLines(t, stdout.String(), []windowLine{
		{Key: "k", Start: 0, End: 1000, Count: 1, Sum: 2},
		{Key: "k", Start: 600, End: 1600, Count: 2, Sum: 5},
	})
}

// TestAggregateCLIEventForIdlePartitionIsFatal guards the failure boundary:
// an event arriving while its partition is idle is fatal with exit code 1,
// naming the idle partition and the event's physical input line (blank lines
// still count), even when the event time is below the current effective
// watermark -- it must not be downgraded to an ordinary late-event notice and
// a successful exit. The two fully written results stay on standard output,
// no later watermark produces anything, and the explanation stays on
// standard error only.
func TestAggregateCLIEventForIdlePartitionIsFatal(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":700,"value":2,"partition":0}`,  // line 1
		`{"type":"event","key":"k","time":1000,"value":3,"partition":1}`, // line 2
		`{"type":"event","key":"k","time":5000,"value":7,"partition":0}`, // line 3: later windows left open
		`{"type":"watermark","time":1600,"partition":0}`,                 // line 4: effective still unknown
		`{"type":"watermark","time":600,"partition":1}`,                  // line 5: effective 600, nothing closes
		`{"type":"idle","partition":1}`,                                  // line 6: effective 1600, two results
		``,                                                               // line 7: blank, still counted
		`{"type":"event","key":"k","time":500,"value":1,"partition":1}`,  // line 8: idle p1 AND below wm 1600
		`{"type":"watermark","time":5800,"partition":0}`,                 // line 9: must never be read
	}, "\n") + "\n"

	cmd := aggregateCommand(t, input, "--window-ms", "1000", "--slide-ms", "600", "--partitions", "2")
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err == nil {
		t.Fatal("aggregate succeeded, want exit code 1 for an event on an idle partition")
	}
	if code := cmd.ProcessState.ExitCode(); code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}

	errText := stderr.String()
	for _, want := range []string{"line 8", "idle partition 1"} {
		if !strings.Contains(errText, want) {
			t.Fatalf("stderr = %q, want it to mention %q", errText, want)
		}
	}
	if strings.Contains(errText, "late event") {
		t.Fatalf("idle-partition event must not be reported as an ordinary late event: stderr = %q", errText)
	}
	if strings.Contains(stdout.String(), "idle") || strings.Contains(stdout.String(), "aggregate:") {
		t.Fatalf("error explanation must not appear on stdout: %q", stdout.String())
	}

	// Exactly the two results completed before the error; the time-5000 event
	// opened later windows that line 9's watermark would close, so any extra
	// line would prove processing continued after the fatal record.
	assertWindowLines(t, stdout.String(), []windowLine{
		{Key: "k", Start: 0, End: 1000, Count: 1, Sum: 2},
		{Key: "k", Start: 600, End: 1600, Count: 2, Sum: 5},
	})
}
