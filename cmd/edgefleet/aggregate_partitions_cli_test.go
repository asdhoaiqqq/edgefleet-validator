package main

import (
	"bytes"
	"os/exec"
	"strings"
	"testing"
)

// Command-line regression coverage for how "aggregate" selects its input mode
// from --partitions. Omitting the flag and passing an explicit count are
// different modes and must not be conflated by the parsed value alone: the
// default 0 means legacy single-watermark mode, while any explicitly given
// positive count (including 1) means partitioned mode with per-record
// partition validation. These tests drive the real main() in a child process
// (see main_test.go for the harness) and assert on the process exit code,
// standard output and standard error separately, so an argument error (exit
// 2, before any input is read) stays distinguishable from a record error
// (exit 1, after processing has started).

// runAggregateCLI runs the aggregate command in a child process with the
// given flags and standard input, returning the exit code and what the
// process wrote to standard output and standard error.
func runAggregateCLI(t *testing.T, input string, args ...string) (int, string, string) {
	t.Helper()
	cmd := aggregateCommand(t, input, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	runErr := cmd.Run()
	code := 0
	if runErr != nil {
		exitErr, ok := runErr.(*exec.ExitError)
		if !ok {
			t.Fatalf("aggregate did not run: %v", runErr)
		}
		code = exitErr.ExitCode()
	}
	return code, stdout.String(), stderr.String()
}

// Without --partitions the command keeps the single-watermark behavior:
// plain event and watermark records carry no partition field, and a record
// that still brings one has it ignored as an extra field.
func TestAggregateCLIPartitionsOmittedKeepsSingleWatermarkMode(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":100,"value":2}`,
		`{"type":"event","key":"k","time":200,"value":3,"partition":7}`, // extra field ignored
		`{"type":"watermark","time":1000}`,
		`{"type":"watermark","time":1500,"partition":9}`, // extra field ignored
	}, "\n") + "\n"

	code, stdout, stderr := runAggregateCLI(t, input, "--window-ms", "1000")
	if code != 0 {
		t.Fatalf("exit code = %d, want 0; stderr: %s", code, stderr)
	}
	if stderr != "" {
		t.Fatalf("stderr = %q, want empty", stderr)
	}
	assertWindowLines(t, stdout, []windowLine{
		{Key: "k", Start: 0, End: 1000, Count: 2, Sum: 5},
	})
}

// An explicit --partitions 1 is partition mode, not "one input, skip the
// checks": every event and watermark must carry partition 0, the merged
// output keeps the existing key,start,end,count,sum shape with no partition
// field, and a normal run exits 0 with empty standard error.
func TestAggregateCLIPartitionsOneIsPartitionMode(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":100,"value":2,"partition":0}`,
		`{"type":"event","key":"k","time":200,"value":3,"partition":0}`,
		`{"type":"watermark","time":1000,"partition":0}`,
	}, "\n") + "\n"

	code, stdout, stderr := runAggregateCLI(t, input, "--window-ms", "1000", "--partitions", "1")
	if code != 0 {
		t.Fatalf("exit code = %d, want 0; stderr: %s", code, stderr)
	}
	if stderr != "" {
		t.Fatalf("stderr = %q, want empty", stderr)
	}
	assertWindowLines(t, stdout, []windowLine{
		{Key: "k", Start: 0, End: 1000, Count: 2, Sum: 5},
	})
}

// The same records that are valid without --partitions become input errors
// under --partitions 1: a missing partition field or a value equal to the
// count ends the run with exit code 1, the physical line number and the
// partition problem on standard error. This is what separates "flag omitted"
// from "one partition".
func TestAggregateCLIPartitionsOneValidatesEveryRecord(t *testing.T) {
	cases := []struct {
		name      string
		input     string
		wantLine  string
		wantInMsg []string
	}{
		{
			"event missing partition",
			`{"type":"event","key":"k","time":100,"value":2}` + "\n",
			"line 1", []string{`"partition"`},
		},
		{
			"watermark missing partition",
			`{"type":"watermark","time":1000}` + "\n",
			"line 1", []string{`"partition"`},
		},
		{
			"partition equals count",
			`{"type":"event","key":"k","time":100,"value":2,"partition":1}` + "\n",
			"line 1", []string{"partition", "[0,1)"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, stdout, stderr := runAggregateCLI(t, tc.input, "--window-ms", "1000", "--partitions", "1")
			if code != 1 {
				t.Fatalf("exit code = %d, want 1; stderr: %s", code, stderr)
			}
			if stdout != "" {
				t.Fatalf("stdout = %q, want empty: no window closed before the bad record", stdout)
			}
			for _, want := range append([]string{tc.wantLine}, tc.wantInMsg...) {
				if !strings.Contains(stderr, want) {
					t.Fatalf("stderr = %q, want it to mention %q", stderr, want)
				}
			}
		})
	}
}

// A --partitions count that is zero, negative, not an integer or outside the
// signed 64-bit range is an argument error: the command exits 2 with the
// reason on standard error only, before any business record is read. Input
// that would close a window must not produce results first, and empty input
// must not mask the bad argument either.
func TestAggregateCLIInvalidPartitionCountExits2BeforeInput(t *testing.T) {
	badCounts := []struct {
		name  string
		value string
	}{
		{"zero", "0"},
		{"negative", "-3"},
		{"min int64", "-9223372036854775808"},
		{"not an integer", "two"},
		{"not a whole number", "1.5"},
		{"empty", ""},
		{"above max int64", "9223372036854775808"},
		{"far above max int64", "99999999999999999999999999"},
	}
	// Records that would close [0,1000) and print a result in any valid mode.
	closingInput := strings.Join([]string{
		`{"type":"event","key":"k","time":100,"value":2,"partition":0}`,
		`{"type":"watermark","time":1000,"partition":0}`,
	}, "\n") + "\n"

	for _, bc := range badCounts {
		for _, input := range []struct {
			name  string
			value string
		}{
			{"with window-closing input", closingInput},
			{"with empty input", ""},
		} {
			t.Run(bc.name+"/"+input.name, func(t *testing.T) {
				code, stdout, stderr := runAggregateCLI(t, input.value, "--window-ms", "1000", "--partitions", bc.value)
				if code != 2 {
					t.Fatalf("exit code = %d, want 2; stderr: %s", code, stderr)
				}
				if stdout != "" {
					t.Fatalf("stdout = %q, want empty: invalid --partitions must be rejected before any record is read", stdout)
				}
				if !strings.Contains(stderr, "partitions") {
					t.Fatalf("stderr = %q, want it to name the --partitions problem", stderr)
				}
			})
		}
	}
}

// Once partition mode is successfully enabled, a record that violates the
// partition requirement is a record error, not an argument error: exit code
// 1, the physical input line (blank lines count) and the partition problem on
// standard error, window results already completed stay on standard output,
// and records after the bad one never change the output.
func TestAggregateCLIPartitionRecordErrorExit1RetainsOutput(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":100,"value":2,"partition":0}`, // line 1
		`{"type":"watermark","time":1000,"partition":0}`,                // line 2
		`{"type":"watermark","time":1000,"partition":1}`,                // line 3: closes [0,1000)
		``,                                                              // line 4: blank, still counted
		`{"type":"event","key":"k","time":1200,"value":5,"partition":2}`, // line 5: partition == count, fatal
		`{"type":"watermark","time":5000,"partition":0}`,                // line 6: must never be read
		`{"type":"watermark","time":5000,"partition":1}`,                // line 7: would close [1000,2000)
	}, "\n") + "\n"

	code, stdout, stderr := runAggregateCLI(t, input, "--window-ms", "1000", "--partitions", "2")
	if code != 1 {
		t.Fatalf("exit code = %d, want 1; stderr: %s", code, stderr)
	}
	for _, want := range []string{"line 5", "partition", "[0,2)"} {
		if !strings.Contains(stderr, want) {
			t.Fatalf("stderr = %q, want it to mention %q", stderr, want)
		}
	}
	// Only the window closed before the bad record may appear; the time-1200
	// event opened [1000,2000) and lines 6-7 would close it, so a second
	// result would prove processing continued after the fatal record.
	assertWindowLines(t, stdout, []windowLine{
		{Key: "k", Start: 0, End: 1000, Count: 1, Sum: 2},
	})
}
