package main

import (
	"bytes"
	"strings"
	"testing"
)

// Command-line coverage for aggregate --max-open-windows: the flag is an
// optional positive signed 64-bit integer capping the number of open
// (key, window) aggregation slots. Invalid values fail before any input is
// read with exit code 2 and empty standard output; an event that would
// exceed the limit fails its physical input line with exit code 1 while
// earlier output is retained. The library tests in the edgefleet package
// pin the slot-counting rules themselves.

// TestAggregateCLIMaxOpenWindowsInvalidValues rejects an explicitly given
// zero, negative, non-integer or out-of-range --max-open-windows before any
// input is read: standard output stays empty, standard error explains the
// parameter problem and the exit code is 2.
func TestAggregateCLIMaxOpenWindowsInvalidValues(t *testing.T) {
	cases := []struct {
		name  string
		value string
	}{
		{"zero", "0"},
		{"negative", "-3"},
		{"minimum int64", "-9223372036854775808"},
		{"non-integer", "abc"},
		{"above max int64", "9223372036854775808"},
		{"below min int64", "-9223372036854775809"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			input := `{"type":"event","key":"k","time":100,"value":1}` + "\n" +
				`{"type":"watermark","time":1000}` + "\n"
			cmd := aggregateCommand(t, input, "--window-ms", "1000", "--max-open-windows", tc.value)
			var stdout, stderr bytes.Buffer
			cmd.Stdout = &stdout
			cmd.Stderr = &stderr
			err := cmd.Run()
			if err == nil {
				t.Fatalf("aggregate succeeded with --max-open-windows %s, want exit code 2", tc.value)
			}
			if code := cmd.ProcessState.ExitCode(); code != 2 {
				t.Fatalf("exit code = %d, want 2", code)
			}
			if stdout.Len() != 0 {
				t.Fatalf("stdout = %q, want empty: no input may be processed after a parameter error", stdout.String())
			}
			if !strings.Contains(stderr.String(), "max-open-windows") {
				t.Fatalf("stderr = %q, want it to name the --max-open-windows parameter", stderr.String())
			}
		})
	}
}

// TestAggregateCLIMaxOpenWindowsOmittedKeepsUnlimited proves the flag is
// optional: without it, distinct keys accumulate without any limit, exactly
// as before the flag existed.
func TestAggregateCLIMaxOpenWindowsOmittedKeepsUnlimited(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"a","time":100,"value":1}`,
		`{"type":"event","key":"b","time":200,"value":2}`,
		`{"type":"event","key":"c","time":300,"value":3}`,
		`{"type":"watermark","time":1000}`,
	}, "\n") + "\n"
	cmd := aggregateCommand(t, input, "--window-ms", "1000")
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("aggregate failed: %v\nstderr: %s", err, stderr.String())
	}
	if code := cmd.ProcessState.ExitCode(); code != 0 {
		t.Fatalf("exit code = %d, want 0", code)
	}
	assertWindowLines(t, stdout.String(), []windowLine{
		{Key: "a", Start: 0, End: 1000, Count: 1, Sum: 1},
		{Key: "b", Start: 0, End: 1000, Count: 1, Sum: 2},
		{Key: "c", Start: 0, End: 1000, Count: 1, Sum: 3},
	})
}

// TestAggregateCLIMaxOpenWindowsExactLimitAccepted drives a run that fills
// every slot exactly: two distinct keys under a limit of two succeed, the
// window closes normally and the exit code is 0.
func TestAggregateCLIMaxOpenWindowsExactLimitAccepted(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"a","time":100,"value":1}`,
		`{"type":"event","key":"b","time":200,"value":2}`,
		`{"type":"watermark","time":1000}`,
	}, "\n") + "\n"
	cmd := aggregateCommand(t, input, "--window-ms", "1000", "--max-open-windows", "2")
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
		{Key: "a", Start: 0, End: 1000, Count: 1, Sum: 1},
		{Key: "b", Start: 0, End: 1000, Count: 1, Sum: 2},
	})
}

// TestAggregateCLIMaxOpenWindowsExceededFailsLine drives a run whose fifth
// physical line (a blank line 3 still counts) would open a second slot under
// a limit of one. The command exits 1, standard error names the line, the
// key, the slots in use, the slots needed and the configured limit; the
// window closed before the fatal line stays on standard output and the
// watermark after the fatal line is never processed.
func TestAggregateCLIMaxOpenWindowsExceededFailsLine(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"a","time":100,"value":1}`, // line 1: fills the only slot
		`{"type":"watermark","time":1000}`,                // line 2: closes [0,1000), frees the slot
		``,                                                // line 3: blank, still counted
		`{"type":"event","key":"b","time":1100,"value":2}`, // line 4: takes the slot again
		`{"type":"event","key":"c","time":1200,"value":3}`, // line 5: needs a 2nd slot -> fatal
		`{"type":"watermark","time":2000}`,                 // line 6: never read
	}, "\n") + "\n"
	cmd := aggregateCommand(t, input, "--window-ms", "1000", "--max-open-windows", "1")
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err == nil {
		t.Fatal("aggregate succeeded, want exit code 1 for an event beyond --max-open-windows")
	}
	if code := cmd.ProcessState.ExitCode(); code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}

	errText := stderr.String()
	for _, want := range []string{"line 5", `"c"`, "1 window slot(s) currently open", "needs 1 new slot(s)", "limit is 1"} {
		if !strings.Contains(errText, want) {
			t.Fatalf("stderr = %q, want it to mention %q", errText, want)
		}
	}

	// Exactly the one result completed before the error; line 6's watermark
	// would have closed b's window, so any second line would prove
	// processing continued after the fatal record.
	assertWindowLines(t, stdout.String(), []windowLine{
		{Key: "a", Start: 0, End: 1000, Count: 1, Sum: 1},
	})
}
