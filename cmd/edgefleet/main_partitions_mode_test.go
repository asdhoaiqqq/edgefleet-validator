package main

import (
	"bytes"
	"strings"
	"testing"
)

// These tests pin down how the aggregate command chooses its input mode from
// the --partitions flag itself, not from the flag's numeric value:
//
//   - omitting --partitions is the legacy single-watermark mode, where plain
//     records carry no partition field and extra fields are ignored;
//   - passing --partitions with any positive integer (including 1) is
//     partitioned mode, where every event and watermark is checked;
//   - an unparsable or out-of-range count, or zero and negatives, is a
//     command-parameter error (exit 2, stderr only, before stdin is read),
//     which must stay distinct from a record error discovered while
//     processing input (exit 1, line number on stderr, output retained).
//
// The tests drive the real main() entry point via aggregateCommand, so the
// process exit code and the stdout/stderr split are exercised end-to-end.

// runAggregateCommand starts the aggregate child with input on stdin, waits
// for it and returns its exit code and captured stdout/stderr text.
func runAggregateCommand(t *testing.T, input string, args ...string) (code int, stdout, stderr string) {
	t.Helper()
	cmd := aggregateCommand(t, input, args...)
	var outBuf, errBuf bytes.Buffer
	cmd.Stdout = &outBuf
	cmd.Stderr = &errBuf
	err := cmd.Run()
	if err != nil {
		if code = cmd.ProcessState.ExitCode(); code < 0 {
			t.Fatalf("aggregate child terminated abnormally: %v", err)
		}
	}
	return code, outBuf.String(), errBuf.String()
}

// TestAggregateCLIPartitionsOmittedKeepsLegacyMode proves that without the
// flag the command stays in single-watermark mode even though the internal
// default value is also 0: an ordinary event and watermark need no partition
// field, a stray partition field is ignored under the existing extra-fields
// rule, and the window closes with the existing key,start,end,count,sum
// record and no partition field at exit 0 with empty stderr.
func TestAggregateCLIPartitionsOmittedKeepsLegacyMode(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":100,"value":2,"partition":7}`, // stray field, ignored
		`{"type":"watermark","time":1000,"partition":9}`,                // closes [0,1000)
	}, "\n") + "\n"

	code, stdout, stderr := runAggregateCommand(t, input, "--window-ms", "1000")
	if code != 0 {
		t.Fatalf("exit code = %d, want 0; stderr: %s", code, stderr)
	}
	if stderr != "" {
		t.Fatalf("stderr = %q, want empty", stderr)
	}
	assertWindowLines(t, stdout, []windowLine{
		{Key: "k", Start: 0, End: 1000, Count: 1, Sum: 2},
	})
}

// TestAggregateCLIPartitionsOmittedIdleIsUnknown guards the mode boundary
// from the other direction: an idle record only exists in partitioned mode,
// so with --partitions omitted it must be rejected as an unknown record
// type. Accepting it would mean the omitted flag had silently enabled
// partition handling.
func TestAggregateCLIPartitionsOmittedIdleIsUnknown(t *testing.T) {
	input := `{"type":"idle","partition":0}` + "\n"

	code, stdout, stderr := runAggregateCommand(t, input, "--window-ms", "1000")
	if code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	if stdout != "" {
		t.Fatalf("stdout = %q, want empty", stdout)
	}
	for _, want := range []string{"line 1", "unknown record type", "idle"} {
		if !strings.Contains(stderr, want) {
			t.Fatalf("stderr = %q, want substring %q", stderr, want)
		}
	}
}

// TestAggregateCLIPartitionsOnePartition is the explicit counterpart:
// --partitions 1 is full partitioned mode even though only one input source
// exists. Partition 0 records aggregate normally and close a window with the
// ordinary five-field output, and an empty input with the flag present is a
// successful run, not a parameter complaint.
func TestAggregateCLIPartitionsOnePartition(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":100,"value":2,"partition":0}`,
		`{"type":"watermark","time":1000,"partition":0}`,
	}, "\n") + "\n"

	code, stdout, stderr := runAggregateCommand(t, input, "--window-ms", "1000", "--partitions", "1")
	if code != 0 {
		t.Fatalf("exit code = %d, want 0; stderr: %s", code, stderr)
	}
	if stderr != "" {
		t.Fatalf("stderr = %q, want empty", stderr)
	}
	assertWindowLines(t, stdout, []windowLine{
		{Key: "k", Start: 0, End: 1000, Count: 1, Sum: 2},
	})

	// Empty input never masks or fabricates a parameter problem: a valid
	// explicit count on empty input still succeeds quietly.
	code, stdout, stderr = runAggregateCommand(t, "", "--window-ms", "1000", "--partitions", "1")
	if code != 0 {
		t.Fatalf("empty input: exit code = %d, want 0; stderr: %s", code, stderr)
	}
	if stdout != "" || stderr != "" {
		t.Fatalf("empty input: stdout = %q stderr = %q, want both empty", stdout, stderr)
	}
}

// TestAggregateCLIPartitionsOnePartitionStillValidatesRecords is the core
// distinction: a single partition is not a shortcut that skips per-record
// checks. Missing partition fields on either record type and partition
// values outside [0,1) (equalling the count, or negative) are input errors
// on the actual physical line (blank lines count), exit 1, with nothing on
// stdout. These runs never get confused with parameter errors.
func TestAggregateCLIPartitionsOnePartitionStillValidatesRecords(t *testing.T) {
	cases := []struct {
		name      string
		input     string
		wantLine  string
		wantInMsg string
	}{
		{
			"event missing partition",
			`{"type":"event","key":"k","time":100,"value":2}` + "\n",
			"line 1", `"partition"`,
		},
		{
			"watermark missing partition after blank line",
			"\n" + `{"type":"watermark","time":1000}` + "\n",
			"line 2", `"partition"`,
		},
		{
			"partition equals count",
			`{"type":"watermark","time":1000,"partition":1}` + "\n",
			"line 1", "[0,1)",
		},
		{
			"negative partition",
			`{"type":"event","key":"k","time":100,"value":2,"partition":-1}` + "\n",
			"line 1", "[0,1)",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, stdout, stderr := runAggregateCommand(t, tc.input, "--window-ms", "1000", "--partitions", "1")
			if code != 1 {
				t.Fatalf("exit code = %d, want 1; stderr: %s", code, stderr)
			}
			if stdout != "" {
				t.Fatalf("stdout = %q, want empty", stdout)
			}
			for _, want := range []string{tc.wantLine, tc.wantInMsg} {
				if !strings.Contains(stderr, want) {
					t.Fatalf("stderr = %q, want substring %q", stderr, want)
				}
			}
		})
	}
}

// TestAggregateCLIPartitionsRecordErrorRetainsOutput checks the processing
// phase boundary end to end: a window fully emitted before the bad record
// stays on stdout, the fatal record reports its physical line (blank lines
// still count) on stderr with exit 1, and a later watermark that would close
// another window is never read, so it cannot change the output.
func TestAggregateCLIPartitionsRecordErrorRetainsOutput(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":100,"value":2,"partition":0}`, // line 1
		``, // line 2: blank, still counted
		`{"type":"watermark","time":1000,"partition":0}`, // line 3: closes [0,1000)
		``, // line 4: blank, still counted
		`{"type":"event","key":"k","time":200,"value":3}`, // line 5: missing partition
		`{"type":"watermark","time":5000,"partition":0}`,  // line 6: must never be read
	}, "\n") + "\n"

	code, stdout, stderr := runAggregateCommand(t, input, "--window-ms", "1000", "--partitions", "1")
	if code != 1 {
		t.Fatalf("exit code = %d, want 1; stderr: %s", code, stderr)
	}
	if !strings.Contains(stderr, "line 5") || !strings.Contains(stderr, `"partition"`) {
		t.Fatalf("stderr = %q, want line 5 and the partition problem", stderr)
	}
	assertWindowLines(t, stdout, []windowLine{
		{Key: "k", Start: 0, End: 1000, Count: 1, Sum: 2},
	})
}

// TestAggregateCLIInvalidPartitionsRejectedBeforeInput covers the parameter
// boundary: every non-positive, non-integer or out-of-int64 count must be
// refused with exit 2 and the reason on stderr before any business record is
// read. The stdin carries records that would validly close a window, so any
// stdout would prove the command started processing before validating its
// parameters; a separate empty-input run shows empty input cannot hide an
// illegal parameter either.
func TestAggregateCLIInvalidPartitionsRejectedBeforeInput(t *testing.T) {
	closingInput := strings.Join([]string{
		`{"type":"event","key":"k","time":100,"value":2,"partition":0}`,
		`{"type":"watermark","time":1000,"partition":0}`,
	}, "\n") + "\n"

	cases := []struct {
		name    string
		value   string
		wantMsg string // substring the reason must contain beyond the flag name
	}{
		{"zero", "0", "positive signed 64-bit integer"},
		{"negative", "-1", "positive signed 64-bit integer"},
		{"large negative", "-3", "positive signed 64-bit integer"},
		{"non-numeric text", "abc", "invalid value"},
		{"decimal text", "1.5", "invalid value"},
		{"above signed 64-bit max", "9223372036854775808", "out of range"},
		{"below signed 64-bit min", "-9223372036854775809", "out of range"},
	}
	for _, tc := range cases {
		// The same bad parameter must be refused both when stdin could
		// produce results and when stdin is empty.
		for _, in := range []struct {
			label string
			input string
		}{{"closing input", closingInput}, {"empty input", ""}} {
			name := tc.name + "/" + in.label
			t.Run(name, func(t *testing.T) {
				code, stdout, stderr := runAggregateCommand(t, in.input, "--window-ms", "1000", "--partitions", tc.value)
				if code != 2 {
					t.Fatalf("exit code = %d, want 2; stderr: %s", code, stderr)
				}
				if stdout != "" {
					t.Fatalf("stdout = %q, want empty: parameters must be rejected before any output", stdout)
				}
				if stderr == "" {
					t.Fatal("stderr must explain the rejected parameter")
				}
				if !strings.Contains(stderr, "partitions") {
					t.Fatalf("stderr = %q, must name the --partitions parameter", stderr)
				}
				if !strings.Contains(stderr, tc.wantMsg) {
					t.Fatalf("stderr = %q, want substring %q", stderr, tc.wantMsg)
				}
			})
		}
	}
}
