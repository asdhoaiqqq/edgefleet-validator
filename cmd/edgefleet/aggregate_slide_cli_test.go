package main

import (
	"strings"
	"testing"
)

// Command-line regression coverage for how "aggregate" applies --slide-ms.
// The window math itself is covered by the package tests; these tests drive
// the real main() in a child process (see main_test.go for the harness and
// runAggregateCLI in aggregate_partitions_cli_test.go) so the flag's omitted
// value, explicit values and invalid values are all observed through the
// command entry point: exit code, standard output and standard error are
// asserted separately, keeping an argument error (exit 2, before any input
// is read) distinguishable from a record error (exit 1, after processing
// has started).

// Omitting --slide-ms must be exactly the fixed-window behavior, not merely
// a similar one: the same input run with no --slide-ms and with an explicit
// --slide-ms equal to --window-ms produces byte-identical output -- every
// event lands only in its own fixed window and the start, end, count, sum
// and ordering of the result lines all match.
func TestAggregateCLISlideOmittedMatchesExplicitWindowLength(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":100,"value":2}`,
		`{"type":"event","key":"k","time":1500,"value":3}`,
		`{"type":"watermark","time":2000}`,
	}, "\n") + "\n"
	want := []windowLine{
		{Key: "k", Start: 0, End: 1000, Count: 1, Sum: 2},
		{Key: "k", Start: 1000, End: 2000, Count: 1, Sum: 3},
	}

	codeOmit, outOmit, errOmit := runAggregateCLI(t, input, "--window-ms", "1000")
	if codeOmit != 0 {
		t.Fatalf("omitted --slide-ms: exit code = %d, want 0; stderr: %s", codeOmit, errOmit)
	}
	if errOmit != "" {
		t.Fatalf("omitted --slide-ms: stderr = %q, want empty", errOmit)
	}
	assertWindowLines(t, outOmit, want)

	codeEq, outEq, errEq := runAggregateCLI(t, input, "--window-ms", "1000", "--slide-ms", "1000")
	if codeEq != 0 {
		t.Fatalf("explicit --slide-ms 1000: exit code = %d, want 0; stderr: %s", codeEq, errEq)
	}
	if errEq != "" {
		t.Fatalf("explicit --slide-ms 1000: stderr = %q, want empty", errEq)
	}
	assertWindowLines(t, outEq, want)

	if outOmit != outEq {
		t.Fatalf("omitted --slide-ms output %q differs from explicit --slide-ms 1000 output %q", outOmit, outEq)
	}
}

// A slide interval below the window length makes windows overlap, and every
// event counts in full in each window that contains it: the value is neither
// split across the windows nor merely reflected in shifted start/end times.
// With length 1000 and interval 600 the time-700 event belongs to [0,1000)
// and [600,1600) and adds its full 2 to both sums; the time-1000 event falls
// exactly on the end of [0,1000) and is outside it (left-closed right-open),
// so that window reports count 1, not 2.
func TestAggregateCLISlideCountsEventFullyInEveryContainingWindow(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":700,"value":2}`,
		`{"type":"event","key":"k","time":1000,"value":3}`,
		`{"type":"watermark","time":1600}`,
	}, "\n") + "\n"

	code, stdout, stderr := runAggregateCLI(t, input, "--window-ms", "1000", "--slide-ms", "600")
	if code != 0 {
		t.Fatalf("exit code = %d, want 0; stderr: %s", code, stderr)
	}
	if stderr != "" {
		t.Fatalf("stderr = %q, want empty", stderr)
	}
	assertWindowLines(t, stdout, []windowLine{
		{Key: "k", Start: 0, End: 1000, Count: 1, Sum: 2},
		{Key: "k", Start: 600, End: 1600, Count: 2, Sum: 5},
	})
}

// The interval does not have to divide the window length. The same input
// under length 1000 with interval 600 (not a divisor), interval 500 (a
// divisor) and no interval (fixed windows) yields three different result
// sets, proving the non-dividing interval is honored as given: starts land
// on 0, 600, 1200, ... rather than being snapped to a divisor or to the
// fixed grid, and the window [1000,2000) that a dividing interval would
// close at watermark 1600 stays open and silent here.
func TestAggregateCLISlideNeedNotDivideWindowLength(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":600,"value":2}`,
		`{"type":"event","key":"k","time":1100,"value":3}`,
		`{"type":"watermark","time":1600}`,
	}, "\n") + "\n"

	cases := []struct {
		name string
		args []string
		want []windowLine
	}{
		{
			"non-dividing interval 600",
			[]string{"--window-ms", "1000", "--slide-ms", "600"},
			[]windowLine{
				{Key: "k", Start: 0, End: 1000, Count: 1, Sum: 2},
				{Key: "k", Start: 600, End: 1600, Count: 2, Sum: 5},
			},
		},
		{
			"dividing interval 500",
			[]string{"--window-ms", "1000", "--slide-ms", "500"},
			[]windowLine{
				{Key: "k", Start: 0, End: 1000, Count: 1, Sum: 2},
				{Key: "k", Start: 500, End: 1500, Count: 2, Sum: 5},
			},
		},
		{
			"fixed windows",
			[]string{"--window-ms", "1000"},
			[]windowLine{
				{Key: "k", Start: 0, End: 1000, Count: 1, Sum: 2},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, stdout, stderr := runAggregateCLI(t, input, tc.args...)
			if code != 0 {
				t.Fatalf("exit code = %d, want 0; stderr: %s", code, stderr)
			}
			if stderr != "" {
				t.Fatalf("stderr = %q, want empty", stderr)
			}
			assertWindowLines(t, stdout, tc.want)
		})
	}
}

// Windows still start at time zero and stay left-closed right-open under a
// slide interval: an event exactly on a window start (time 800, the start of
// [800,1800) with interval 400) belongs to that window, and an event exactly
// on a window end (time 1000, the end of [0,1000)) does not.
func TestAggregateCLISlideWindowStartInclusiveEndExclusive(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":800,"value":5}`,
		`{"type":"event","key":"k","time":1000,"value":7}`,
		`{"type":"watermark","time":1800}`,
	}, "\n") + "\n"

	code, stdout, stderr := runAggregateCLI(t, input, "--window-ms", "1000", "--slide-ms", "400")
	if code != 0 {
		t.Fatalf("exit code = %d, want 0; stderr: %s", code, stderr)
	}
	if stderr != "" {
		t.Fatalf("stderr = %q, want empty", stderr)
	}
	assertWindowLines(t, stdout, []windowLine{
		{Key: "k", Start: 0, End: 1000, Count: 1, Sum: 5},    // time 1000 excluded
		{Key: "k", Start: 400, End: 1400, Count: 2, Sum: 12}, // both events in full
		{Key: "k", Start: 800, End: 1800, Count: 2, Sum: 12}, // time 800 included
	})
}

// Only windows whose end is less than or equal to the watermark are emitted.
// With length 1000 and interval 600 a watermark of 1000 closes [0,1000) but
// leaves [600,1600) open, and the still-open window stays silent when the
// input ends -- it is never flushed at end of input.
func TestAggregateCLISlideUnclosedWindowsStaySilentAtEndOfInput(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":700,"value":2}`,
		`{"type":"watermark","time":1000}`,
	}, "\n") + "\n"

	code, stdout, stderr := runAggregateCLI(t, input, "--window-ms", "1000", "--slide-ms", "600")
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

// Each key accumulates separately inside every overlapping window, and the
// result lines keep the existing order: window end ascending, then key in
// UTF-8 byte order within the same end.
func TestAggregateCLISlideOrdersByEndThenKey(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"b","time":100,"value":1}`,
		`{"type":"event","key":"a","time":100,"value":2}`,
		`{"type":"event","key":"a","time":600,"value":3}`,
		`{"type":"watermark","time":1500}`,
	}, "\n") + "\n"

	code, stdout, stderr := runAggregateCLI(t, input, "--window-ms", "1000", "--slide-ms", "500")
	if code != 0 {
		t.Fatalf("exit code = %d, want 0; stderr: %s", code, stderr)
	}
	if stderr != "" {
		t.Fatalf("stderr = %q, want empty", stderr)
	}
	assertWindowLines(t, stdout, []windowLine{
		{Key: "a", Start: 0, End: 1000, Count: 2, Sum: 5},
		{Key: "b", Start: 0, End: 1000, Count: 1, Sum: 1},
		{Key: "a", Start: 500, End: 1500, Count: 1, Sum: 3},
	})
}

// An invalid --slide-ms is an argument error, not a record error and not a
// reason to fall back to the fixed-window default: an explicit zero (unlike
// omitting the flag), a negative value, an interval larger than the window
// length, a value outside the signed 64-bit range and non-integer text all
// exit 2 with the --slide-ms problem on standard error before any input is
// read. Standard output stays empty even when the input would close windows,
// and a malformed first record must not turn the argument problem into a
// record error carrying an input line number.
func TestAggregateCLIInvalidSlideExits2BeforeInput(t *testing.T) {
	badSlides := []struct {
		name  string
		value string
	}{
		{"explicit zero", "0"},
		{"negative", "-600"},
		{"min int64", "-9223372036854775808"},
		{"larger than window", "1500"},
		{"above max int64", "9223372036854775808"},
		{"far above max int64", "99999999999999999999999999"},
		{"not an integer", "fast"},
		{"not a whole number", "1.5"},
		{"empty", ""},
	}
	// A first record that is not even valid JSON: if the bad interval were
	// somehow tolerated, this line would surface as a record error at line 1.
	malformedInput := "this is not json\n"
	// Records that would close [0,1000) and print a result under any valid
	// interval, proving nothing is read when the argument is rejected.
	closingInput := strings.Join([]string{
		`{"type":"event","key":"k","time":100,"value":2}`,
		`{"type":"watermark","time":1000}`,
	}, "\n") + "\n"

	for _, bs := range badSlides {
		for _, input := range []struct {
			name  string
			value string
		}{
			{"with malformed first record", malformedInput},
			{"with window-closing input", closingInput},
			{"with empty input", ""},
		} {
			t.Run(bs.name+"/"+input.name, func(t *testing.T) {
				code, stdout, stderr := runAggregateCLI(t, input.value, "--window-ms", "1000", "--slide-ms", bs.value)
				if code != 2 {
					t.Fatalf("exit code = %d, want 2; stderr: %s", code, stderr)
				}
				if stdout != "" {
					t.Fatalf("stdout = %q, want empty: invalid --slide-ms must be rejected before any record is read", stdout)
				}
				if !strings.Contains(stderr, "slide-ms") {
					t.Fatalf("stderr = %q, want it to name the --slide-ms problem", stderr)
				}
				if strings.Contains(stderr, "line 1") {
					t.Fatalf("stderr = %q, want an argument error, not a record error with an input line number", stderr)
				}
			})
		}
	}
}

// --slide-ms never relaxes the existing --window-ms requirement: giving an
// interval without a window length is still an argument error (exit 2)
// naming --window-ms, before any input is read.
func TestAggregateCLISlideDoesNotReplaceRequiredWindow(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":100,"value":2}`,
		`{"type":"watermark","time":1000}`,
	}, "\n") + "\n"

	code, stdout, stderr := runAggregateCLI(t, input, "--slide-ms", "600")
	if code != 2 {
		t.Fatalf("exit code = %d, want 2; stderr: %s", code, stderr)
	}
	if stdout != "" {
		t.Fatalf("stdout = %q, want empty: missing --window-ms must be rejected before any record is read", stdout)
	}
	if !strings.Contains(stderr, "window-ms") {
		t.Fatalf("stderr = %q, want it to name the missing --window-ms", stderr)
	}
}
