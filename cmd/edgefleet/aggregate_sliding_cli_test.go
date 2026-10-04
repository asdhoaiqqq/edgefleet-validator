package main

import (
	"strings"
	"testing"
)

// Command-line regression coverage for how "aggregate" treats --slide-ms at
// the command entry point. The package tests cover the sliding window math
// directly; these tests drive the real main() in a child process (see
// main_test.go for the harness) so they guard what the flag actually decides:
//
//   - omitting --slide-ms is fixed-window mode, byte for byte identical to
//     passing --slide-ms equal to --window-ms;
//   - an explicit positive interval below the window length reaches the
//     aggregator and each event counts in full in every window containing it
//     (the value is never split, and this is not just relabeled start/end);
//   - a non-dividing interval is legal and produces a distinct result grid;
//   - an explicitly given zero is rejected (exit 2) even though an omitted
//     flag defaults to fixed windows, along with negative, oversized and
//     out-of-int64 values, all before the first input record is read.

// Omitting --slide-ms and setting it explicitly to the window length must run
// the same fixed windows through the command entry point. With two keys and
// out-of-order records this asserts start/end, count and sum per line, the
// end-then-key ordering, and the successful exit with an empty standard error
// for both invocations; the two runs' raw output is compared as well so no
// whitespace or field difference can slip in between the two ways of asking.
func TestAggregateCLISlideOmittedEqualsExplicitWindowLength(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"b","time":800,"value":-2}`,
		`{"type":"event","key":"a","time":1500,"value":7}`,
		`{"type":"event","key":"a","time":1200,"value":5}`, // out of order, same fixed window
		`{"type":"event","key":"b","time":0,"value":4}`,
		`{"type":"watermark","time":2000}`,
	}, "\n") + "\n"

	type run struct {
		name   string
		args   []string
		stdout string
	}
	runs := []run{
		{name: "slide omitted", args: []string{"--window-ms", "1000"}},
		{name: "slide equals window", args: []string{"--window-ms", "1000", "--slide-ms", "1000"}},
	}
	expected := []windowLine{
		{Key: "b", Start: 0, End: 1000, Count: 2, Sum: 2},     // 4 and -2
		{Key: "a", Start: 1000, End: 2000, Count: 2, Sum: 12}, // 5 and 7
	}

	for i := range runs {
		code, stdout, stderr := runAggregateCLI(t, input, runs[i].args...)
		if code != 0 {
			t.Fatalf("%s: exit code = %d, want 0; stderr: %s", runs[i].name, code, stderr)
		}
		if stderr != "" {
			t.Fatalf("%s: stderr = %q, want empty", runs[i].name, stderr)
		}
		assertWindowLines(t, stdout, expected)
		runs[i].stdout = stdout
	}
	if runs[0].stdout != runs[1].stdout {
		t.Fatalf("omitted slide output %q != explicit --slide-ms 1000 output %q", runs[0].stdout, runs[1].stdout)
	}
}

// A smaller explicit --slide-ms must do more than rename window boundaries:
// an event counts once, with its full value, in EVERY window that contains
// its time. Length 1000 with slide 600 gives [0,1000), [600,1600),
// [1200,2200); the boundary events prove the left-closed/right-open rule
// survives the command entry: time 0 enters the first window, time 1000 is
// excluded from it (an event exactly at a start belongs to that window, an
// event exactly at an end does not), and time 1600 belongs to neither of the
// first two. The watermark 1600 closes exactly end <= 1600, ordered by end
// then key; [1200,2200) stays open at end of input and must never appear.
func TestAggregateCLISlideExplicitCountsEventInFullInEveryContainingWindow(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":0,"value":5}`,    // [0,1000) only
		`{"type":"event","key":"k","time":700,"value":-3}`, // [0,1000) and [600,1600)
		`{"type":"event","key":"k","time":1000,"value":9}`, // [600,1600) only, not [0,1000)
		`{"type":"event","key":"k","time":1600,"value":2}`, // [1200,2200) only; excluded at [600,1600)'s end
		`{"type":"watermark","time":1000}`,
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
		{Key: "k", Start: 0, End: 1000, Count: 2, Sum: 2},   // 5 + (-3): full values, time-1000 excluded
		{Key: "k", Start: 600, End: 1600, Count: 2, Sum: 6}, // (-3) + 9: time-1600 excluded
	})
}

// Multiple keys inside one overlapping window are accumulated separately and
// emitted in the existing end-ascending, key (UTF-8 byte) order. A multi-key,
// multi-window batch closed by one watermark guards that the ordering rule is
// unchanged when --slide-ms is supplied.
func TestAggregateCLISlideExplicitClosureOrderByEndThenKey(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"中","time":700,"value":1}`,
		`{"type":"event","key":"A","time":700,"value":1}`,
		`{"type":"event","key":"é","time":100,"value":1}`,
		`{"type":"event","key":"A","time":1100,"value":1}`,
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
		{Key: "A", Start: 0, End: 1000, Count: 1, Sum: 1},
		{Key: "é", Start: 0, End: 1000, Count: 1, Sum: 1},
		{Key: "中", Start: 0, End: 1000, Count: 1, Sum: 1},
		{Key: "A", Start: 600, End: 1600, Count: 2, Sum: 2}, // t=700 and t=1100
		{Key: "中", Start: 600, End: 1600, Count: 1, Sum: 1}, // t=700
	})
}

// An interval that does not divide the window length is legal, and the
// observed output must distinguish it from both fixed windows and a dividing
// interval. Length 1000 with slide 300 starts windows at 0, 300, 600, ...;
// the event at 700 lands in [0,1000), [300,1300) and [600,1600) in full,
// while an equal fixed/1000 run would show one window and an equal dividing
// 500 run would show a different two-window grid. The empty [900,1900) is not
// emitted and [1200,2200)-and-later stay open past watermark 2000.
func TestAggregateCLISlideNonDividingIntervalIsDistinctGrid(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":0,"value":1}`,
		`{"type":"event","key":"k","time":700,"value":1}`,
		`{"type":"watermark","time":2000}`,
	}, "\n") + "\n"

	code, stdout, stderr := runAggregateCLI(t, input, "--window-ms", "1000", "--slide-ms", "300")
	if code != 0 {
		t.Fatalf("exit code = %d, want 0; stderr: %s", code, stderr)
	}
	if stderr != "" {
		t.Fatalf("stderr = %q, want empty", stderr)
	}
	nonDividing := stdout
	assertWindowLines(t, nonDividing, []windowLine{
		{Key: "k", Start: 0, End: 1000, Count: 2, Sum: 2},
		{Key: "k", Start: 300, End: 1300, Count: 1, Sum: 1},
		{Key: "k", Start: 600, End: 1600, Count: 1, Sum: 1},
	})

	// Same input as fixed windows: a single non-overlapping result.
	fixedCode, fixedOut, fixedErr := runAggregateCLI(t, input, "--window-ms", "1000")
	if fixedCode != 0 || fixedErr != "" {
		t.Fatalf("fixed run: code=%d stderr=%q", fixedCode, fixedErr)
	}
	assertWindowLines(t, fixedOut, []windowLine{
		{Key: "k", Start: 0, End: 1000, Count: 2, Sum: 2},
	})
	if nonDividing == fixedOut {
		t.Fatalf("slide 300 output must differ from fixed windows:\n%s", nonDividing)
	}

	// Same input with a dividing interval (500): windows at 0, 500, 1000, so
	// t=700 covers [0,1000) and [500,1500) only -- different starts/ends from
	// the 300 grid.
	divCode, divOut, divErr := runAggregateCLI(t, input, "--window-ms", "1000", "--slide-ms", "500")
	if divCode != 0 || divErr != "" {
		t.Fatalf("dividing run: code=%d stderr=%q", divCode, divErr)
	}
	assertWindowLines(t, divOut, []windowLine{
		{Key: "k", Start: 0, End: 1000, Count: 2, Sum: 2},
		{Key: "k", Start: 500, End: 1500, Count: 1, Sum: 1},
	})
	if nonDividing == divOut {
		t.Fatalf("slide 300 output must differ from slide 500:\n%s", nonDividing)
	}
}

// Windows that have not closed stay silent through end of input: the
// watermark closes only end <= watermark windows and the process exits 0
// without flushing the open overlap window or printing anything on stderr.
func TestAggregateCLISlideWatermarkClosesOnlyEndAtOrBelowIt(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":700,"value":5}`,
		`{"type":"watermark","time":999}`, // [0,1000) end 1000 > 999: nothing
		`{"type":"watermark","time":1000}`,
	}, "\n") + "\n"

	code, stdout, stderr := runAggregateCLI(t, input, "--window-ms", "1000", "--slide-ms", "600")
	if code != 0 {
		t.Fatalf("exit code = %d, want 0; stderr: %s", code, stderr)
	}
	if stderr != "" {
		t.Fatalf("stderr = %q, want empty", stderr)
	}
	// [600,1600) remains open after EOF and must not be emitted.
	assertWindowLines(t, stdout, []windowLine{
		{Key: "k", Start: 0, End: 1000, Count: 1, Sum: 5},
	})
}

// Explicit zero must be rejected even though omitting the flag defaults to
// fixed windows: the two spellings are observably different at the entry
// point. Zero exits 2 as an argument error with the flag name and the reason
// on stderr and nothing on stdout; omission exits 0 with normal results.
func TestAggregateCLISlideZeroRejectedWhileOmissionAccepted(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":100,"value":2}`,
		`{"type":"watermark","time":1000}`,
	}, "\n") + "\n"

	code, stdout, stderr := runAggregateCLI(t, input, "--window-ms", "1000", "--slide-ms", "0")
	if code != 2 {
		t.Fatalf("explicit zero: exit code = %d, want 2; stderr: %s", code, stderr)
	}
	if stdout != "" {
		t.Fatalf("explicit zero: stdout = %q, want empty", stdout)
	}
	if !strings.Contains(stderr, "--slide-ms") || !strings.Contains(stderr, "positive") {
		t.Fatalf("explicit zero: stderr = %q, want it to name --slide-ms and the positive-integer reason", stderr)
	}
	if strings.Contains(stderr, "line ") {
		t.Fatalf("argument error must not carry an input line number: stderr = %q", stderr)
	}

	omitCode, omitOut, omitErr := runAggregateCLI(t, input, "--window-ms", "1000")
	if omitCode != 0 {
		t.Fatalf("omitted slide: exit code = %d, want 0; stderr: %s", omitCode, omitErr)
	}
	if omitErr != "" {
		t.Fatalf("omitted slide: stderr = %q, want empty", omitErr)
	}
	assertWindowLines(t, omitOut, []windowLine{
		{Key: "k", Start: 0, End: 1000, Count: 1, Sum: 2},
	})
}

// Every invalid --slide-ms spelling fails as an argument error: exit code 2,
// the offending flag and reason on standard error, no window result on
// standard output, and the check happens before input is read. The last point
// is pinned two ways: input that would immediately close a window must still
// produce nothing, and input whose FIRST record is malformed must surface the
// parameter problem -- never a "line 1" record error and never a silent
// fallback to the default fixed interval.
func TestAggregateCLISlideInvalidValuesExit2BeforeInput(t *testing.T) {
	invalid := []struct {
		name     string
		value    string
		wantMsgs []string // substrings that must all appear on stderr
	}{
		{"zero", "0", []string{"--slide-ms", "positive"}},
		{"negative", "-600", []string{"--slide-ms", "positive"}},
		{"min int64", "-9223372036854775808", []string{"--slide-ms", "positive"}},
		{"larger than window", "1001", []string{"--slide-ms", "must not exceed", "--window-ms"}},
		{"equal plus one of larger window", "2001", []string{"--slide-ms", "must not exceed"}},
		{"above max int64", "9223372036854775808", []string{"slide-ms"}},
		{"far above max int64", "99999999999999999999999999", []string{"slide-ms"}},
		{"not an integer", "six-hundred", []string{"slide-ms"}},
		{"not a whole number", "6.0", []string{"slide-ms"}},
	}

	// Would close [0,1000) as soon as parsing succeeded.
	closingInput := strings.Join([]string{
		`{"type":"event","key":"k","time":100,"value":2}`,
		`{"type":"watermark","time":1000}`,
	}, "\n") + "\n"
	// First physical line is not a record at all: a record error would say
	// "line 1"; an argument error must win and mention no line number.
	malformedFirstInput := `{"type":"event",broken` + "\n"

	for _, iv := range invalid {
		window := "1000"
		if iv.name == "equal plus one of larger window" {
			window = "2000"
		}
		for _, in := range []struct {
			name, text string
		}{
			{"closing input", closingInput},
			{"malformed first record", malformedFirstInput},
			{"empty input", ""},
		} {
			t.Run(iv.name+"/"+in.name, func(t *testing.T) {
				code, stdout, stderr := runAggregateCLI(t, in.text, "--window-ms", window, "--slide-ms", iv.value)
				if code != 2 {
					t.Fatalf("exit code = %d, want 2; stderr: %s", code, stderr)
				}
				if stdout != "" {
					t.Fatalf("stdout = %q, want empty: invalid --slide-ms must fail before any window is produced", stdout)
				}
				for _, want := range iv.wantMsgs {
					if !strings.Contains(stderr, want) {
						t.Fatalf("stderr = %q, want it to mention %q", stderr, want)
					}
				}
				if strings.Contains(stderr, "line 1") || strings.Contains(stderr, "invalid JSON") {
					t.Fatalf("bad --slide-ms must be reported as an argument error, not a record problem: stderr = %q", stderr)
				}
			})
		}
	}
}

// The existing --window-ms requirement is unchanged by adding --slide-ms
// coverage: with slide given but window omitted the run still exits 2 naming
// --window-ms, before input is read; and a non-positive window exits 2 even
// when a valid slide accompanies it. No window results may appear.
func TestAggregateCLISlideKeepsWindowRequired(t *testing.T) {
	input := `{"type":"watermark","time":1000}` + "\n"

	cases := []struct {
		name     string
		args     []string
		wantText string
	}{
		{"window omitted, no slide", []string{"--slide-ms", "600"}, "--window-ms"},
		{"window omitted", nil, "--window-ms"},
		{"window zero despite valid slide", []string{"--window-ms", "0", "--slide-ms", "600"}, "--window-ms"},
		{"window negative despite valid slide", []string{"--window-ms", "-1000", "--slide-ms", "600"}, "--window-ms"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, stdout, stderr := runAggregateCLI(t, input, tc.args...)
			if code != 2 {
				t.Fatalf("exit code = %d, want 2; stderr: %s", code, stderr)
			}
			if stdout != "" {
				t.Fatalf("stdout = %q, want empty", stdout)
			}
			if !strings.Contains(stderr, tc.wantText) {
				t.Fatalf("stderr = %q, want it to mention %q", stderr, tc.wantText)
			}
		})
	}
}
