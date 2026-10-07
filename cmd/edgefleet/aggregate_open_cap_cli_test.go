package main

import (
	"bytes"
	"os"
	"os/exec"
	"strings"
	"testing"
)

// Command-line regression coverage for the optional --max-open-windows cap
// added to "aggregate". These tests drive the real main() in a child process
// (see main_test.go) so they pin what the flag actually decides end to end:
//
//   - omitting the flag keeps the unlimited behavior exactly;
//   - an explicit positive cap reaches the library and bounds open
//     key/window pairs, with repeated and cross-partition contributions to an
//     existing pair taking no new slot and sliding events paying only for the
//     pairs they open for the first time;
//   - an event that would exceed the cap fails at its physical line with exit
//     code 1, naming the line, the key, the open count, the new windows
//     needed and the configured cap, while earlier output stays and later
//     records are never read;
//   - zero, negative, non-integer and out-of-int64 values are argument
//     errors: exit 2, stderr only, empty stdout, before any input is read.

// Omitting --max-open-windows is the unlimited behavior: with no watermark
// ever advancing, more distinct keys than any small cap would allow are all
// retained and closed at the end; the run exits 0 with empty stderr.
func TestAggregateCLIMaxOpenWindowsOmittedIsUnlimited(t *testing.T) {
	var lines []string
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		lines = append(lines, `{"type":"event","key":"`+k+`","time":100,"value":1}`)
	}
	lines = append(lines, `{"type":"watermark","time":1000}`)
	input := strings.Join(lines, "\n") + "\n"

	code, stdout, stderr := runAggregateCLI(t, input, "--window-ms", "1000")
	if code != 0 {
		t.Fatalf("exit code = %d, want 0; stderr: %s", code, stderr)
	}
	if stderr != "" {
		t.Fatalf("stderr = %q, want empty", stderr)
	}
	assertWindowLines(t, stdout, []windowLine{
		{Key: "a", Start: 0, End: 1000, Count: 1, Sum: 1},
		{Key: "b", Start: 0, End: 1000, Count: 1, Sum: 1},
		{Key: "c", Start: 0, End: 1000, Count: 1, Sum: 1},
		{Key: "d", Start: 0, End: 1000, Count: 1, Sum: 1},
		{Key: "e", Start: 0, End: 1000, Count: 1, Sum: 1},
	})
}

// A positive cap is honored at the command entry: distinct keys in the same
// window are accepted exactly up to the cap (the second key lands on it), and
// the next key is rejected at its physical line with exit code 1. The error
// names every required figure -- line, key, current open count, new windows
// needed and the cap -- and nothing is printed to stdout before a watermark.
func TestAggregateCLIMaxOpenWindowsExceededExits1(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"a","time":100,"value":1}`, // line 1: 1 open
		`{"type":"event","key":"b","time":200,"value":2}`, // line 2: 2 open == cap
		`{"type":"event","key":"c","time":300,"value":3}`, // line 3: needs 1 more -> rejected
		`{"type":"event","key":"d","time":400,"value":4}`, // line 4: never read
	}, "\n") + "\n"

	code, stdout, stderr := runAggregateCLI(t, input, "--window-ms", "1000", "--max-open-windows", "2")
	if code != 1 {
		t.Fatalf("exit code = %d, want 1; stderr: %s", code, stderr)
	}
	if stdout != "" {
		t.Fatalf("no window closed before the failure, stdout = %q", stdout)
	}
	for _, want := range []string{
		"line 3",
		`key "c"`,
		"2 window(s) currently open",
		"needs 1 new window(s)",
		"limit 2",
	} {
		if !strings.Contains(stderr, want) {
			t.Fatalf("stderr = %q, want it to mention %q", stderr, want)
		}
	}
	if strings.Contains(stderr, "line 4") {
		t.Fatalf("processing must stop at the failing line; stderr referenced line 4: %q", stderr)
	}
}

// Results fully written before the over-cap line stay on stdout, and records
// after it never run: two windows close first and remain, the over-cap event
// adds nothing, and a later watermark that would close its window is never
// read.
func TestAggregateCLIMaxOpenWindowsRetainsEarlierOutput(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"a","time":100,"value":1}`,  // line 1
		`{"type":"event","key":"b","time":100,"value":2}`,  // line 2
		`{"type":"watermark","time":1000}`,                 // line 3: closes a and b, frees both
		`{"type":"event","key":"c","time":1100,"value":3}`, // line 4
		`{"type":"event","key":"d","time":1100,"value":4}`, // line 5: cap 2 reached
		`{"type":"event","key":"e","time":1100,"value":5}`, // line 6: exceeds -> rejected
		`{"type":"watermark","time":2000}`,                 // line 7: never read
	}, "\n") + "\n"

	code, stdout, stderr := runAggregateCLI(t, input, "--window-ms", "1000", "--max-open-windows", "2")
	if code != 1 {
		t.Fatalf("exit code = %d, want 1; stderr: %s", code, stderr)
	}
	for _, want := range []string{"line 6", `key "e"`, "needs 1 new window(s)"} {
		if !strings.Contains(stderr, want) {
			t.Fatalf("stderr = %q, want it to mention %q", stderr, want)
		}
	}
	assertWindowLines(t, stdout, []windowLine{
		{Key: "a", Start: 0, End: 1000, Count: 1, Sum: 1},
		{Key: "b", Start: 0, End: 1000, Count: 1, Sum: 2},
	})
}

// Repeated events for a key/window pair that already exists take no new slot,
// so a cap of one still accepts every later event for that same pair and the
// closed window reports the full count and sum.
func TestAggregateCLIMaxOpenWindowsRepeatedPairStillAccepted(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"a","time":100,"value":1}`,
		`{"type":"event","key":"a","time":200,"value":2}`,
		`{"type":"event","key":"a","time":300,"value":3}`,
		`{"type":"watermark","time":1000}`,
	}, "\n") + "\n"

	code, stdout, stderr := runAggregateCLI(t, input, "--window-ms", "1000", "--max-open-windows", "1")
	if code != 0 {
		t.Fatalf("exit code = %d, want 0; stderr: %s", code, stderr)
	}
	if stderr != "" {
		t.Fatalf("stderr = %q, want empty", stderr)
	}
	assertWindowLines(t, stdout, []windowLine{
		{Key: "a", Start: 0, End: 1000, Count: 3, Sum: 6},
	})
}

// Sliding windows plus the cap: one event can open several overlapping pairs
// at once. Length 1000/slide 600, event at 700 opens [0,1000) and [600,1600)
// and is accepted exactly at a cap of 2; a second key at the same time would
// need two more pairs and is rejected as a whole line. Another event for the
// first key at another overlapping time folds into existing pairs where they
// exist and opens only the genuinely new start.
func TestAggregateCLIMaxOpenWindowsSlidingAtomicAndExistingPairsFree(t *testing.T) {
	// Exactly fills two slots and later closes both windows.
	fills := strings.Join([]string{
		`{"type":"event","key":"a","time":700,"value":1}`,
		`{"type":"event","key":"a","time":1200,"value":2}`, // reuses [600,1600), opens [1200,2200): cap 3 not set here
		`{"type":"watermark","time":2200}`,
	}, "\n") + "\n"
	code, stdout, stderr := runAggregateCLI(t, fills, "--window-ms", "1000", "--slide-ms", "600", "--max-open-windows", "3")
	if code != 0 {
		t.Fatalf("reusing an existing overlapping pair must take no slot: %s", stderr)
	}
	assertWindowLines(t, stdout, []windowLine{
		{Key: "a", Start: 0, End: 1000, Count: 1, Sum: 1},
		{Key: "a", Start: 600, End: 1600, Count: 2, Sum: 3},
		{Key: "a", Start: 1200, End: 2200, Count: 1, Sum: 2},
	})

	exceeds := strings.Join([]string{
		`{"type":"event","key":"a","time":700,"value":1}`, // line 1: 2 new pairs == cap
		`{"type":"event","key":"b","time":700,"value":2}`, // line 2: needs 2 more -> rejected
	}, "\n")
	code, stdout, stderr = runAggregateCLI(t, exceeds, "--window-ms", "1000", "--slide-ms", "600", "--max-open-windows", "2")
	if code != 1 {
		t.Fatalf("exit code = %d, want 1; stderr: %s", code, stderr)
	}
	for _, want := range []string{"line 2", `key "b"`, "2 window(s) currently open", "needs 2 new window(s)", "limit 2"} {
		if !strings.Contains(stderr, want) {
			t.Fatalf("stderr = %q, want it to mention %q", stderr, want)
		}
	}
	if stdout != "" {
		t.Fatalf("the rejected event must not enter either window and nothing closed yet: %q", stdout)
	}
}

// Partitions never consume separate slots: the same key in the same window
// contributed from both partitions merges into the one pair, so a cap of one
// accepts both.
func TestAggregateCLIMaxOpenWindowsPartitionsMergeSlots(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"a","time":100,"value":1,"partition":0}`,
		`{"type":"event","key":"a","time":200,"value":2,"partition":1}`,
		`{"type":"watermark","time":5000,"partition":0}`,
		`{"type":"watermark","time":5000,"partition":1}`,
	}, "\n") + "\n"

	code, stdout, stderr := runAggregateCLI(t, input, "--window-ms", "1000", "--partitions", "2", "--max-open-windows", "1")
	if code != 0 {
		t.Fatalf("exit code = %d, want 0; stderr: %s", code, stderr)
	}
	if stderr != "" {
		t.Fatalf("stderr = %q, want empty", stderr)
	}
	assertWindowLines(t, stdout, []windowLine{
		{Key: "a", Start: 0, End: 1000, Count: 2, Sum: 3},
	})
}

// A slot released by a window that closed and was written is reusable: with a
// cap of one the second key is accepted only after the first key's window
// closes, both windows appearing on stdout in order.
func TestAggregateCLIMaxOpenWindowsSlotReusedAfterClosure(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"a","time":100,"value":1}`,
		`{"type":"watermark","time":1000}`,
		`{"type":"event","key":"b","time":1100,"value":2}`,
		`{"type":"watermark","time":2000}`,
	}, "\n") + "\n"

	code, stdout, stderr := runAggregateCLI(t, input, "--window-ms", "1000", "--max-open-windows", "1")
	if code != 0 {
		t.Fatalf("exit code = %d, want 0; stderr: %s", code, stderr)
	}
	if stderr != "" {
		t.Fatalf("stderr = %q, want empty", stderr)
	}
	assertWindowLines(t, stdout, []windowLine{
		{Key: "a", Start: 0, End: 1000, Count: 1, Sum: 1},
		{Key: "b", Start: 1000, End: 2000, Count: 1, Sum: 2},
	})
}

// Every invalid --max-open-windows spelling is an argument error: exit 2, the
// problem named on stderr, nothing on stdout, before input is read. Input that
// would immediately close a window must produce no results, and empty input
// must not hide the bad argument either.
func TestAggregateCLIInvalidMaxOpenWindowsExits2BeforeInput(t *testing.T) {
	bad := []struct {
		name  string
		value string
	}{
		{"zero", "0"},
		{"negative", "-2"},
		{"min int64", "-9223372036854775808"},
		{"not an integer", "two"},
		{"not a whole number", "1.0"},
		{"above max int64", "9223372036854775808"},
		{"far above max int64", "314159265358979323846264"},
	}
	closingInput := strings.Join([]string{
		`{"type":"event","key":"k","time":100,"value":2}`,
		`{"type":"watermark","time":1000}`,
	}, "\n") + "\n"

	for _, bc := range bad {
		for _, in := range []struct {
			name, text string
		}{
			{"with window-closing input", closingInput},
			{"with empty input", ""},
		} {
			t.Run(bc.name+"/"+in.name, func(t *testing.T) {
				code, stdout, stderr := runAggregateCLI(t, in.text, "--window-ms", "1000", "--max-open-windows", bc.value)
				if code != 2 {
					t.Fatalf("exit code = %d, want 2; stderr: %s", code, stderr)
				}
				if stdout != "" {
					t.Fatalf("stdout = %q, want empty: the bad flag must be rejected before input", stdout)
				}
				if !strings.Contains(stderr, "max-open-windows") {
					t.Fatalf("stderr = %q, want it to name --max-open-windows", stderr)
				}
				if strings.Contains(stderr, "line ") {
					t.Fatalf("an argument error must carry no input line number: %q", stderr)
				}
			})
		}
	}
}

// The largest signed 64-bit integer is a legal cap and, given a fixed window
// and a handful of distinct keys, behaves as effectively unlimited: the run
// exits 0 with all windows closed.
func TestAggregateCLIMaxOpenWindowsMaxInt64Accepted(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"a","time":100,"value":1}`,
		`{"type":"event","key":"b","time":100,"value":2}`,
		`{"type":"watermark","time":1000}`,
	}, "\n") + "\n"

	code, stdout, stderr := runAggregateCLI(t, input, "--window-ms", "1000", "--max-open-windows", "9223372036854775807")
	if code != 0 {
		t.Fatalf("exit code = %d, want 0; stderr: %s", code, stderr)
	}
	if stderr != "" {
		t.Fatalf("stderr = %q, want empty", stderr)
	}
	assertWindowLines(t, stdout, []windowLine{
		{Key: "a", Start: 0, End: 1000, Count: 1, Sum: 1},
		{Key: "b", Start: 0, End: 1000, Count: 1, Sum: 2},
	})
}

// The flag is visible in the aggregate subcommand's short usage, and the full
// "edgefleet help" text explains how slots are counted, so the cap can never
// be mistaken for an event-count or per-partition limit.
func TestAggregateCLIMaxOpenWindowsHelpExplainsSlots(t *testing.T) {
	// --help under ContinueOnError prints the short flag usage to stderr and
	// exits 2 before input is read.
	code, stdout, stderr := runAggregateCLI(t, "", "--window-ms", "1000", "--help")
	if code != 2 {
		t.Fatalf("--help exits 2 from flag parsing, got %d", code)
	}
	if stdout != "" {
		t.Fatalf("usage goes to stderr, stdout = %q", stdout)
	}
	if !strings.Contains(stderr, "--max-open-windows <count>") {
		t.Fatalf("short usage = %q, want it to list --max-open-windows", stderr)
	}

	// The detailed rule lives in the top-level "edgefleet help" text, which
	// the child reaches through the same helper harness as the aggregate
	// command but with the "help" subcommand.
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("locate test binary: %v", err)
	}
	cmd := exec.Command(exe, "-test.run=^TestHelperProcess$", "--", "help")
	cmd.Env = append(os.Environ(), "GO_WANT_HELPER_PROCESS=1")
	var helpOut bytes.Buffer
	cmd.Stdout = &helpOut
	if err := cmd.Run(); err != nil {
		t.Fatalf("edgefleet help failed: %v", err)
	}
	helpText := helpOut.String()
	for _, want := range []string{
		"--max-open-windows",
		"decoded event key",
		"one window interval",
		"different partitions merge into the one slot",
	} {
		if !strings.Contains(helpText, want) {
			t.Fatalf("help text missing %q:\n%s", want, helpText)
		}
	}
}
