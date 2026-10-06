package main

// Command-line regression tests for cumulative missed-duty counts beyond the
// 32-bit integer range. The domain comparison is tested in package
// edgefleet; these tests pin what a user sees through the real
// `edgefleet heartbeat health` entry point on both 32-bit and 64-bit builds:
//
//   - a plain query must print the original full counter (never a wrapped or
//     zeroed one) and alarm on 2147483648 with tolerance 0, while equality at
//     the largest 32-bit tolerance stays silent;
//   - a --missed-since-seq baseline around 2^32 shows new_missed=1 from the
//     exact int64 difference and does not alarm at tolerance 1, while the
//     plain query over the same saved data alarms on the full cumulative
//     value;
//   - the maximum legal counter 9223372036854775807 is accepted on submit and
//     query and displayed in full rather than rejected or shown as unknown.
//
// The --tolerated-misses flag is a native int, so on a 32-bit binary it can
// name at most 2147483647; the tolerances used here stay within that bound so
// the same expectations hold on every width.

import (
	"strings"
	"testing"
)

func TestCLIHealthLargeCumulativeMissedAlarms(t *testing.T) {
	dir := t.TempDir()
	submitBatch(t, dir, `[{"node":"wide","seq":1,"collected_at":"2026-10-01T11:59:59Z","version":"1.0","height":1,"missed":2147483648}]`)

	// 2^31 with tolerance 0 must alarm on both widths; the first line shows
	// the original full counter.
	out, errOut, code := healthQueryAt(t, dir, cliQueryAt,
		"--node", "wide", "--expected-version", "1.0", "--tolerated-misses", "0")
	if code != 0 {
		t.Fatalf("health must exit 0 even with an alarm: %d stderr=%q", code, errOut)
	}
	if !strings.Contains(out, "missed=2147483648 ") {
		t.Errorf("output must show the full cumulative missed=2147483648: %q", out)
	}
	if !strings.Contains(out, "missed duties above tolerance") {
		t.Errorf("2147483648 > 0 must alarm on every width: %q", out)
	}
	if strings.Contains(out, "offline") {
		t.Errorf("fresh telemetry stays online regardless of missed: %q", out)
	}

	// Strictly over even the largest 32-bit representable tolerance: alarm.
	out, _, code = healthQueryAt(t, dir, cliQueryAt,
		"--node", "wide", "--expected-version", "1.0", "--tolerated-misses", "2147483647")
	if code != 0 || !strings.Contains(out, "missed duties above tolerance") {
		t.Errorf("2147483648 strictly over tolerance 2147483647 must alarm: code=%d out=%q", code, out)
	}
}

func TestCLIHealthLargeMissedEqualityBoundary(t *testing.T) {
	dir := t.TempDir()
	submitBatch(t, dir, `[{"node":"wideeq","seq":1,"collected_at":"2026-10-01T11:59:59Z","version":"1.0","height":1,"missed":2147483647}]`)

	// Equality with the largest 32-bit tolerance must not alarm on any width.
	out, errOut, code := healthQueryAt(t, dir, cliQueryAt,
		"--node", "wideeq", "--expected-version", "1.0", "--tolerated-misses", "2147483647")
	if code != 0 {
		t.Fatalf("exit=%d stderr=%q", code, errOut)
	}
	lines := outputLines(out)
	want := "node=wideeq status=online seq=1 collected_at=2026-10-01T11:59:59Z version=1.0 height=1 missed=2147483647 findings=[]"
	if len(lines) != 1 || lines[0] != want {
		t.Errorf("equality line = %q\nwant             %q", out, want)
	}
}

func TestCLIHealthSinceLargeBaselineShowsNewMissedOnly(t *testing.T) {
	dir := t.TempDir()
	submitBatch(t, dir, `[
	  {"node":"bwide","seq":1,"collected_at":"2026-10-01T11:59:58Z","version":"1.0","height":100,"missed":4294967296},
	  {"node":"bwide","seq":2,"collected_at":"2026-10-01T11:59:59Z","version":"1.0","height":101,"missed":4294967297}
	]`)

	// Plain query judges the full cumulative 4294967297 against tolerance 1.
	out, _, code := healthQueryAt(t, dir, cliQueryAt,
		"--node", "bwide", "--expected-version", "1.0", "--tolerated-misses", "1")
	if code != 0 {
		t.Fatalf("plain query exit=%d out=%q", code, out)
	}
	if !strings.Contains(out, "missed=4294967297 ") || !strings.Contains(out, "missed duties above tolerance") {
		t.Errorf("plain query must show full missed=4294967297 and alarm: %q", out)
	}

	// Baseline query: only one new missed duty after seq 1; equality with
	// tolerance 1 silences the alarm. Both full counters still print.
	var errOut string
	out, errOut, code = healthQueryAt(t, dir, cliQueryAt,
		"--node", "bwide", "--expected-version", "1.0",
		"--tolerated-misses", "1", "--missed-since-seq", "1")
	if code != 0 {
		t.Fatalf("baseline query exit=%d stderr=%q", code, errOut)
	}
	lines := outputLines(out)
	if len(lines) != 2 {
		t.Fatalf("want two lines, got %q", out)
	}
	wantFirst := "node=bwide status=online seq=2 collected_at=2026-10-01T11:59:59Z version=1.0 height=101 missed=4294967297 findings=[]"
	if lines[0] != wantFirst {
		t.Errorf("first line = %q\nwant          %q", lines[0], wantFirst)
	}
	wantSecond := "baseline_seq=1 baseline_missed=4294967296 new_missed=1 tolerated_misses=1"
	if lines[1] != wantSecond {
		t.Errorf("second line = %q\nwant           %q", lines[1], wantSecond)
	}
}

func TestCLIHealthMaxInt64CounterAcceptedAndShown(t *testing.T) {
	dir := t.TempDir()
	submitBatch(t, dir, `[{"node":"maxc","seq":1,"collected_at":"2026-10-01T11:59:59Z","version":"1.0","height":1,"missed":9223372036854775807}]`)

	// The legal upper bound must query successfully, print in full and alarm;
	// never be refused, zeroed or reported as unjudgeable.
	out, errOut, code := healthQueryAt(t, dir, cliQueryAt,
		"--node", "maxc", "--expected-version", "1.0", "--tolerated-misses", "0")
	if code != 0 {
		t.Fatalf("max-int64 counter must query successfully: %d stderr=%q", code, errOut)
	}
	if !strings.Contains(out, "missed=9223372036854775807 ") {
		t.Errorf("output must show the full max int64 counter: %q", out)
	}
	if !strings.Contains(out, "missed duties above tolerance") {
		t.Errorf("max int64 counter over tolerance 0 must alarm: %q", out)
	}
	if strings.Contains(out, "无法判断") {
		t.Errorf("large counters are never unjudgeable: %q", out)
	}
}
