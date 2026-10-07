package main

// Command-line regression tests for the collected_at fractional-second rule
// at the real `heartbeat submit` / `history` / `health` entry points. Go's
// time.Parse silently truncates a fraction beyond nine digits, so two
// genuinely different collection moments (e.g. .1234567891 and .1234567892)
// would collapse into one nanosecond instant and be miscounted as
// duplicates. The command must instead:
//
//   - reject such input with a non-zero exit, stderr naming the 1-based
//     record position, collected_at and the offending fraction, and no
//     success line or counters on stdout;
//   - leave every node file untouched (an existing node's history
//     byte-for-byte intact, no file for a new node), even when the truncated
//     form would exactly match a saved record;
//   - keep accepting exact forms: no fraction, up to nine digits, and longer
//     fractions whose digits past the ninth are all zero;
//   - report a saved file carrying an unrepresentable fraction as data
//     corruption on history/health, and refuse further submits to that node
//     without overwriting the file.

import (
	"os"
	"strings"
	"testing"
)

// TestCLISubmitRejectsFractionBeyondNanoseconds sends the bad record first
// and second; the error must name its real 1-based position.
func TestCLISubmitRejectsFractionBeyondNanoseconds(t *testing.T) {
	cases := []struct {
		name  string
		stamp string
		frac  string
	}{
		{"tenth digit non-zero", "2026-10-01T11:59:00.1234567891Z", ".1234567891"},
		{"eleven digits, last non-zero", "2026-10-01T11:59:00.12345678901Z", ".12345678901"},
		{"non-zero far past the ninth", "2026-10-01T11:59:00.0000000000001Z", ".0000000000001"},
		{"non-zero tenth digit with offset", "2026-10-01T11:59:00.1234567891+08:00", ".1234567891"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bad := rec("bad", 1, tc.stamp)
			good := rec("good", 1, "2026-10-01T11:59:59Z")

			// Bad record first: position 1.
			dir := t.TempDir()
			out, errOut, code := submit(t, dir, "["+bad+","+good+"]")
			assertRejectedBatch(t, out, errOut, code, "1", "collected_at")
			if !strings.Contains(errOut, tc.frac) {
				t.Errorf("stderr must quote the offending fraction %q, got %q", tc.frac, errOut)
			}
			for _, node := range []string{"bad", "good"} {
				if _, err := os.Stat(nodeFileHexPath(dir, node)); !os.IsNotExist(err) {
					t.Errorf("no file may be created for %q after rejection, stat err=%v", node, err)
				}
			}

			// Same bad record second: position 2, and the leading good record
			// still must not land.
			dir = t.TempDir()
			out, errOut, code = submit(t, dir, "["+good+","+bad+"]")
			assertRejectedBatch(t, out, errOut, code, "2", "collected_at")
			if !strings.Contains(errOut, tc.frac) {
				t.Errorf("stderr must quote the offending fraction %q, got %q", tc.frac, errOut)
			}
			for _, node := range []string{"bad", "good"} {
				if _, err := os.Stat(nodeFileHexPath(dir, node)); !os.IsNotExist(err) {
					t.Errorf("no file may be created for %q after rejection, stat err=%v", node, err)
				}
			}
		})
	}
}

// TestCLISubmitFractionTruncationCollisionRejected is the reported regression
// end to end: two records whose collection moments differ only past the
// ninth fractional digit would truncate to the same instant. The batch must
// be refused — never silently merged into one record or counted as
// duplicates.
func TestCLISubmitFractionTruncationCollisionRejected(t *testing.T) {
	dir := t.TempDir()
	batch := "[" +
		rec("n1", 1, "2026-10-01T11:59:00.1234567891Z") + "," +
		rec("n1", 1, "2026-10-01T11:59:00.1234567892Z") +
		"]"
	out, errOut, code := submit(t, dir, batch)
	assertRejectedBatch(t, out, errOut, code, "1", "collected_at")
	if _, err := os.Stat(nodeFileHexPath(dir, "n1")); !os.IsNotExist(err) {
		t.Errorf("no file may be created for n1, stat err=%v", err)
	}
}

// TestCLISubmitUnrepresentableFractionNotADuplicate pins the atomicity rule
// for the trickiest case: the offending record's truncated form exactly
// matches an already-saved record. It must still fail the whole batch as
// invalid — never be absorbed as a duplicate of the saved one.
func TestCLISubmitUnrepresentableFractionNotADuplicate(t *testing.T) {
	dir := t.TempDir()
	// old seq 1 is saved at the instant the bad record would truncate to.
	submitBatch(t, dir, "["+rec("old", 1, "2026-10-01T11:59:00.123456789Z")+"]")
	oldBefore := readNodeFile(t, dir, "old")

	batch := "[" +
		rec("fresh", 1, "2026-10-01T11:59:58Z") + "," +
		rec("old", 2, "2026-10-01T11:59:58Z") + "," +
		// Truncates to the exact saved instant of old seq 1: position 3.
		rec("old", 1, "2026-10-01T11:59:00.1234567891Z") +
		"]"
	out, errOut, code := submit(t, dir, batch)
	assertRejectedBatch(t, out, errOut, code, "3", "collected_at")
	if !strings.Contains(errOut, ".1234567891") {
		t.Errorf("stderr must quote the offending fraction, got %q", errOut)
	}

	if got := readNodeFile(t, dir, "old"); got != oldBefore {
		t.Errorf("existing node file was modified during the rejected batch")
	}
	if _, err := os.Stat(nodeFileHexPath(dir, "fresh")); !os.IsNotExist(err) {
		t.Errorf("no file may be created for fresh, stat err=%v", err)
	}
	histOut, _, histCode := historyCLI(t, dir, "fresh")
	if histCode != 0 || strings.TrimRight(histOut, "\n") != "node=fresh no heartbeats" {
		t.Errorf("fresh must have no telemetry, code=%d out=%q", histCode, histOut)
	}
}

// TestCLISubmitAcceptsExactFractionForms pins the legal forms through the
// real command: no fraction, nine digits, and trailing zeros past the ninth
// all submit; a trailing-zero respelling of a saved instant is a duplicate.
func TestCLISubmitAcceptsExactFractionForms(t *testing.T) {
	dir := t.TempDir()

	// Nine-digit fraction saves as new.
	out, errOut, code := submit(t, dir, "["+rec("n1", 1, "2026-10-01T11:59:00.123456789Z")+"]")
	if code != 0 {
		t.Fatalf("nine-digit fraction must submit: exit=%d stderr=%q", code, errOut)
	}
	if strings.TrimRight(out, "\n") != "submitted: new=1 duplicate=0" {
		t.Errorf("stdout=%q, want new=1 duplicate=0", out)
	}

	// The same instant with a trailing zero appended is a duplicate, and the
	// escaped spelling of that text is the same duplicate.
	for _, stamp := range []string{
		"2026-10-01T11:59:00.1234567890Z",
		`2026-10-01T11:59:00.123456789\u0030Z`,
	} {
		out, errOut, code = submit(t, dir, "["+rec("n1", 1, stamp)+"]")
		if code != 0 {
			t.Fatalf("trailing-zero form %q must submit: exit=%d stderr=%q", stamp, code, errOut)
		}
		if strings.TrimRight(out, "\n") != "submitted: new=0 duplicate=1" {
			t.Errorf("trailing-zero form %q: stdout=%q, want new=0 duplicate=1", stamp, out)
		}
	}

	// No fraction and an all-zero long fraction are ordinary legal input.
	out, errOut, code = submit(t, dir, "["+
		rec("n1", 2, "2026-10-01T11:59:30Z")+","+
		rec("n1", 3, "2026-10-01T11:59:40.0000000000000Z")+
		"]")
	if code != 0 {
		t.Fatalf("exact fraction forms must submit: exit=%d stderr=%q", code, errOut)
	}
	if strings.TrimRight(out, "\n") != "submitted: new=2 duplicate=0" {
		t.Errorf("stdout=%q, want new=2 duplicate=0", out)
	}

	// The saved history holds exactly the three records, fractions intact.
	lines := historyLines(t, dir, "n1")
	if len(lines) != 3 {
		t.Fatalf("history = %q, want three records", lines)
	}
	if raw := readNodeFile(t, dir, "n1"); !strings.Contains(raw, ".123456789") {
		t.Errorf("saved file must keep the nanosecond fraction: %s", raw)
	}
}

// TestCLIStoredUnrepresentableFractionIsCorrupt forges a node file whose
// collected_at carries a non-zero digit past the ninth, with a checksum
// matching the truncated interpretation. History and health must report data
// corruption naming the record and field; a further submit to the node is
// refused as a whole and the file is not overwritten. The trailing-zero
// form on disk stays readable.
func TestCLIStoredUnrepresentableFractionIsCorrupt(t *testing.T) {
	dir := t.TempDir()
	submitBatch(t, dir, "["+rec("bad", 1, "2026-10-01T11:59:59Z")+"]")

	// The forged checksum hashes the record as a truncating reader would
	// decode it (.12345678901 -> .123456789).
	tampered := `[{"node":"bad","seq":1,"collected_at":"2026-10-01T11:59:00.12345678901Z","version":"1.26.0","height":100,"missed":0}]`
	truncated := `[{"node":"bad","seq":1,"collected_at":"2026-10-01T11:59:00.123456789Z","version":"1.26.0","height":100,"missed":0}]`
	corruptNodeFile(t, dir, "bad", envelope(strictChecksum(truncated), tampered))
	corruptBefore := readNodeFile(t, dir, "bad")

	// history: non-zero exit, corruption reason naming record and field, no
	// output on stdout.
	out, errOut, code := historyCLI(t, dir, "bad")
	if code == 0 {
		t.Errorf("history on corrupt node must exit non-zero; stdout=%q", out)
	}
	if out != "" {
		t.Errorf("history must print no results on corruption, stdout=%q", out)
	}
	for _, want := range []string{"corruption", "record 1", "collected_at", ".12345678901"} {
		if !strings.Contains(errOut, want) {
			t.Errorf("history stderr must contain %q, got %q", want, errOut)
		}
	}

	// health: same contract.
	out, errOut, code = runCLI(t, dir, "",
		"heartbeat", "health", "--data-dir", dir, "--at", cliQueryAt,
		"--node", "bad", "--expected-version", "1.26.0", "--tolerated-misses", "0")
	if code == 0 {
		t.Errorf("health on corrupt node must exit non-zero; stdout=%q", out)
	}
	if out != "" {
		t.Errorf("health must print no results on corruption, stdout=%q", out)
	}
	for _, want := range []string{"corruption", "record 1", "collected_at"} {
		if !strings.Contains(errOut, want) {
			t.Errorf("health stderr must contain %q, got %q", want, errOut)
		}
	}

	// A further submit to the corrupt node is refused as a whole and the
	// file stays byte-for-byte intact.
	out, errOut, code = submit(t, dir, "["+rec("bad", 2, "2026-10-01T11:59:59Z")+"]")
	if code == 0 {
		t.Errorf("submit to a corrupt node must exit non-zero; stdout=%q", out)
	}
	if strings.Contains(out, "submitted") {
		t.Errorf("refused submit must not print success counts, stdout=%q", out)
	}
	if !strings.Contains(errOut, "corruption") {
		t.Errorf("submit stderr must report the corruption, got %q", errOut)
	}
	if got := readNodeFile(t, dir, "bad"); got != corruptBefore {
		t.Errorf("corrupt file was overwritten by the refused submit")
	}

	// The trailing-zero form on disk is legal and stays readable.
	corruptNodeFile(t, dir, "bad", envelope(strictChecksum(truncated),
		`[{"node":"bad","seq":1,"collected_at":"2026-10-01T11:59:00.1234567890Z","version":"1.26.0","height":100,"missed":0}]`))
	lines := historyLines(t, dir, "bad")
	if len(lines) != 1 || !strings.Contains(lines[0], "seq=1") {
		t.Errorf("trailing-zero stored fraction must stay readable, got %q", lines)
	}
}
