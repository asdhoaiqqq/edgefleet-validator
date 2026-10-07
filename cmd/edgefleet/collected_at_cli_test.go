package main

// Command-line regression tests for collected_at validity at the real
// `heartbeat submit` entry point. The rules themselves are tested in package
// edgefleet; these tests protect what a user observes:
//
//   - an out-of-range numeric offset (+24:00, -24:00, +00:60) fails the whole
//     batch non-zero even though Go's own parser accepts/folds those strings;
//   - stderr names the 1-based record position and collected_at with the
//     offending offset;
//   - stdout carries no success line or counters;
//   - no node file is created and an existing node's file is byte-for-byte
//     unchanged;
//   - the boundary offsets that are legal still submit with the usual counts.

import (
	"os"
	"strconv"
	"strings"
	"testing"
)

func rec(node string, seq int, stamp string) string {
	return `{"node":"` + node + `","seq":` + strconv.Itoa(seq) +
		`,"collected_at":"` + stamp + `","version":"1.26.0","height":1,"missed":0}`
}

// TestCLISubmitOutOfRangeOffsetRejectedAtEachPosition sends the bad record
// first and second; the error must name its real 1-based position.
func TestCLISubmitOutOfRangeOffsetRejectedAtEachPosition(t *testing.T) {
	cases := []struct {
		name   string
		stamp  string
		offset string
	}{
		{"plus 24 hours", "2026-10-01T11:59:00+24:00", "+24:00"},
		{"minus 24 hours", "2026-10-01T11:59:00-24:00", "-24:00"},
		{"60 minutes folds to an hour", "2026-10-01T11:59:00+00:60", "+00:60"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Bad record first: position 1.
			dir := t.TempDir()
			bad := rec("bad", 1, tc.stamp)
			good := rec("good", 1, "2026-10-01T11:59:59Z")
			out, errOut, code := submit(t, dir, "["+bad+","+good+"]")
			assertRejectedBatch(t, out, errOut, code, "1", "collected_at")
			if !strings.Contains(errOut, tc.offset) {
				t.Errorf("stderr must quote the offending offset %q, got %q", tc.offset, errOut)
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
			if !strings.Contains(errOut, tc.offset) {
				t.Errorf("stderr must quote the offending offset %q, got %q", tc.offset, errOut)
			}
			for _, node := range []string{"bad", "good"} {
				if _, err := os.Stat(nodeFileHexPath(dir, node)); !os.IsNotExist(err) {
					t.Errorf("no file may be created for %q after rejection, stat err=%v", node, err)
				}
			}
		})
	}
}

// TestCLISubmitBadOffsetLeavesExistingAndNewNodesUntouched exercises the
// all-or-nothing guarantee across an existing node, a brand-new node and a
// duplicated record in the same batch as the invalid one.
func TestCLISubmitBadOffsetLeavesExistingAndNewNodesUntouched(t *testing.T) {
	dir := t.TempDir()
	submitBatch(t, dir, "["+rec("old", 1, "2026-10-01T11:59:50Z")+"]")
	oldBefore := readNodeFile(t, dir, "old")
	f := &fixture{t: t, dir: dir}

	batch := "[" +
		rec("fresh", 1, "2026-10-01T11:59:58Z") + "," +
		rec("old", 2, "2026-10-01T11:59:58Z") + "," +
		rec("old", 1, "2026-10-01T11:59:50Z") + "," + // duplicate
		rec("bad", 1, "2026-10-01T11:59:00+00:60") + // position 4
		"]"
	out, errOut, code := submit(t, dir, batch)
	assertRejectedBatch(t, out, errOut, code, "4", "collected_at")
	if !strings.Contains(errOut, "+00:60") {
		t.Errorf("stderr must name the +00:60 offset, got %q", errOut)
	}

	// Existing history is byte-for-byte unchanged.
	if got := readNodeFile(t, dir, "old"); got != oldBefore {
		t.Errorf("existing node file was modified during the rejected batch")
	}
	// Neither new node exists.
	for _, node := range []string{"fresh", "bad"} {
		if _, err := os.Stat(nodeFileHexPath(dir, node)); !os.IsNotExist(err) {
			t.Errorf("no file may be created for %q, stat err=%v", node, err)
		}
		histOut, _, histCode := historyCLI(t, dir, node)
		if histCode != 0 || strings.TrimRight(histOut, "\n") != "node="+node+" no heartbeats" {
			t.Errorf("node %q must have no telemetry, code=%d out=%q", node, histCode, histOut)
		}
	}
	// The existing node is still healthy on its original seq.
	hout, _, hcode := health(t, f,
		"--node", "old", "--expected-version", "1.26.0", "--tolerated-misses", "0")
	if hcode != 0 || !strings.Contains(hout, "seq=1") || !strings.Contains(hout, "status=online") {
		t.Errorf("existing node health changed: code=%d out=%q", hcode, hout)
	}

	// Correcting only the offset lets the same batch succeed with the usual
	// counts: old seq2 and fresh seq1 are new, old seq1 is the duplicate.
	fixed := "[" +
		rec("fresh", 1, "2026-10-01T11:59:58Z") + "," +
		rec("old", 2, "2026-10-01T11:59:58Z") + "," +
		rec("old", 1, "2026-10-01T11:59:50Z") + "," +
		rec("bad", 1, "2026-10-01T11:59:00+00:00") +
		"]"
	out, errOut, code = submit(t, dir, fixed)
	if code != 0 {
		t.Fatalf("corrected batch must submit: exit=%d stderr=%q", code, errOut)
	}
	if strings.TrimRight(out, "\n") != "submitted: new=3 duplicate=1" {
		t.Errorf("corrected batch stdout=%q, want new=3 duplicate=1", out)
	}
}

// TestCLISubmitAcceptsBoundaryOffsets pins the legal edge values through the
// real command: the largest magnitude offsets on each side and fractional
// seconds still submit with the normal counts.
func TestCLISubmitAcceptsBoundaryOffsets(t *testing.T) {
	cases := []struct {
		name  string
		stamp string
	}{
		// Wall clocks chosen so each UTC instant is not later than the fixed
		// receive time 2026-10-01T12:00:00Z.
		{"max positive offset", "2026-09-30T12:00:00+23:59"},
		{"max negative offset", "2026-09-30T12:01:00-23:59"},
		{"zulu", "2026-10-01T11:59:59Z"},
		{"nanosecond fraction", "2026-10-01T11:59:59.123456789+08:00"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			out, errOut, code := submit(t, dir, "["+rec("edge", 1, tc.stamp)+"]")
			if code != 0 {
				t.Fatalf("boundary value %q must submit: exit=%d stderr=%q", tc.stamp, code, errOut)
			}
			if strings.TrimRight(out, "\n") != "submitted: new=1 duplicate=0" {
				t.Errorf("stdout=%q, want new=1 duplicate=0", out)
			}
		})
	}
}

// TestCLISubmitRejectsSecondPrecisionOffset documents that offsets carry no
// seconds field: such text is not RFC3339 at this boundary and is refused.
func TestCLISubmitRejectsSecondPrecisionOffset(t *testing.T) {
	dir := t.TempDir()
	out, errOut, code := submit(t, dir, "["+rec("bad", 1, "2026-10-01T11:59:00+08:00:30")+"]")
	assertRejectedBatch(t, out, errOut, code, "1", "collected_at")
	if _, err := os.Stat(nodeFileHexPath(dir, "bad")); !os.IsNotExist(err) {
		t.Errorf("no file may be created, stat err=%v", err)
	}
}

// TestCLISubmitRejectsSubNanosecondFraction is the user-visible side of the
// reported regression: Go's parser silently truncates fractional seconds past
// the ninth digit, so ".1234567891" and ".1234567892" would both save as
// ".123456789" and two different collection moments would merge into one
// record. Any non-zero digit past the ninth must fail the whole batch
// non-zero, with stderr naming the 1-based record position, collected_at and
// the precision problem, and nothing saved — including the legal record in
// the same batch and an existing node's file.
func TestCLISubmitRejectsSubNanosecondFraction(t *testing.T) {
	cases := []struct {
		name  string
		stamp string
	}{
		{"tenth digit non-zero", "2026-10-01T11:59:00.1234567891Z"},
		{"the other value that used to merge", "2026-10-01T11:59:00.1234567892Z"},
		{"eleventh digit non-zero", "2026-10-01T11:59:00.12345678901Z"},
		{"tenth digit non-zero with offset", "2026-10-01T11:59:00.1234567891+08:00"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			submitBatch(t, dir, "["+rec("old", 1, "2026-10-01T11:59:50Z")+"]")
			oldBefore := readNodeFile(t, dir, "old")

			// Bad record second: position 2, and the leading good record must
			// not land either.
			batch := "[" + rec("fresh", 1, "2026-10-01T11:59:58Z") + "," + rec("bad", 1, tc.stamp) + "]"
			out, errOut, code := submit(t, dir, batch)
			assertRejectedBatch(t, out, errOut, code, "2", "collected_at")
			if !strings.Contains(errOut, "fractional seconds") {
				t.Errorf("stderr must explain the fractional precision problem, got %q", errOut)
			}
			if got := readNodeFile(t, dir, "old"); got != oldBefore {
				t.Errorf("existing node file was modified during the rejected batch")
			}
			for _, node := range []string{"fresh", "bad"} {
				if _, err := os.Stat(nodeFileHexPath(dir, node)); !os.IsNotExist(err) {
					t.Errorf("no file may be created for %q, stat err=%v", node, err)
				}
			}
		})
	}
}

// TestCLISubmitTruncatedDuplicateIsNotAccepted pins the exact failure mode:
// the offending record's truncated form would be identical to a record
// already saved for that node and seq, so the old code would have counted it
// as a duplicate. It must instead fail the batch as invalid input.
func TestCLISubmitTruncatedDuplicateIsNotAccepted(t *testing.T) {
	dir := t.TempDir()
	submitBatch(t, dir, "["+rec("n1", 1, "2026-10-01T11:59:00.123456789Z")+"]")
	before := readNodeFile(t, dir, "n1")

	out, errOut, code := submit(t, dir, "["+rec("n1", 1, "2026-10-01T11:59:00.1234567891Z")+"]")
	assertRejectedBatch(t, out, errOut, code, "1", "collected_at")
	if strings.Contains(out, "duplicate") {
		t.Errorf("a record that only matches after truncation must not count as a duplicate, stdout=%q", out)
	}
	if got := readNodeFile(t, dir, "n1"); got != before {
		t.Errorf("saved file changed during the rejected batch")
	}
}

// TestCLISubmitAcceptsTrailingZeroFraction pins the legal long form through
// the real command: ".1234567890" is exactly ".123456789", submits normally,
// and a resubmit of the same instant in the other spelling is a duplicate.
func TestCLISubmitAcceptsTrailingZeroFraction(t *testing.T) {
	dir := t.TempDir()
	out, errOut, code := submit(t, dir, "["+rec("n1", 1, "2026-10-01T11:59:00.1234567890Z")+"]")
	if code != 0 {
		t.Fatalf("trailing-zero fraction must submit: exit=%d stderr=%q", code, errOut)
	}
	if strings.TrimRight(out, "\n") != "submitted: new=1 duplicate=0" {
		t.Errorf("stdout=%q, want new=1 duplicate=0", out)
	}

	// The same instant spelled with nine digits is the same record.
	out, errOut, code = submit(t, dir, "["+rec("n1", 1, "2026-10-01T11:59:00.123456789Z")+"]")
	if code != 0 {
		t.Fatalf("equivalent nine-digit spelling must submit: exit=%d stderr=%q", code, errOut)
	}
	if strings.TrimRight(out, "\n") != "submitted: new=0 duplicate=1" {
		t.Errorf("stdout=%q, want new=0 duplicate=1", out)
	}

	// Exactly one record is held; the terminal keeps its existing display
	// format (the instant itself is verified at the package level).
	lines := historyLines(t, dir, "n1")
	if len(lines) != 1 || !strings.Contains(lines[0], "node=n1 seq=1 ") {
		t.Errorf("history must hold the one exact record, got %q", lines)
	}
}

// TestCLIStoredSubNanosecondFractionIsCorrupt forges a node file whose raw
// JSON carries a fraction time.Parse would truncate, with a checksum matching
// the truncated interpretation. History and health must report corruption
// naming the record and field, and a submit touching the node must refuse
// the whole batch without overwriting the file.
func TestCLIStoredSubNanosecondFractionIsCorrupt(t *testing.T) {
	dir := t.TempDir()
	f := &fixture{t: t, dir: dir}

	// A genuine saved record, then replaced by the tampered file.
	submitBatch(t, dir, "["+rec("n1", 1, "2026-10-01T11:59:00.123456789Z")+"]")
	const tamper = `[{"node":"n1","seq":1,"collected_at":"2026-10-01T11:59:00.1234567891Z","version":"1.26.0","height":100,"missed":0}]`
	// The forged checksum hashes the truncated record a lenient reader
	// reconstructs — the strongest form of the attack.
	const interpreted = `[{"node":"n1","seq":1,"collected_at":"2026-10-01T11:59:00.123456789Z","version":"1.26.0","height":100,"missed":0}]`
	corruptNodeFile(t, dir, "n1", envelope(strictChecksum(interpreted), tamper))
	before := readNodeFile(t, dir, "n1")

	out, errOut, code := historyCLI(t, dir, "n1")
	if code == 0 || out != "" {
		t.Errorf("history must refuse the corrupt file: code=%d stdout=%q", code, out)
	}
	for _, want := range []string{"record 1", "collected_at", "fractional seconds"} {
		if !strings.Contains(errOut, want) {
			t.Errorf("history stderr must mention %q, got %q", want, errOut)
		}
	}

	out, errOut, code = health(t, f, "--node", "n1", "--expected-version", "1.26.0", "--tolerated-misses", "0")
	if code == 0 || out != "" {
		t.Errorf("health must refuse the corrupt file: code=%d stdout=%q", code, out)
	}
	if !strings.Contains(errOut, "collected_at") {
		t.Errorf("health stderr must name collected_at, got %q", errOut)
	}

	// A submit batch touching the corrupt node is refused as a whole; the
	// corrupt file and a brand-new node's absence are both preserved.
	batch := "[" + rec("n1", 2, "2026-10-01T11:59:59Z") + "," + rec("brandnew", 1, "2026-10-01T11:59:59Z") + "]"
	out, _, code = submit(t, dir, batch)
	if code == 0 {
		t.Errorf("submit touching the corrupt node must fail non-zero; stdout=%q", out)
	}
	if strings.Contains(out, "submitted") {
		t.Errorf("rejected batch must not print success counts, stdout=%q", out)
	}
	if got := readNodeFile(t, dir, "n1"); got != before {
		t.Errorf("corrupt file was overwritten during the refused submit")
	}
	if _, err := os.Stat(nodeFileHexPath(dir, "brandnew")); !os.IsNotExist(err) {
		t.Errorf("brand-new node file must not be created, stat err=%v", err)
	}
}
