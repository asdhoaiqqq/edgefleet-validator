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
