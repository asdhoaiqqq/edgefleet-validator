package main

// Command-line regression tests for collected_at validation at the real
// `heartbeat submit` entry point.
//
// Before the fix, Go's RFC3339 parser accepted out-of-range numeric offsets
// (+24:00, -24:00) and folded overflowing minutes (+00:60, -23:60) into a
// legal offset. The +24:00 case only failed while serializing the save file,
// so in a multi-node batch the records of other nodes were already written;
// the folded cases were silently saved as a different offset. These tests
// pin the user-visible contract: any such record rejects the whole batch
// before storage, the error names the 1-based record position and the exact
// collected_at problem, stdout carries no success/count line, and no node
// file is created or modified.

import (
	"os"
	"strconv"
	"strings"
	"testing"
)

const goodCollectedAt = "2026-10-01T11:59:00Z"

func goodRecord(node string, seq int) string {
	return `{"node":"` + node + `","seq":` + strconv.Itoa(seq) +
		`,"collected_at":"` + goodCollectedAt + `","version":"1.26.0","height":7,"missed":0}`
}

func badOffsetRecord(node string, seq int, offset string) string {
	return `{"node":"` + node + `","seq":` + strconv.Itoa(seq) +
		`,"collected_at":"2026-10-01T11:59:00` + offset + `","version":"1.26.0","height":7,"missed":0}`
}

func assertNoNodeFiles(t *testing.T, dir string, nodes ...string) {
	t.Helper()
	for _, node := range nodes {
		if _, err := os.Stat(nodeFileHexPath(dir, node)); !os.IsNotExist(err) {
			t.Errorf("no file may exist for node %q after rejected batch, stat err=%v", node, err)
		}
	}
}

// TestCLISubmitBadTimezoneOffsetRejectsBatch covers each rejected offset
// spelling in a two-record batch where the first record is valid. The good
// record must not be saved and the error must locate record 2.
func TestCLISubmitBadTimezoneOffsetRejectsBatch(t *testing.T) {
	cases := []struct {
		name   string
		offset string
	}{
		{"plus 24 hours", "+24:00"},
		{"minus 24 hours", "-24:00"},
		{"minute 60 folded plus", "+00:60"},
		{"minute 60 folded minus", "-23:60"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			batch := "[" + goodRecord("new-a", 1) + "," + badOffsetRecord("new-b", 2, tc.offset) + "]"
			out, errOut, code := submit(t, dir, batch)
			assertRejectedBatch(t, out, errOut, code, "2", "collected_at")
			if code != 1 {
				t.Errorf("rejected submit exit code=%d, want 1", code)
			}
			if !strings.Contains(errOut, tc.offset) {
				t.Errorf("stderr must quote the offending offset %q, got %q", tc.offset, errOut)
			}
			assertNoNodeFiles(t, dir, "new-a", "new-b")
		})
	}
}

// TestCLISubmitBadOffsetSingleRecordPosition pins 1-based positioning for a
// batch consisting of just the bad record: the error says record 1, never
// record 0.
func TestCLISubmitBadOffsetSingleRecordPosition(t *testing.T) {
	dir := t.TempDir()
	out, errOut, code := submit(t, dir, "["+badOffsetRecord("solo", 1, "+24:00")+"]")
	assertRejectedBatch(t, out, errOut, code, "1", "collected_at")
	if strings.Contains(errOut, "record 0") {
		t.Errorf("positions are 1-based; stderr must not say record 0: %q", errOut)
	}
	assertNoNodeFiles(t, dir, "solo")
}

// TestCLISubmitBadOffsetLeavesExistingHistoryAndRejectsDuplicate mirrors the
// real partial-save condition: an existing node with history, a batch that
// both re-submits one of its records (a duplicate) and would create
// telemetry for a new node whose time is invalid. The duplicate changes
// nothing, the existing file is untouched, and the new node gets no file.
// A follow-up healthy submit then reports the usual counts.
func TestCLISubmitBadOffsetLeavesExistingHistoryAndRejectsDuplicate(t *testing.T) {
	dir := t.TempDir()
	submitBatch(t, dir, "["+goodRecord("old", 1)+"]")
	before, err := os.ReadFile(nodeFileHexPath(dir, "old"))
	if err != nil {
		t.Fatal(err)
	}

	batch := "[" + goodRecord("old", 1) + "," + badOffsetRecord("fresh", 1, "+00:60") + "]"
	out, errOut, code := submit(t, dir, batch)
	assertRejectedBatch(t, out, errOut, code, "2", "collected_at")
	if !strings.Contains(errOut, "+00:60") {
		t.Errorf("stderr must quote +00:60, got %q", errOut)
	}

	after, err := os.ReadFile(nodeFileHexPath(dir, "old"))
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Errorf("existing node file changed after rejected batch:\nbefore=%s\nafter=%s", before, after)
	}
	lines := historyLines(t, dir, "old")
	if len(lines) != 1 || !strings.Contains(lines[0], "seq=1") {
		t.Errorf("old history must keep exactly seq 1, got %q", lines)
	}
	assertNoNodeFiles(t, dir, "fresh")

	// The rejected batch leaves no trace: a valid new seq for old counts as
	// one new record with no duplicates.
	out, errOut, code = submit(t, dir, "["+goodRecord("old", 2)+"]")
	if code != 0 {
		t.Fatalf("follow-up submit must succeed: exit=%d stderr=%q", code, errOut)
	}
	if strings.TrimRight(out, "\n") != "submitted: new=1 duplicate=0" {
		t.Errorf("follow-up submit stdout=%q, want new=1 duplicate=0", out)
	}
}

// TestCLISubmitOffsetBoundariesAccepted pins the legal complements through
// the command: the largest-magnitude offsets ±23:59 submit successfully and
// come back through history, while an offset that folds the wall-clock date
// past the receive time is still refused by the receive-time rule.
func TestCLISubmitOffsetBoundariesAccepted(t *testing.T) {
	dir := t.TempDir()
	// 2026-10-01T11:59:00+23:59 == 2026-09-30T12:00:00Z, before receive.
	plus := `[{"node":"edge-p","seq":1,"collected_at":"2026-10-01T11:59:00+23:59","version":"1.26.0","height":7,"missed":0}]`
	out, errOut, code := submit(t, dir, plus)
	if code != 0 {
		t.Fatalf("+23:59 must be accepted: exit=%d stderr=%q", code, errOut)
	}
	if strings.TrimRight(out, "\n") != "submitted: new=1 duplicate=0" {
		t.Errorf("+23:59 submit stdout=%q", out)
	}
	lines := historyLines(t, dir, "edge-p")
	if len(lines) != 1 || !strings.Contains(lines[0], "collected_at=2026-10-01T11:59:00+23:59") {
		t.Errorf("+23:59 must be stored with its original offset, got %q", lines)
	}

	// -23:59 at the same wall-clock date is the next UTC day and therefore
	// later than receive: the existing receive-time rule keeps firing, with
	// its own message rather than an offset error.
	minus := `[{"node":"edge-m","seq":1,"collected_at":"2026-10-01T11:59:00-23:59","version":"1.26.0","height":7,"missed":0}]`
	out, errOut, code = submit(t, t.TempDir(), minus)
	if code == 0 {
		t.Fatalf("future collection time must still be rejected, stdout=%q", out)
	}
	if !strings.Contains(errOut, "later than receive time") {
		t.Errorf("receive-time rejection wording expected, got %q", errOut)
	}
}
