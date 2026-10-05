package main

// Command-line regression tests for `edgefleet heartbeat submit` input
// validation: every heartbeat in a batch must be complete and unambiguous,
// and a single bad record rejects the whole batch.
//
// The field rules themselves (missing, null, duplicated, value ranges) are
// covered by unit tests in package edgefleet (ParseHeartbeats). These tests
// protect what a user observes at the real command entry point after piping
// a JSON array to stdin:
//
//   - the process exit status (invalid input fails non-zero),
//   - stderr naming the 1-based record position and the offending field,
//   - stdout never showing success counts for a rejected batch,
//   - and the stored data: nothing from a rejected batch is saved — not even
//     the valid records that came before the bad one — while explicit zero
//     values for height/missed are genuine telemetry and stay queryable.

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// cliFullRecord is a complete, valid heartbeat object for node "ok"; the
// broken records below are derived from it by deleting or nulling one field.
const cliFullRecord = `{"node":"ok","seq":1,"collected_at":"2026-10-01T11:59:59Z","version":"1.26.0","height":100,"missed":0}`

// assertNoTelemetrySaved checks the observable aftermath of a rejected batch
// in a fresh data directory: the valid record's node has no history, and no
// node file was created at all.
func assertNoTelemetrySaved(t *testing.T, dir, node string) {
	t.Helper()
	out, errOut, code := historyCLI(t, dir, node)
	if code != 0 {
		t.Fatalf("history query after rejected batch must itself work: exit=%d stderr=%q", code, errOut)
	}
	if strings.TrimRight(out, "\n") != "node="+node+" no heartbeats" {
		t.Errorf("valid record from a rejected batch must not be saved, history=%q", out)
	}
	files, err := os.ReadDir(filepath.Join(dir, "nodes"))
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	if len(files) != 0 {
		t.Errorf("no node file may be created by a rejected batch, found %d", len(files))
	}
}

// TestCLISubmitRejectsMissingOrNullField drives every required field through
// the real submit command twice: once absent from the object, once written as
// null. Both must fail the same way — a non-zero exit, stderr naming the
// 1-based record position and the field, no success count on stdout, and the
// valid record earlier in the same batch not saved either.
func TestCLISubmitRejectsMissingOrNullField(t *testing.T) {
	cases := []struct {
		name  string
		bad   string // the broken record, placed at array position 2
		field string
	}{
		{"missing node", `{"seq":1,"collected_at":"2026-10-01T11:59:59Z","version":"1.26.0","height":100,"missed":0}`, "node"},
		{"missing seq", `{"node":"bad","collected_at":"2026-10-01T11:59:59Z","version":"1.26.0","height":100,"missed":0}`, "seq"},
		{"missing collected_at", `{"node":"bad","seq":1,"version":"1.26.0","height":100,"missed":0}`, "collected_at"},
		{"missing version", `{"node":"bad","seq":1,"collected_at":"2026-10-01T11:59:59Z","height":100,"missed":0}`, "version"},
		{"missing height", `{"node":"bad","seq":1,"collected_at":"2026-10-01T11:59:59Z","version":"1.26.0","missed":0}`, "height"},
		{"missing missed", `{"node":"bad","seq":1,"collected_at":"2026-10-01T11:59:59Z","version":"1.26.0","height":100}`, "missed"},
		{"null node", `{"node":null,"seq":1,"collected_at":"2026-10-01T11:59:59Z","version":"1.26.0","height":100,"missed":0}`, "node"},
		{"null seq", `{"node":"bad","seq":null,"collected_at":"2026-10-01T11:59:59Z","version":"1.26.0","height":100,"missed":0}`, "seq"},
		{"null collected_at", `{"node":"bad","seq":1,"collected_at":null,"version":"1.26.0","height":100,"missed":0}`, "collected_at"},
		{"null version", `{"node":"bad","seq":1,"collected_at":"2026-10-01T11:59:59Z","version":null,"height":100,"missed":0}`, "version"},
		{"null height", `{"node":"bad","seq":1,"collected_at":"2026-10-01T11:59:59Z","version":"1.26.0","height":null,"missed":0}`, "height"},
		{"null missed", `{"node":"bad","seq":1,"collected_at":"2026-10-01T11:59:59Z","version":"1.26.0","height":100,"missed":null}`, "missed"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			// A valid record sits at position 1; the broken one at position 2,
			// so the reported position proves the count is 1-based and the
			// valid record proves the batch is all-or-nothing.
			out, errOut, code := submit(t, dir, "["+cliFullRecord+","+tc.bad+"]")
			if code == 0 {
				t.Errorf("%s must exit non-zero; stdout=%q", tc.name, out)
			}
			if strings.Contains(out, "submitted") || strings.Contains(out, "new=") {
				t.Errorf("rejected batch must not print success counts, stdout=%q", out)
			}
			for _, want := range []string{"error:", "record 2", tc.field} {
				if !strings.Contains(errOut, want) {
					t.Errorf("stderr %q must contain %q", errOut, want)
				}
			}
			assertNoTelemetrySaved(t, dir, "ok")
		})
	}
}

// TestCLISubmitExplicitZeroHeightAndMissed pins the other side of the rule:
// height and missed written explicitly as 0 are genuine reports, not missing
// data. The record is saved, both zeros survive into history and health
// output, and a verbatim resubmit deduplicates against the stored zeros —
// proof the stored 0 is a real value, not a defaulted one.
func TestCLISubmitExplicitZeroHeightAndMissed(t *testing.T) {
	dir := t.TempDir()
	f := &fixture{t: t, dir: dir}

	const record = `{"node":"zero","seq":1,"collected_at":"2026-10-01T11:59:59Z","version":"1.26.0","height":0,"missed":0}`
	out, errOut, code := submit(t, dir, "["+record+"]")
	if code != 0 {
		t.Fatalf("explicit zeros must be accepted: exit=%d stderr=%q", code, errOut)
	}
	if errOut != "" {
		t.Errorf("a successful submit must leave stderr empty, got %q", errOut)
	}
	if strings.TrimRight(out, "\n") != "submitted: new=1 duplicate=0" {
		t.Errorf("stdout = %q, want submitted: new=1 duplicate=0", out)
	}

	lines := historyLines(t, dir, "zero")
	want := "node=zero seq=1 collected_at=2026-10-01T11:59:59Z version=1.26.0 height=0 missed=0"
	if len(lines) != 1 || lines[0] != want {
		t.Fatalf("history must show the stored zeros, got %q", lines)
	}

	out, _, code = health(t, f, "--node", "zero", "--expected-version", "1.26.0", "--tolerated-misses", "0")
	if code != 0 {
		t.Fatalf("health on a zero-valued record must succeed, exit=%d", code)
	}
	if !strings.Contains(out, "height=0") || !strings.Contains(out, "missed=0") {
		t.Errorf("health must show the stored zero values: %q", out)
	}
	if strings.Contains(out, "above tolerance") {
		t.Errorf("missed=0 with tolerance 0 must not alarm: %q", out)
	}

	// Resubmitting the identical record is a duplicate: the stored zeros
	// matched the incoming zeros field for field.
	out, _, code = submit(t, dir, "["+record+"]")
	if code != 0 || strings.TrimRight(out, "\n") != "submitted: new=0 duplicate=1" {
		t.Errorf("verbatim resubmit must dedup against stored zeros: exit=%d stdout=%q", code, out)
	}
}

// TestCLISubmitRejectsDuplicateFieldInOneRecord covers the field-uniqueness
// rule at the command line: within one heartbeat object a field may appear
// only once. Keeping the first or the last occurrence is not allowed, even
// when both values are identical, and a Unicode-escaped spelling of the same
// name is still the same field. The whole batch is refused and nothing is
// saved.
func TestCLISubmitRejectsDuplicateFieldInOneRecord(t *testing.T) {
	cases := []struct {
		name  string
		bad   string // the broken record, placed at array position 2
		field string
	}{
		{"same value twice", `{"node":"bad","seq":1,"collected_at":"2026-10-01T11:59:59Z","version":"1.26.0","height":100,"missed":0,"missed":0}`, "missed"},
		{"different values", `{"node":"bad","seq":1,"collected_at":"2026-10-01T11:59:59Z","version":"1.26.0","height":100,"missed":0,"missed":1}`, "missed"},
		{"unicode escape spelling", `{"node":"bad","seq":1,"collected_at":"2026-10-01T11:59:59Z","version":"1.26.0","height":100,"missed":0,"m\u0069ssed":0}`, "missed"},
		{"node twice same value", `{"node":"bad","node":"bad","seq":1,"collected_at":"2026-10-01T11:59:59Z","version":"1.26.0","height":100,"missed":0}`, "node"},
		{"seq twice", `{"node":"bad","seq":1,"seq":1,"collected_at":"2026-10-01T11:59:59Z","version":"1.26.0","height":100,"missed":0}`, "seq"},
		{"collected_at twice", `{"node":"bad","seq":1,"collected_at":"2026-10-01T11:59:59Z","collected_at":"2026-10-01T11:59:59Z","version":"1.26.0","height":100,"missed":0}`, "collected_at"},
		{"version twice", `{"node":"bad","seq":1,"collected_at":"2026-10-01T11:59:59Z","version":"1.26.0","version":"1.26.0","height":100,"missed":0}`, "version"},
		{"height twice", `{"node":"bad","seq":1,"collected_at":"2026-10-01T11:59:59Z","version":"1.26.0","height":100,"height":100,"missed":0}`, "height"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			out, errOut, code := submit(t, dir, "["+cliFullRecord+","+tc.bad+"]")
			if code == 0 {
				t.Errorf("%s must exit non-zero; stdout=%q", tc.name, out)
			}
			if strings.Contains(out, "submitted") || strings.Contains(out, "new=") {
				t.Errorf("rejected batch must not print success counts, stdout=%q", out)
			}
			for _, want := range []string{"error:", "record 2", "duplicate", tc.field} {
				if !strings.Contains(errOut, want) {
					t.Errorf("stderr %q must contain %q", errOut, want)
				}
			}
			assertNoTelemetrySaved(t, dir, "ok")
		})
	}
}

// TestCLISubmitFieldOrderWhitespaceAndEscapeVariantsAccepted pins the legal
// inputs that must NOT be misread as duplicates or unknown fields: every
// heartbeat object in a batch naturally repeats the same field names (the
// uniqueness rule is scoped to one object), fields may arrive in any order
// with legal whitespace, and a field name written with a Unicode escape is
// the same field, accepted once and stored under its decoded name.
func TestCLISubmitFieldOrderWhitespaceAndEscapeVariantsAccepted(t *testing.T) {
	dir := t.TempDir()

	// Two objects, same field names each — normal input. The first shuffles
	// the field order, adds legal whitespace and writes "version" and "seq"
	// with escapes; the second writes "node" and "collected_at" escaped.
	batch := `[
	  {
	    "missed" : 3,
	    "height" : 0,
	    "\u0076ersion": "1.26.0",
	    "collected_at": "2026-10-01T11:59:59Z",
	    "\u0073eq": 2,
	    "node": "n1"
	  },
	  {"\u006eode":"n1","seq":1,"\u0063ollected_at":"2026-10-01T11:59:58Z","version":"1.26.0","height":0,"missed":0}
	]`
	out, errOut, code := submit(t, dir, batch)
	if code != 0 {
		t.Fatalf("legal field order/whitespace/escapes must be accepted: exit=%d stderr=%q", code, errOut)
	}
	if strings.TrimRight(out, "\n") != "submitted: new=2 duplicate=0" {
		t.Errorf("stdout = %q, want submitted: new=2 duplicate=0", out)
	}

	// Both records are stored under the decoded field names, ascending by seq.
	lines := historyLines(t, dir, "n1")
	if len(lines) != 2 {
		t.Fatalf("history = %q, want exactly two records", lines)
	}
	want1 := "node=n1 seq=1 collected_at=2026-10-01T11:59:58Z version=1.26.0 height=0 missed=0"
	want2 := "node=n1 seq=2 collected_at=2026-10-01T11:59:59Z version=1.26.0 height=0 missed=3"
	if lines[0] != want1 {
		t.Errorf("first line = %q, want %q", lines[0], want1)
	}
	if lines[1] != want2 {
		t.Errorf("second line = %q, want %q", lines[1], want2)
	}
}

// TestCLISubmitBadRecordAfterValidOnesRejectsWholeBatch is the end-to-end
// atomicity contract: a batch mixes a valid new seq for an existing node, a
// valid first heartbeat for a node with no telemetry yet, and — placed LAST —
// a record with a null field. The whole batch fails; afterwards the existing
// node's file and health answer are exactly as before, the first-time node
// still has no telemetry, and nothing was partially saved. Fixing the bad
// field and resubmitting the same batch then lands every record under the
// existing seq rules.
func TestCLISubmitBadRecordAfterValidOnesRejectsWholeBatch(t *testing.T) {
	dir := t.TempDir()
	f := &fixture{t: t, dir: dir}

	// Existing node with one saved heartbeat.
	submitBatch(t, dir, `[
	  {"node":"old","seq":1,"collected_at":"2026-10-01T11:59:50Z","version":"1.26.0","height":50,"missed":1}
	]`)
	oldFileBefore := readNodeFile(t, dir, "old")
	healthArgs := []string{"--node", "old", "--expected-version", "1.26.0", "--tolerated-misses", "9"}
	oldHealthBefore, _, code := health(t, f, healthArgs...)
	if code != 0 {
		t.Fatalf("health on the existing node must work before the test, exit=%d", code)
	}

	// Valid new seq for the existing node, valid first heartbeat for a new
	// node, then the bad record (null height) at position 3.
	batch := `[
	  {"node":"old","seq":2,"collected_at":"2026-10-01T11:59:59Z","version":"1.26.0","height":51,"missed":1},
	  {"node":"new","seq":1,"collected_at":"2026-10-01T11:59:58Z","version":"1.26.0","height":1,"missed":0},
	  {"node":"bad","seq":1,"collected_at":"2026-10-01T11:59:57Z","version":"1.26.0","height":null,"missed":0}
	]`
	out, errOut, code := submit(t, dir, batch)
	if code == 0 {
		t.Fatalf("a batch with a null field must fail non-zero; stdout=%q", out)
	}
	if strings.Contains(out, "submitted") || strings.Contains(out, "new=") {
		t.Errorf("rejected batch must not print success counts, stdout=%q", out)
	}
	for _, want := range []string{"error:", "record 3", "height"} {
		if !strings.Contains(errOut, want) {
			t.Errorf("stderr %q must contain %q", errOut, want)
		}
	}

	// The existing node is byte-for-byte untouched, and the same health query
	// at the same query time gives the identical answer.
	if got := readNodeFile(t, dir, "old"); got != oldFileBefore {
		t.Errorf("existing node file was modified by the rejected batch")
	}
	oldHealthAfter, _, code := health(t, f, healthArgs...)
	if code != 0 {
		t.Fatalf("health on the existing node must still work, exit=%d", code)
	}
	if oldHealthAfter != oldHealthBefore {
		t.Errorf("health answer changed after rejected batch:\nbefore=%q\nafter =%q", oldHealthBefore, oldHealthAfter)
	}
	if lines := historyLines(t, dir, "old"); len(lines) != 1 || !strings.Contains(lines[0], "seq=1") {
		t.Errorf("existing node history changed after rejected batch: %q", lines)
	}

	// The first-time node still has no telemetry: history is empty, health
	// reports 无遥测, and no file exists for it or the bad record's node.
	out, _, code = health(t, f, "--node", "new", "--expected-version", "1.26.0", "--tolerated-misses", "0")
	if code != 0 || !strings.Contains(out, "status=无遥测") {
		t.Errorf("first-time node must still report 无遥测: exit=%d out=%q", code, out)
	}
	out, _, code = historyCLI(t, dir, "new")
	if code != 0 || strings.TrimRight(out, "\n") != "node=new no heartbeats" {
		t.Errorf("first-time node must have no history: exit=%d out=%q", code, out)
	}
	for _, node := range []string{"new", "bad"} {
		if _, err := os.Stat(nodeFileHexPath(dir, node)); !os.IsNotExist(err) {
			t.Errorf("no file may be created for node %q, stat err=%v", node, err)
		}
	}

	// Fix the bad field and resubmit the same batch: all three records land.
	fixed := strings.Replace(batch, `"height":null`, `"height":60`, 1)
	out, errOut, code = submit(t, dir, fixed)
	if code != 0 {
		t.Fatalf("corrected batch must succeed: exit=%d stderr=%q", code, errOut)
	}
	if strings.TrimRight(out, "\n") != "submitted: new=3 duplicate=0" {
		t.Errorf("corrected batch stdout = %q, want submitted: new=3 duplicate=0", out)
	}

	// The existing node keeps its original seq 1 and gains seq 2, ascending.
	old := historyLines(t, dir, "old")
	if len(old) != 2 {
		t.Fatalf("old history = %q, want seq 1 and 2", old)
	}
	if old[0] != "node=old seq=1 collected_at=2026-10-01T11:59:50Z version=1.26.0 height=50 missed=1" {
		t.Errorf("original old seq 1 telemetry altered: %q", old[0])
	}
	if old[1] != "node=old seq=2 collected_at=2026-10-01T11:59:59Z version=1.26.0 height=51 missed=1" {
		t.Errorf("new old seq 2 line = %q", old[1])
	}
	// The first-time node now has exactly its own first heartbeat, and the
	// corrected record saved the fixed height.
	if lines := historyLines(t, dir, "new"); len(lines) != 1 ||
		lines[0] != "node=new seq=1 collected_at=2026-10-01T11:59:58Z version=1.26.0 height=1 missed=0" {
		t.Errorf("new node history = %q", lines)
	}
	if lines := historyLines(t, dir, "bad"); len(lines) != 1 ||
		lines[0] != "node=bad seq=1 collected_at=2026-10-01T11:59:57Z version=1.26.0 height=60 missed=0" {
		t.Errorf("corrected node history = %q", lines)
	}
	// Health for the existing node now reflects the new latest seq.
	out, _, code = health(t, f, healthArgs...)
	if code != 0 || !strings.Contains(out, "seq=2") || !strings.Contains(out, "height=51") {
		t.Errorf("health after corrected submit must show the new latest record: exit=%d out=%q", code, out)
	}
}
