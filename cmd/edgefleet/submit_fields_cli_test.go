package main

// Command-line regression tests for strict heartbeat-field integrity at the
// real `heartbeat submit` entry point (JSON array on stdin).
//
// The field rules themselves (required fields, null rejection, duplicate
// keys, explicit-zero handling) have focused tests in package edgefleet.
// These tests extend that guarantee to what a user actually observes when
// submitting through the command:
//
//   - exit status (non-zero for any malformed record in the batch),
//   - stderr placement and content (the 1-based array position and the
//     offending field),
//   - stdout (no success count may appear for a rejected batch),
//   - and the queryable data afterwards (history and health).
//
// The all-or-nothing rule is exercised under the mixed condition that
// matters in practice: a batch that appends a new seq to an existing node
// AND creates telemetry for a node that had none must still fail as a whole
// when one later record is invalid, leaving both nodes exactly as they were.

import (
	"os"
	"strings"
	"testing"
)

// assertRejectedBatch is the shared user-visible contract for a refused
// submit: non-zero exit, the record position and field on stderr, and no
// success count anywhere on stdout.
func assertRejectedBatch(t *testing.T, out, errOut string, code int, position, field string) {
	t.Helper()
	if code == 0 {
		t.Errorf("submit must exit non-zero for bad record %s; stdout=%q stderr=%q", position, out, errOut)
	}
	for _, want := range []string{"error:", "record " + position, field} {
		if !strings.Contains(errOut, want) {
			t.Errorf("stderr %q must contain %q", errOut, want)
		}
	}
	if strings.Contains(out, "submitted") || strings.Contains(out, "new=") {
		t.Errorf("rejected batch must print no success count on stdout, got %q", out)
	}
}

// TestCLISubmitMissingFieldRejectedNamesPositionAndField removes each
// required field in turn. Every omission must fail at the command boundary:
// non-zero exit, stderr naming the 1-based position inside the JSON array and
// the exact missing field, no success count on stdout. The position is
// checked independently by placing the bad record second, after a good one.
func TestCLISubmitMissingFieldRejectedNamesPositionAndField(t *testing.T) {
	const good = `{"node":"ok","seq":1,"collected_at":"2026-10-01T11:59:59Z","version":"1.26.0","height":7,"missed":0}`
	cases := []struct {
		name  string
		bad   string
		field string
	}{
		{"missing node",
			`{"seq":2,"collected_at":"2026-10-01T11:59:59Z","version":"1.26.0","height":8,"missed":0}`, "node"},
		{"missing seq",
			`{"node":"ok","collected_at":"2026-10-01T11:59:59Z","version":"1.26.0","height":8,"missed":0}`, "seq"},
		{"missing collected_at",
			`{"node":"ok","seq":2,"version":"1.26.0","height":8,"missed":0}`, "collected_at"},
		{"missing version",
			`{"node":"ok","seq":2,"collected_at":"2026-10-01T11:59:59Z","height":8,"missed":0}`, "version"},
		{"missing height",
			`{"node":"ok","seq":2,"collected_at":"2026-10-01T11:59:59Z","version":"1.26.0","missed":0}`, "height"},
		{"missing missed",
			`{"node":"ok","seq":2,"collected_at":"2026-10-01T11:59:59Z","version":"1.26.0","height":8}`, "missed"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			out, errOut, code := submit(t, dir, "["+good+","+tc.bad+"]")
			assertRejectedBatch(t, out, errOut, code, "2", tc.field)

			// Nothing landed: the good record before the bad one is not saved.
			if _, err := os.Stat(nodeFileHexPath(dir, "ok")); !os.IsNotExist(err) {
				t.Errorf("no node file may exist after rejected batch, stat err=%v", err)
			}
		})
	}

	// Position 1 in a single-record batch is reported from 1, not 0.
	dir := t.TempDir()
	out, errOut, code := submit(t, dir,
		`[{"seq":1,"collected_at":"2026-10-01T11:59:59Z","version":"1.26.0","height":1,"missed":0}]`)
	assertRejectedBatch(t, out, errOut, code, "1", "node")
	if strings.Contains(errOut, "record 0") {
		t.Errorf("positions are 1-based; stderr must not say record 0: %q", errOut)
	}
}

// TestCLISubmitNullFieldRejectedNamesPositionAndField writes every required
// field as explicit null in turn. null means "not reported" and must be
// rejected exactly like omission — never silently saved as the zero value.
func TestCLISubmitNullFieldRejectedNamesPositionAndField(t *testing.T) {
	const good = `{"node":"ok","seq":1,"collected_at":"2026-10-01T11:59:59Z","version":"1.26.0","height":7,"missed":0}`
	cases := []struct {
		name  string
		bad   string
		field string
	}{
		{"null node",
			`{"node":null,"seq":2,"collected_at":"2026-10-01T11:59:59Z","version":"1.26.0","height":8,"missed":0}`, "node"},
		{"null seq",
			`{"node":"ok","seq":null,"collected_at":"2026-10-01T11:59:59Z","version":"1.26.0","height":8,"missed":0}`, "seq"},
		{"null collected_at",
			`{"node":"ok","seq":2,"collected_at":null,"version":"1.26.0","height":8,"missed":0}`, "collected_at"},
		{"null version",
			`{"node":"ok","seq":2,"collected_at":"2026-10-01T11:59:59Z","version":null,"height":8,"missed":0}`, "version"},
		{"null height",
			`{"node":"ok","seq":2,"collected_at":"2026-10-01T11:59:59Z","version":"1.26.0","height":null,"missed":0}`, "height"},
		{"null missed",
			`{"node":"ok","seq":2,"collected_at":"2026-10-01T11:59:59Z","version":"1.26.0","height":8,"missed":null}`, "missed"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			out, errOut, code := submit(t, dir, "["+good+","+tc.bad+"]")
			assertRejectedBatch(t, out, errOut, code, "2", tc.field)
			if !strings.Contains(strings.ToLower(errOut), "null") {
				t.Errorf("stderr should explain the null value for %s, got %q", tc.field, errOut)
			}
			if _, err := os.Stat(nodeFileHexPath(dir, "ok")); !os.IsNotExist(err) {
				t.Errorf("good record before the null record must not be saved, stat err=%v", err)
			}
		})
	}
}

// TestCLISubmitExplicitZeroHeightAndMissedAreGenuine pins the distinction
// the null cases rely on: height 0 and missed 0 written explicitly are real
// reported values. They submit successfully and come back verbatim through
// history and health, alongside records carrying positive values.
func TestCLISubmitExplicitZeroHeightAndMissedAreGenuine(t *testing.T) {
	dir := t.TempDir()

	batch := `[
	  {"node":"zero","seq":1,"collected_at":"2026-10-01T11:59:58Z","version":"1.26.0","height":0,"missed":0},
	  {"node":"zero","seq":2,"collected_at":"2026-10-01T11:59:59Z","version":"1.26.0","height":0,"missed":3}
	]`
	out, errOut, code := submit(t, dir, batch)
	if code != 0 {
		t.Fatalf("explicit zero height/missed must submit: exit=%d stderr=%q", code, errOut)
	}
	if strings.TrimRight(out, "\n") != "submitted: new=2 duplicate=0" {
		t.Errorf("explicit-zero records must count as new, stdout=%q", out)
	}

	// History shows the literal zeros; they were not confused with missing.
	lines := historyLines(t, dir, "zero")
	if len(lines) != 2 {
		t.Fatalf("history = %q, want two records", lines)
	}
	if !strings.Contains(lines[0], "height=0 missed=0") {
		t.Errorf("first history line must keep the zero values: %q", lines[0])
	}
	if !strings.Contains(lines[1], "height=0 missed=3") {
		t.Errorf("second history line mismatch: %q", lines[1])
	}

	// Health (latest seq 2) reports the explicit zero height.
	f := &fixture{t: t, dir: dir}
	out, _, code = health(t, f,
		"--node", "zero", "--expected-version", "1.26.0", "--tolerated-misses", "3")
	if code != 0 {
		t.Fatalf("health on zero-height node must succeed, exit=%d", code)
	}
	if !strings.Contains(out, "status=online") || !strings.Contains(out, "height=0") ||
		!strings.Contains(out, "missed=3") {
		t.Errorf("health output must show the real zero height: %q", out)
	}
}

// TestCLISubmitDuplicateKeyWithinObjectRejected covers the uniqueness rule
// for one heartbeat object: a field appearing twice is refused even when
// both values agree, and names are compared after JSON decoding so a Unicode
// escape spelling of the same key still collides. It also pins the legal
// complements: same field names across different objects, reordered keys,
// insignificant whitespace, and a key written once as a Unicode escape.
func TestCLISubmitDuplicateKeyWithinObjectRejected(t *testing.T) {
	// bs lets a Go raw string carry a literal backslash for a JSON \u escape.
	bs := "\\"
	cases := []struct {
		name  string
		bad   string
		field string
	}{
		{"duplicate missed same value",
			`{"node":"dup","seq":1,"collected_at":"2026-10-01T11:59:59Z","version":"1.26.0","height":1,"missed":0,"missed":0}`,
			"missed"},
		{"duplicate missed different value",
			`{"node":"dup","seq":1,"collected_at":"2026-10-01T11:59:59Z","version":"1.26.0","height":1,"missed":0,"missed":1}`,
			"missed"},
		{"duplicate height",
			`{"node":"dup","seq":1,"collected_at":"2026-10-01T11:59:59Z","version":"1.26.0","height":1,"height":2,"missed":0}`,
			"height"},
		{"duplicate node",
			`{"node":"dup","node":"other","seq":1,"collected_at":"2026-10-01T11:59:59Z","version":"1.26.0","height":1,"missed":0}`,
			"node"},
		{"duplicate seq",
			`{"node":"dup","seq":1,"seq":2,"collected_at":"2026-10-01T11:59:59Z","version":"1.26.0","height":1,"missed":0}`,
			"seq"},
		{"duplicate collected_at",
			`{"node":"dup","seq":1,"collected_at":"2026-10-01T11:59:59Z","collected_at":"2026-10-01T11:59:58Z","version":"1.26.0","height":1,"missed":0}`,
			"collected_at"},
		{"duplicate version",
			`{"node":"dup","seq":1,"collected_at":"2026-10-01T11:59:59Z","version":"1.26.0","version":"2.0","height":1,"missed":0}`,
			"version"},
		// "missed" decodes to "missed": an escaped repeat is a repeat,
		// in either order and even with identical values.
		{"duplicate missed via unicode escape",
			`{"node":"dup","seq":1,"collected_at":"2026-10-01T11:59:59Z","version":"1.26.0","height":1,"missed":0,"m` + bs + `u0069ssed":0}`,
			"missed"},
		{"duplicate missed via unicode escape reversed",
			`{"node":"dup","seq":1,"collected_at":"2026-10-01T11:59:59Z","version":"1.26.0","height":1,"m` + bs + `u0069ssed":0,"missed":1}`,
			"missed"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			// A good record placed before the duplicated-key record must not
			// survive the rejected batch.
			good := `{"node":"good","seq":1,"collected_at":"2026-10-01T11:59:59Z","version":"1.26.0","height":1,"missed":0}`
			out, errOut, code := submit(t, dir, "["+good+","+tc.bad+"]")
			assertRejectedBatch(t, out, errOut, code, "2", tc.field)
			if !strings.Contains(strings.ToLower(errOut), "duplicate") {
				t.Errorf("stderr should call out the duplicate key, got %q", errOut)
			}
			for _, node := range []string{"good", "dup"} {
				if _, err := os.Stat(nodeFileHexPath(dir, node)); !os.IsNotExist(err) {
					t.Errorf("no file may be created for %q after rejected batch, stat err=%v", node, err)
				}
			}
		})
	}

	// Legal complements, one fresh directory per case.
	t.Run("same field names in different objects are normal", func(t *testing.T) {
		dir := t.TempDir()
		out, errOut, code := submit(t, dir, `[
		  {"node":"a","seq":1,"collected_at":"2026-10-01T11:59:59Z","version":"1.26.0","height":1,"missed":0},
		  {"node":"b","seq":1,"collected_at":"2026-10-01T11:59:59Z","version":"1.26.0","height":2,"missed":0}
		]`)
		if code != 0 || !strings.Contains(out, "new=2") {
			t.Fatalf("two objects sharing field names must submit: code=%d out=%q err=%q", code, out, errOut)
		}
	})

	t.Run("reordered keys whitespace and single escaped key are accepted", func(t *testing.T) {
		dir := t.TempDir()
		// Keys in a different order, with whitespace around punctuation, and
		// "node" written once via a Unicode escape: complete and unique.
		input := "[\n\t{\n" +
			`  "missed" : 0 ,` + "\n" +
			`  "height" : 42 ,` + "\n" +
			`  "version" : "1.26.0" ,` + "\n" +
			`  "collected_at" : "2026-10-01T11:59:59Z" ,` + "\n" +
			`  "seq" : 1 ,` + "\n" +
			`  "n` + bs + `u006fde" : "weird"` + "\n" +
			"}\n]"
		out, errOut, code := submit(t, dir, input)
		if code != 0 {
			t.Fatalf("reordered/whitespace/escaped complete record must submit: stderr=%q", errOut)
		}
		if strings.TrimRight(out, "\n") != "submitted: new=1 duplicate=0" {
			t.Errorf("stdout=%q, want one new record", out)
		}
		// The escaped key decoded to node, so the record is found under "weird".
		lines := historyLines(t, dir, "weird")
		if len(lines) != 1 || !strings.Contains(lines[0], "node=weird seq=1") ||
			!strings.Contains(lines[0], "height=42 missed=0") {
			t.Errorf("history for escaped-key record = %q", lines)
		}
	})
}

// TestCLISubmitStrictFieldFailureRollsBackExistingAndNewNodes exercises the
// all-or-nothing guarantee under the mixed real-world condition: one valid
// record appends a new seq to a node that already has telemetry, another
// valid record is the very first heartbeat for a node that had none, and a
// later record is invalid. The whole batch must fail — no partial save of
// the leading valid records — and after correcting the bad field the same
// valid records submit and query under the usual seq rules.
func TestCLISubmitStrictFieldFailureRollsBackExistingAndNewNodes(t *testing.T) {
	dir := t.TempDir()

	// "existing" already has seq 1; "fresh" has no telemetry at all.
	submitBatch(t, dir, `[
	  {"node":"existing","seq":1,"collected_at":"2026-10-01T11:59:50Z","version":"1.26.0","height":10,"missed":0}
	]`)
	existingBefore := readNodeFile(t, dir, "existing")
	f := &fixture{t: t, dir: dir}

	healthArgs := []string{"--node", "existing", "--expected-version", "1.26.0", "--tolerated-misses", "0"}
	out, errOut, code := health(t, f, healthArgs...)
	if code != 0 || !strings.Contains(out, "status=online") || !strings.Contains(out, "seq=1") ||
		!strings.Contains(out, "height=10 missed=0") {
		t.Fatalf("baseline health wrong: code=%d out=%q err=%q", code, out, errOut)
	}
	healthBefore := out

	// Batch: valid append to the existing node, valid first heartbeat for the
	// fresh node, then an invalid record (explicit null missed) at position 3.
	badBatch := `[
	  {"node":"existing","seq":2,"collected_at":"2026-10-01T11:59:58Z","version":"1.26.0","height":11,"missed":0},
	  {"node":"fresh","seq":1,"collected_at":"2026-10-01T11:59:58Z","version":"1.26.0","height":1,"missed":0},
	  {"node":"fresh","seq":2,"collected_at":"2026-10-01T11:59:59Z","version":"1.26.0","height":2,"missed":null}
	]`
	out, errOut, code = submit(t, dir, badBatch)
	assertRejectedBatch(t, out, errOut, code, "3", "missed")

	// The existing node's stored history is byte-for-byte unchanged even
	// though its valid record came first in the batch.
	if got := readNodeFile(t, dir, "existing"); got != existingBefore {
		t.Errorf("existing node file was modified during the rejected batch")
	}
	// Same query time and criteria: identical latest health result.
	out, _, code = health(t, f, healthArgs...)
	if code != 0 || out != healthBefore {
		t.Errorf("health of existing node changed after rejected batch:\nbefore=%q\nafter =%q (code=%d)", healthBefore, out, code)
	}

	// The first-time node must still have no history and answer 无遥测, and
	// no node file may have appeared for it.
	histOut, histErr, histCode := historyCLI(t, dir, "fresh")
	if histCode != 0 {
		t.Fatalf("history for fresh node after rejection exited %d: %q", histCode, histErr)
	}
	if strings.TrimRight(histOut, "\n") != "node=fresh no heartbeats" {
		t.Errorf("fresh node must have no history after rejected batch, got %q", histOut)
	}
	out, _, code = health(t, f,
		"--node", "fresh", "--expected-version", "1.26.0", "--tolerated-misses", "0")
	if code != 0 {
		t.Fatalf("health for fresh node must succeed, exit=%d", code)
	}
	if !strings.Contains(out, "status=无遥测") || strings.Contains(out, "seq=") {
		t.Errorf("fresh node must still report no telemetry, got %q", out)
	}
	if _, err := os.Stat(nodeFileHexPath(dir, "fresh")); !os.IsNotExist(err) {
		t.Errorf("no file may be created for the first-time node, stat err=%v", err)
	}

	// Correct the bad field and resubmit exactly the (now valid) records.
	fixedBatch := `[
	  {"node":"existing","seq":2,"collected_at":"2026-10-01T11:59:58Z","version":"1.26.0","height":11,"missed":0},
	  {"node":"fresh","seq":1,"collected_at":"2026-10-01T11:59:58Z","version":"1.26.0","height":1,"missed":0},
	  {"node":"fresh","seq":2,"collected_at":"2026-10-01T11:59:59Z","version":"1.26.0","height":2,"missed":0}
	]`
	out, errOut, code = submit(t, dir, fixedBatch)
	if code != 0 {
		t.Fatalf("corrected batch must submit successfully: exit=%d stderr=%q", code, errOut)
	}
	if strings.TrimRight(out, "\n") != "submitted: new=3 duplicate=0" {
		t.Errorf("corrected batch stdout=%q, want new=3 duplicate=0", out)
	}

	// The existing node keeps seq 1 and gains the appended seq 2, ascending.
	lines := historyLines(t, dir, "existing")
	if len(lines) != 2 || !strings.Contains(lines[0], "seq=1") ||
		!strings.Contains(lines[1], "seq=2") || !strings.Contains(lines[1], "height=11 missed=0") {
		t.Errorf("existing node history after recovery = %q", lines)
	}
	// The fresh node now has its first two heartbeats, ordered by seq.
	freshLines := historyLines(t, dir, "fresh")
	if len(freshLines) != 2 || !strings.Contains(freshLines[0], "seq=1") ||
		!strings.Contains(freshLines[1], "seq=2") || !strings.Contains(freshLines[1], "height=2 missed=0") {
		t.Errorf("fresh node history after recovery = %q", freshLines)
	}
	// Latest-telemetry rules apply to both: greatest seq, online at query time.
	out, _, code = health(t, f, healthArgs...)
	if code != 0 || !strings.Contains(out, "status=online") || !strings.Contains(out, "seq=2") ||
		!strings.Contains(out, "height=11 missed=0") {
		t.Errorf("existing node health after recovery = %q (code=%d)", out, code)
	}
	out, _, code = health(t, f,
		"--node", "fresh", "--expected-version", "1.26.0", "--tolerated-misses", "0")
	if code != 0 || !strings.Contains(out, "status=online") || !strings.Contains(out, "seq=2") ||
		!strings.Contains(out, "height=2 missed=0") {
		t.Errorf("fresh node health after recovery = %q (code=%d)", out, code)
	}
}
