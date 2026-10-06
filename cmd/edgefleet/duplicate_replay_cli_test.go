package main

// Command-line regression tests for duplicate-only replays.
//
// `heartbeat submit` historically re-saved every node a batch touched, even
// when that node gained no new records. An exact replay could therefore be
// reported as a write failure when the node's save location could not create
// or replace files, and it pointlessly rewrote files that were already
// correct. These tests pin the fixed user-visible contract:
//
//   - a batch whose records all duplicate saved history succeeds with
//     new=0 and one duplicate count per input occurrence, even when the save
//     location denies file creation/replacement;
//   - the existing history file is not rewritten and stays the single source
//     of truth;
//   - in a mixed batch only nodes that gained records are saved, so one
//     node's read-only location cannot fail records another node needs;
//   - checks are not skipped: lock failure, saved-history corruption and
//     same-seq content conflicts still reject the batch non-zero with the
//     reason on stderr and no success count on stdout.

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// chmodReadOnly denies create/replace permission in dir while keeping its
// contents readable; the mode is restored before the temp dir is removed.
func chmodReadOnly(t *testing.T, dir string) {
	t.Helper()
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })
}

// TestCLISubmitPureDuplicateBatchSucceedsWhenSaveLocationReadOnly saves one
// heartbeat, makes the nodes directory unable to create or replace files, and
// then submits the same saved record three times. The batch must succeed with
// new=0 duplicate=3, stderr empty, and the original file byte-for-byte
// unchanged.
func TestCLISubmitPureDuplicateBatchSucceedsWhenSaveLocationReadOnly(t *testing.T) {
	dir := t.TempDir()
	const saved = `{"node":"dupnode","seq":7,"collected_at":"2026-10-01T11:59:00Z","version":"1.26.0","height":12345,"missed":0}`
	submitBatch(t, dir, "["+saved+"]")
	before := readNodeFile(t, dir, "dupnode")

	chmodReadOnly(t, filepath.Join(dir, "nodes"))

	out, errOut, code := submit(t, dir, "["+saved+","+saved+","+saved+"]")
	if code != 0 {
		t.Fatalf("pure duplicate replay must succeed without write access: exit=%d stderr=%q", code, errOut)
	}
	if errOut != "" {
		t.Errorf("successful replay must leave stderr empty, got %q", errOut)
	}
	if strings.TrimRight(out, "\n") != "submitted: new=0 duplicate=3" {
		t.Errorf("stdout=%q, want new=0 duplicate=3", out)
	}
	if got := readNodeFile(t, dir, "dupnode"); got != before {
		t.Errorf("duplicate-only node file was rewritten")
	}

	lines := historyLines(t, dir, "dupnode")
	if len(lines) != 1 || !strings.Contains(lines[0], "node=dupnode seq=7") {
		t.Errorf("history must still hold only the original record: %q", lines)
	}
}

// TestCLISubmitNewRecordStillFailsWhenSaveLocationReadOnly proves the
// permission injection is real: with the save location read-only, a batch
// that actually needs to persist a new record keeps the existing
// persistence-failure contract.
func TestCLISubmitNewRecordStillFailsWhenSaveLocationReadOnly(t *testing.T) {
	dir := t.TempDir()
	submitBatch(t, dir, `[
	  {"node":"n1","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":100,"missed":0}
	]`)
	chmodReadOnly(t, filepath.Join(dir, "nodes"))

	out, errOut, code := submit(t, dir, `[
	  {"node":"n1","seq":2,"collected_at":"2026-10-01T11:59:30Z","version":"1.0","height":101,"missed":0}
	]`)
	if code == 0 {
		t.Fatalf("a genuinely new record must fail to persist, stdout=%q", out)
	}
	if strings.Contains(out, "submitted") || strings.Contains(out, "new=") {
		t.Errorf("persistence failure must print no success count, stdout=%q", out)
	}
	if !strings.Contains(errOut, "failed to persist heartbeat batch") {
		t.Errorf("stderr must report the persistence failure, got %q", errOut)
	}

	// The previous record remains the only history after the failure.
	lines := historyLines(t, dir, "n1")
	if len(lines) != 1 || !strings.Contains(lines[0], "seq=1") {
		t.Errorf("previous history must remain intact: %q", lines)
	}
}

// TestCLISubmitMixedBatchOnlyWritesChangedNodes mixes the two storage
// locations in one batch: an existing long-id node under slots/ is replayed
// as exact duplicates while slots/ denies writes, and a new short-id node
// under nodes/ (still writable) receives its first record. The batch must
// succeed — the duplicate-only node's missing write capability must not fail
// the record the other node genuinely needs.
func TestCLISubmitMixedBatchOnlyWritesChangedNodes(t *testing.T) {
	dir := t.TempDir()

	// 127 ASCII characters: hex name is 259 bytes, over the 255 limit, so the
	// node is stored under slots/.
	longNode := strings.Repeat("a", 127)
	submitBatch(t, dir, `[
	  {"node":"`+longNode+`","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":100,"missed":0}
	]`)
	longBefore, err := os.ReadFile(nodeFileHexPath(dir, longNode))
	if err != nil {
		t.Fatal(err)
	}

	chmodReadOnly(t, filepath.Join(dir, "slots"))

	out, errOut, code := submit(t, dir, `[
	  {"node":"`+longNode+`","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":100,"missed":0},
	  {"node":"`+longNode+`","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":100,"missed":0},
	  {"node":"short","seq":1,"collected_at":"2026-10-01T11:59:30Z","version":"1.0","height":1,"missed":0}
	]`)
	if code != 0 {
		t.Fatalf("mixed batch must not fail on the unchanged node's read-only location: exit=%d stderr=%q", code, errOut)
	}
	if strings.TrimRight(out, "\n") != "submitted: new=1 duplicate=2" {
		t.Errorf("stdout=%q, want new=1 duplicate=2", out)
	}

	if got, err := os.ReadFile(nodeFileHexPath(dir, longNode)); err != nil || string(got) != string(longBefore) {
		t.Errorf("duplicate-only long-id file was rewritten, err=%v", err)
	}
	lines := historyLines(t, dir, "short")
	if len(lines) != 1 || !strings.Contains(lines[0], "node=short seq=1") {
		t.Errorf("the changed node's new record must be saved: %q", lines)
	}
}

// TestCLISubmitPureDuplicateReplayFailsOnCorruptHistory ensures the read-only
// shortcut skips only the write, never the history check: a replay against a
// damaged file fails non-zero with the corruption reason, prints no counts,
// and leaves the damaged bytes exactly as they were.
func TestCLISubmitPureDuplicateReplayFailsOnCorruptHistory(t *testing.T) {
	dir := t.TempDir()
	const record = `{"node":"bad","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":100,"missed":0}`
	submitBatch(t, dir, "["+record+"]")
	path := nodeFileHexPath(dir, "bad")
	if err := os.WriteFile(path, []byte(`{not json`), 0o600); err != nil {
		t.Fatal(err)
	}
	corruptBefore, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	chmodReadOnly(t, filepath.Join(dir, "nodes"))

	out, errOut, code := submit(t, dir, "["+record+","+record+"]")
	if code == 0 {
		t.Fatalf("replay against corrupt history must fail non-zero; stdout=%q", out)
	}
	if strings.Contains(out, "submitted") || strings.Contains(out, "new=") {
		t.Errorf("corruption rejection must print no success count, stdout=%q", out)
	}
	if !strings.Contains(errOut, "error:") || !strings.Contains(errOut, "corruption") {
		t.Errorf("stderr must explain the corruption, got %q", errOut)
	}
	if got, err := os.ReadFile(path); err != nil || string(got) != string(corruptBefore) {
		t.Errorf("corrupt file must stay byte-for-byte unchanged, err=%v", err)
	}
}

// TestCLISubmitPureDuplicateReplayRejectsConflict checks an all-duplicate
// batch that nonetheless contains a same-seq content mismatch: the conflict
// is rejected before any save, a new node sharing the batch is not created,
// and the saved history is untouched.
func TestCLISubmitPureDuplicateReplayRejectsConflict(t *testing.T) {
	dir := t.TempDir()
	submitBatch(t, dir, `[
	  {"node":"n1","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":100,"missed":0}
	]`)
	before := readNodeFile(t, dir, "n1")
	chmodReadOnly(t, filepath.Join(dir, "nodes"))

	out, errOut, code := submit(t, dir, `[
	  {"node":"n1","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":100,"missed":0},
	  {"node":"n1","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"2.0","height":100,"missed":0},
	  {"node":"fresh","seq":1,"collected_at":"2026-10-01T11:59:30Z","version":"1.0","height":1,"missed":0}
	]`)
	if code == 0 {
		t.Fatalf("conflicting replay must fail non-zero; stdout=%q", out)
	}
	if !strings.Contains(errOut, "conflicting record") || !strings.Contains(errOut, "seq 1") {
		t.Errorf("stderr must name the conflict, got %q", errOut)
	}
	if strings.Contains(out, "submitted") || strings.Contains(out, "new=") {
		t.Errorf("conflict rejection must print no success count, stdout=%q", out)
	}
	if got := readNodeFile(t, dir, "n1"); got != before {
		t.Errorf("saved history changed after conflicting replay")
	}
	// slots/nodes read-only also makes a fresh-file create impossible; even so
	// the rejection is the conflict, discovered before any write was attempted.
	if _, err := os.Stat(nodeFileHexPath(dir, "fresh")); !os.IsNotExist(err) {
		t.Errorf("fresh node file must not be created, stat err=%v", err)
	}
}
