package edgefleet

// Regression tests for duplicate-only replays: a batch that adds nothing to a
// node must still be checked (history read, ownership/checksum verified,
// content compared for conflicts), but it must not create or replace that
// node's history file. Previously Submit re-saved every node a batch touched,
// so an exact replay could be reported as a write failure and pointlessly
// rewrote files that were already correct.

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// makeReadOnly denies create/replace permission in a directory while keeping
// its contents readable; the original mode is restored while the test tears
// down (before t.TempDir's own cleanup, so the temp dir can still be removed).
func makeReadOnly(t *testing.T, dir string) {
	t.Helper()
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })
}

// TestSubmitPureDuplicateBatchSucceedsWithoutWriteAccess replays one saved
// heartbeat three times against a nodes directory in which no file can be
// created or replaced. All inputs match the saved history, so the batch must
// succeed with new=0 duplicate=3 and leave the original file byte-for-byte
// untouched.
func TestSubmitPureDuplicateBatchSucceedsWithoutWriteAccess(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase
	saved := hb("n1", 1, receive.Add(-time.Minute), "1.0", 100, 0)
	if _, _, err := store.Submit([]Heartbeat{saved}, receive); err != nil {
		t.Fatal(err)
	}
	path := store.nodePath("n1")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	// The save location can still be locked and read, but not written.
	makeReadOnly(t, filepath.Join(dir, "nodes"))

	// The saved record appears three times in the input: three duplicates.
	newC, dupC, err := store.Submit([]Heartbeat{saved, saved, saved}, receive)
	if err != nil {
		t.Fatalf("pure duplicate replay must not require write access: %v", err)
	}
	if newC != 0 || dupC != 3 {
		t.Errorf("new=%d dup=%d, want 0/3", newC, dupC)
	}

	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(before) {
		t.Errorf("duplicate-only node file was rewritten")
	}
	hist, err := store.History("n1")
	if err != nil {
		t.Fatal(err)
	}
	if len(hist) != 1 || !hist[0].Equal(saved) {
		t.Errorf("history must still hold the single original record: %+v", hist)
	}
}

// TestSubmitReadOnlyLocationReallyBlocksWrites proves the permission injection
// used above genuinely forbids a write: a new record for the same node under
// the read-only directory must fail. Without this, the "succeeds without
// write access" test would prove nothing.
func TestSubmitReadOnlyLocationReallyBlocksWrites(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase
	if _, _, err := store.Submit([]Heartbeat{hb("n1", 1, receive.Add(-time.Minute), "1.0", 100, 0)}, receive); err != nil {
		t.Fatal(err)
	}
	makeReadOnly(t, filepath.Join(dir, "nodes"))

	_, _, err = store.Submit([]Heartbeat{hb("n1", 2, receive.Add(-time.Second), "1.0", 101, 0)}, receive)
	if err == nil {
		t.Fatal("a genuinely new record under the read-only location must fail to persist")
	}
	if !strings.Contains(err.Error(), "failed to persist heartbeat batch") {
		t.Errorf("persistence failure must keep its public wording, got %v", err)
	}
}

// TestSubmitMixedBatchSkipsUnchangedNodeInReadOnlyLocation mixes two save
// locations: an existing long-id node (stored under slots/) is replayed as a
// pure duplicate while slots/ is read-only, and a brand-new short-id node
// (stored under nodes/, still writable) gets its first record. The batch must
// succeed: only the changed node is written, and the other node's lack of
// write permission must not fail the batch.
func TestSubmitMixedBatchSkipsUnchangedNodeInReadOnlyLocation(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase

	// 127 ASCII bytes -> a 259-byte hex name, over the 255 limit: this node
	// lives under slots/, a different save location than short ids.
	longID := strings.Repeat("a", 127)
	old := hb(longID, 1, receive.Add(-time.Minute), "1.0", 100, 0)
	if _, _, err := store.Submit([]Heartbeat{old}, receive); err != nil {
		t.Fatal(err)
	}
	longPath := store.nodePath(longID)
	before, err := os.ReadFile(longPath)
	if err != nil {
		t.Fatal(err)
	}

	makeReadOnly(t, filepath.Join(dir, "slots"))

	batch := []Heartbeat{
		old, old, old, // three history duplicates for the read-only location
		hb("short", 1, receive.Add(-time.Second), "1.0", 1, 0), // needs a write in nodes/
	}
	newC, dupC, err := store.Submit(batch, receive)
	if err != nil {
		t.Fatalf("mixed batch must not fail on the unchanged node's read-only location: %v", err)
	}
	if newC != 1 || dupC != 3 {
		t.Errorf("new=%d dup=%d, want 1/3", newC, dupC)
	}

	if got, err := os.ReadFile(longPath); err != nil || string(got) != string(before) {
		t.Errorf("unchanged long-id file was rewritten, err=%v", err)
	}
	hist, err := store.History("short")
	if err != nil {
		t.Fatal(err)
	}
	if len(hist) != 1 || hist[0].NodeID != "short" || hist[0].Seq != 1 {
		t.Errorf("new short-id node was not saved: %+v", hist)
	}

	// The read-only location really is unwritable: a new seq for the long-id
	// node must still fail in the public persistence-failure mode.
	_, _, err = store.Submit([]Heartbeat{hb(longID, 2, receive.Add(-time.Second), "1.0", 101, 0)}, receive)
	if err == nil {
		t.Error("new record for the long-id node must fail while slots/ is read-only")
	}
}

// TestSubmitDuplicateNodeMixedWithDuplicateAndNewRecordsWithinNode checks the
// per-node counting and ordering when one node contributes both kinds: the
// new seq's first appearance is new, its repeat and every saved-record
// appearance are duplicates, and history stays ascending with one row per seq.
func TestSubmitDuplicateNodeMixedWithDuplicateAndNewRecordsWithinNode(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase
	first := hb("n1", 1, receive.Add(-2*time.Minute), "1.0", 100, 0)
	if _, _, err := store.Submit([]Heartbeat{first}, receive); err != nil {
		t.Fatal(err)
	}

	second := hb("n1", 2, receive.Add(-time.Minute), "1.0", 101, 0)
	batch := []Heartbeat{
		first,  // saved history: duplicate
		second, // first appearance of a new record: new
		first,  // saved history again: duplicate
		second, // same new record repeated in the batch: duplicate
	}
	newC, dupC, err := store.Submit(batch, receive)
	if err != nil {
		t.Fatalf("submit failed: %v", err)
	}
	if newC != 1 || dupC != 3 {
		t.Errorf("new=%d dup=%d, want 1/3", newC, dupC)
	}
	hist, err := store.History("n1")
	if err != nil {
		t.Fatal(err)
	}
	if len(hist) != 2 || hist[0].Seq != 1 || hist[1].Seq != 2 || !hist[1].Equal(second) {
		t.Errorf("history must be ascending with one record per seq: %+v", hist)
	}
}

// TestSubmitPureDuplicateStillRejectsCorruption makes sure skipping the write
// never skips the checks: an exact replay against a damaged history file is a
// corruption error, counted as nothing, before any save could happen.
func TestSubmitPureDuplicateStillRejectsCorruption(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase
	saved := hb("n1", 1, receive.Add(-time.Minute), "1.0", 100, 0)
	if _, _, err := store.Submit([]Heartbeat{saved}, receive); err != nil {
		t.Fatal(err)
	}
	path := store.nodePath("n1")
	corrupt := []byte(`{not json`)
	if err := os.WriteFile(path, corrupt, 0o600); err != nil {
		t.Fatal(err)
	}
	// A read-only location would make a write impossible; the batch must fail
	// anyway, proving the failure is the corruption check, not a write.
	makeReadOnly(t, filepath.Join(dir, "nodes"))

	newC, dupC, err := store.Submit([]Heartbeat{saved, saved, saved}, receive)
	if err == nil || !IsCorrupt(err) {
		t.Fatalf("replay against corrupt history must be a corruption error, got %v", err)
	}
	if newC != 0 || dupC != 0 {
		t.Errorf("rejected batch must report zero counts, got new=%d dup=%d", newC, dupC)
	}
	if got, err := os.ReadFile(path); err != nil || string(got) != string(corrupt) {
		t.Errorf("corrupt file must stay byte-for-byte unchanged, err=%v", err)
	}
}

// TestSubmitPureDuplicateBatchStillRejectsConflict checks that an all-replay
// batch containing a same-seq content mismatch against history is rejected
// with the usual conflict error, even though no node needed a write.
func TestSubmitPureDuplicateBatchStillRejectsConflict(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase
	saved := hb("n1", 1, receive.Add(-time.Minute), "1.0", 100, 0)
	if _, _, err := store.Submit([]Heartbeat{saved}, receive); err != nil {
		t.Fatal(err)
	}
	makeReadOnly(t, filepath.Join(dir, "nodes"))

	// One verbatim duplicate of n1, one conflicting copy of n1 seq 1, and a
	// legal first record for a fresh node that must not be created.
	batch := []Heartbeat{
		saved,
		hb("n1", 1, receive.Add(-time.Minute), "2.0", 100, 0),
		hb("fresh", 1, receive.Add(-time.Second), "1.0", 1, 0),
	}
	newC, dupC, err := store.Submit(batch, receive)
	if err == nil || !strings.Contains(err.Error(), "conflicting record") {
		t.Fatalf("expected conflict error, got %v", err)
	}
	if newC != 0 || dupC != 0 {
		t.Errorf("conflicting batch must report zero counts, got new=%d dup=%d", newC, dupC)
	}
	hist, err := store.History("n1")
	if err != nil || len(hist) != 1 || !hist[0].Equal(saved) {
		t.Fatalf("saved history must be unchanged after conflict: %+v err=%v", hist, err)
	}
	if _, err := os.Stat(store.nodePath("fresh")); !os.IsNotExist(err) {
		t.Errorf("fresh node file must not be created, stat err=%v", err)
	}
}

// TestSubmitPureDuplicateFailsWhenLockUnavailable pins the other half of the
// contract: duplicate replays need no file-write permission, but they still
// take the data lock. Failure to acquire it is reported as before.
func TestSubmitPureDuplicateFailsWhenLockUnavailable(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase
	saved := hb("n1", 1, receive.Add(-time.Minute), "1.0", 100, 0)
	if _, _, err := store.Submit([]Heartbeat{saved}, receive); err != nil {
		t.Fatal(err)
	}

	// Deny opening the lock file at all; the replay must fail at locking.
	lockPath := filepath.Join(dir, "lock")
	if err := os.Chmod(lockPath, 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(lockPath, 0o600) })

	if _, _, err := store.Submit([]Heartbeat{saved, saved}, receive); err == nil {
		t.Fatal("replay must fail when the data lock cannot be acquired")
	} else if !strings.Contains(err.Error(), "lock") {
		t.Errorf("error must come from locking, got %v", err)
	}
}
