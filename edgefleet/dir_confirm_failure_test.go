package edgefleet

// Regression coverage for directory-confirmation failures at the very end of
// saving a node file: the new file has already been written, fsynced and
// atomically renamed into place, but opening the containing directory or
// syncing it to disk fails. Before the fix these failures were silently
// ignored (a "best-effort" directory fsync), so heartbeat submit could print
// the success line even though the save's durability had not been confirmed.
//
// The required behaviour:
//
//   - the submit reports a save-stage error (non-zero exit at the command,
//     an error from Store.Submit) with new=0 duplicate=0 — the batch must not
//     present partially confirmed saves as a successful batch;
//   - the error names the directory involved and the underlying reason, and
//     is NOT phrased as a content conflict, an invalid field, or stored-data
//     corruption;
//   - records already renamed into place stay exactly where they are: the
//     file is genuinely saved and queryable, and nothing deletes it or
//     restores old content to fake an "all unsaved" outcome;
//   - an all-duplicate batch (or the pure-duplicate node in a mixed batch)
//     still attempts no write and therefore no directory confirmation, so it
//     cannot fail this way;
//   - short ids (nodes/) and long ids (slots/) behave identically.
//
// The fault is injected at the directory-confirmation seam (syncDir), after
// the atomic rename, so the exercise goes through the real persist path —
// real temp file, real fsync, real rename, real on-disk state afterwards.

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// dirConfirmFault temporarily replaces syncDir. failOnCall is the 1-based
// directory-confirmation call that must return an error; every other call
// performs the real open+sync. It returns a pointer to the number of
// confirmation calls actually attempted.
func dirConfirmFault(t *testing.T, failOnCall int) *int {
	t.Helper()
	calls := 0
	orig := syncDir
	syncDir = func(dir string) error {
		calls++
		if calls == failOnCall {
			return errors.New("injected directory confirmation failure")
		}
		return orig(dir)
	}
	t.Cleanup(func() { syncDir = orig })
	return &calls
}

// assertDirConfirmSaveError checks the common shape of a directory-
// confirmation failure reported by Submit: it is a save-stage (persistence)
// error naming the directory, with zero counts, and it is not mislabelled as
// a conflict, a field problem or stored-data corruption.
func assertDirConfirmSaveError(t *testing.T, err error, dir string, newC, dupC int) {
	t.Helper()
	if err == nil {
		t.Fatal("submit must report a failure when the directory confirmation fails")
	}
	msg := err.Error()
	if !strings.Contains(msg, "failed to persist heartbeat batch") {
		t.Errorf("error must place the failure in the save (persist) stage, got: %v", err)
	}
	if !strings.Contains(msg, dir) {
		t.Errorf("error must name the directory involved (%s), got: %v", dir, err)
	}
	if IsCorrupt(err) {
		t.Errorf("a directory-confirmation failure is not stored-data corruption, got: %v", err)
	}
	if strings.Contains(msg, "conflicting record") {
		t.Errorf("a directory-confirmation failure is not a content conflict, got: %v", err)
	}
	if newC != 0 || dupC != 0 {
		t.Errorf("failed submit must report new=0 duplicate=0, got new=%d duplicate=%d (partially confirmed saves are not a successful batch)", newC, dupC)
	}
}

// TestSubmitDirConfirmFailureAfterRenameKeepsSavedRecords is the primary
// scenario: two nodes each hold seq 1, the batch adds seq 2 to both, and the
// FIRST node's directory confirmation fails right after its new file was
// renamed into place. The whole batch reports a save error with zero counts;
// the renamed file's complete records (old seq 1 and new seq 2) remain
// queryable, the other node keeps its original history, and a verbatim
// resubmit after the fault clears converges.
func TestSubmitDirConfirmFailureAfterRenameKeepsSavedRecords(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase

	alpha1 := hb("alpha", 1, receive.Add(-2*time.Minute), "1.0", 100, 0)
	beta1 := hb("beta", 1, receive.Add(-2*time.Minute), "1.0", 200, 0)
	if _, _, err := store.Submit([]Heartbeat{alpha1, beta1}, receive); err != nil {
		t.Fatal(err)
	}

	alpha2 := hb("alpha", 2, receive.Add(-time.Minute), "1.0", 101, 0)
	beta2 := hb("beta", 2, receive.Add(-time.Minute), "1.0", 201, 1)
	batch := []Heartbeat{alpha2, beta2}

	// Fail the FIRST directory confirmation: one node's new file has already
	// been renamed into place, the other node's write is never attempted.
	calls := dirConfirmFault(t, 1)
	newC, dupC, err := store.Submit(batch, receive)
	assertDirConfirmSaveError(t, err, store.nodesDir, newC, dupC)
	if *calls != 1 {
		t.Fatalf("expected exactly one directory confirmation (the failing one), got %d", *calls)
	}

	// Which node was written first is not promised; identify it from the
	// data. The written node must show its COMPLETE history — the original
	// seq 1 untouched plus the new seq 2 — because the rename really happened;
	// the unwritten node keeps exactly its seq 1.
	query, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	alphaHist, err := query.History("alpha")
	if err != nil {
		t.Fatal(err)
	}
	betaHist, err := query.History("beta")
	if err != nil {
		t.Fatal(err)
	}
	var persistedNode, missingNode string
	switch {
	case len(alphaHist) == 2 && len(betaHist) == 1:
		persistedNode, missingNode = "alpha", "beta"
	case len(alphaHist) == 1 && len(betaHist) == 2:
		persistedNode, missingNode = "beta", "alpha"
	default:
		t.Fatalf("after the failed confirmation exactly one node must have 2 records and the other 1, got alpha=%d beta=%d", len(alphaHist), len(betaHist))
	}
	wantPersisted := []Heartbeat{alpha1, alpha2}
	wantMissing := []Heartbeat{beta1}
	if persistedNode == "beta" {
		wantPersisted = []Heartbeat{beta1, beta2}
		wantMissing = []Heartbeat{alpha1}
	}
	assertHistory(t, dir, persistedNode, wantPersisted)
	assertHistory(t, dir, missingNode, wantMissing)

	// Clear the fault and resubmit the SAME batch verbatim: the landed record
	// counts as a duplicate, the missing one is added exactly once.
	newC, dupC, err = store.Submit(batch, receive)
	if err != nil {
		t.Fatalf("verbatim resubmit after the fault is cleared must succeed: %v", err)
	}
	if newC != 1 || dupC != 1 {
		t.Errorf("resubmit: new=%d duplicate=%d, want new=1 duplicate=1", newC, dupC)
	}
	assertHistory(t, dir, "alpha", []Heartbeat{alpha1, alpha2})
	assertHistory(t, dir, "beta", []Heartbeat{beta1, beta2})
}

// TestSubmitDirConfirmFailureOnSecondNodeKeepsBothSavedFiles pins the
// no-rollback rule: the first node's save is fully confirmed, the second
// node's file is renamed into place but ITS directory confirmation fails.
// Both new records are genuinely on disk; the batch still reports the save
// error with zero counts, and nothing deletes the second node's renamed file
// to manufacture an "all unsaved" result. The verbatim resubmit then finds
// both records already saved and counts two duplicates.
func TestSubmitDirConfirmFailureOnSecondNodeKeepsBothSavedFiles(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase

	alpha1 := hb("alpha", 1, receive.Add(-2*time.Minute), "1.0", 100, 0)
	beta1 := hb("beta", 1, receive.Add(-2*time.Minute), "1.0", 200, 0)
	if _, _, err := store.Submit([]Heartbeat{alpha1, beta1}, receive); err != nil {
		t.Fatal(err)
	}

	alpha2 := hb("alpha", 2, receive.Add(-time.Minute), "1.0", 101, 0)
	beta2 := hb("beta", 2, receive.Add(-time.Minute), "1.0", 201, 1)
	batch := []Heartbeat{alpha2, beta2}

	calls := dirConfirmFault(t, 2)
	newC, dupC, err := store.Submit(batch, receive)
	assertDirConfirmSaveError(t, err, store.nodesDir, newC, dupC)
	if *calls != 2 {
		t.Fatalf("test setup: expected two directory confirmations (one success, one failure), got %d", *calls)
	}

	// Both nodes' seq-2 records were renamed into place before the failure,
	// so both complete two-record histories are queryable; the seq-1 records
	// are byte-for-byte the originals.
	assertHistory(t, dir, "alpha", []Heartbeat{alpha1, alpha2})
	assertHistory(t, dir, "beta", []Heartbeat{beta1, beta2})

	newC, dupC, err = store.Submit(batch, receive)
	if err != nil {
		t.Fatalf("verbatim resubmit after the fault is cleared must succeed: %v", err)
	}
	if newC != 0 || dupC != 2 {
		t.Errorf("resubmit: new=%d duplicate=%d, want new=0 duplicate=2 (both records landed before the failure)", newC, dupC)
	}
	assertHistory(t, dir, "alpha", []Heartbeat{alpha1, alpha2})
	assertHistory(t, dir, "beta", []Heartbeat{beta1, beta2})
}

// TestSubmitDirConfirmFailureNamesSlotsDirectoryForLongIds covers the other
// storage layout: a node id too long for a file name lives under slots/, and
// a directory-confirmation failure there must be reported the same way —
// save-stage error naming the slots directory, zero counts, complete saved
// records preserved.
func TestSubmitDirConfirmFailureNamesSlotsDirectoryForLongIds(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase

	longNode := strings.Repeat("长节点", 60) // slots/ layout
	rec1 := hb(longNode, 1, receive.Add(-2*time.Minute), "1.0", 100, 0)
	if _, _, err := store.Submit([]Heartbeat{rec1}, receive); err != nil {
		t.Fatal(err)
	}
	loc := store.nodeLocationFor(longNode)
	if filepath.Dir(loc.path) != store.slotsDir {
		t.Fatalf("test setup: long id must use a slot, got %s", loc.path)
	}

	rec2 := hb(longNode, 2, receive.Add(-time.Minute), "1.0", 101, 0)
	calls := dirConfirmFault(t, 1)
	newC, dupC, err := store.Submit([]Heartbeat{rec2}, receive)
	assertDirConfirmSaveError(t, err, store.slotsDir, newC, dupC)
	if *calls != 1 {
		t.Fatalf("expected one directory confirmation, got %d", *calls)
	}

	// The renamed file keeps both complete records; the verbatim resubmit
	// then counts the landed record as a duplicate.
	assertHistory(t, dir, longNode, []Heartbeat{rec1, rec2})
	newC, dupC, err = store.Submit([]Heartbeat{rec2}, receive)
	if err != nil {
		t.Fatalf("resubmit after the fault is cleared must succeed: %v", err)
	}
	if newC != 0 || dupC != 1 {
		t.Errorf("resubmit: new=%d duplicate=%d, want new=0 duplicate=1", newC, dupC)
	}
	assertHistory(t, dir, longNode, []Heartbeat{rec1, rec2})
}

// TestSubmitAllDuplicateBatchNeverConfirmsDirectory pins the narrow scope of
// the fix: a batch made entirely of records already saved attempts no node-
// file write and therefore no directory confirmation, so even a permanently
// failing confirmation cannot turn the harmless retransmit into a failed
// submit. The pure-duplicate node of a mixed batch is likewise left alone:
// its file is untouched and no confirmation is attempted for it, while the
// new-record node's confirmation failure still fails the batch.
func TestSubmitAllDuplicateBatchNeverConfirmsDirectory(t *testing.T) {
	dir := t.TempDir()
	receive := testBase
	dupRec := hb("dup-node", 1, receive.Add(-time.Minute), "1.0", 100, 0)
	if _, _, err := reopenStore(t, dir).Submit([]Heartbeat{dupRec}, receive); err != nil {
		t.Fatal(err)
	}

	store := reopenStore(t, dir)
	dupPath := store.nodePath("dup-node")
	dupBefore := fileSnapshot(t, dupPath)

	// All-duplicate batch: the confirmation seam fails on ANY call, and must
	// never be called at all.
	calls := dirConfirmFault(t, 1)
	newC, dupC, err := store.Submit([]Heartbeat{dupRec, dupRec}, receive)
	if err != nil {
		t.Fatalf("an all-duplicate batch needs no save confirmation and must succeed: %v", err)
	}
	if newC != 0 || dupC != 2 {
		t.Errorf("new=%d duplicate=%d, want new=0 duplicate=2", newC, dupC)
	}
	if *calls != 0 {
		t.Fatalf("no write means no directory confirmation, got %d call(s)", *calls)
	}
	if got := fileSnapshot(t, dupPath); got != dupBefore {
		t.Errorf("duplicate-only node file was rewritten:\nbefore=%q\nafter =%q", dupBefore, got)
	}

	// Mixed batch: the duplicate-only node keeps its file byte-for-byte while
	// the new-record node's file is renamed and its confirmation then fails.
	newRec := hb("fresh-node", 1, receive.Add(-30*time.Second), "1.0", 200, 2)
	calls = dirConfirmFault(t, 1)
	newC, dupC, err = store.Submit([]Heartbeat{dupRec, newRec}, receive)
	assertDirConfirmSaveError(t, err, store.nodesDir, newC, dupC)
	if *calls != 1 {
		t.Fatalf("only the new-record node's save is confirmed, got %d confirmation call(s)", *calls)
	}
	if got := fileSnapshot(t, dupPath); got != dupBefore {
		t.Errorf("pure-duplicate node file changed during the failed batch:\nbefore=%q\nafter =%q", dupBefore, got)
	}
	assertHistory(t, dir, "dup-node", []Heartbeat{dupRec})
	// The new node's record really landed (rename happened before the
	// confirmation failure) and stays queryable.
	assertHistory(t, dir, "fresh-node", []Heartbeat{newRec})
}

// TestSyncDirDefaultImplementationReportsBothFailureKinds exercises the real
// directory-confirmation step: a directory that cannot be opened is reported
// as an open failure naming the reason, and the error is a plain save
// failure — never a CorruptError.
func TestSyncDirDefaultImplementationReportsBothFailureKinds(t *testing.T) {
	dir := t.TempDir()
	missing := filepath.Join(dir, "no-such-directory")
	err := syncDir(missing)
	if err == nil {
		t.Fatal("opening a missing directory must fail")
	}
	if !strings.Contains(err.Error(), "cannot open directory") {
		t.Errorf("open failure must say the directory cannot be opened, got: %v", err)
	}
	if IsCorrupt(err) {
		t.Errorf("a directory-confirmation failure is not corruption, got: %v", err)
	}

	// A real directory confirms cleanly.
	if err := syncDir(dir); err != nil {
		t.Errorf("a healthy directory must confirm without error, got: %v", err)
	}
}
