package edgefleet

// Regression coverage for the final stage of a node-file save: after the new
// file is fsynced and atomically renamed into place, its directory is opened
// and fsynced to confirm the replacement. Until this change both directory
// operations were best-effort — an unopenable directory or a failing directory
// fsync was silently ignored, so `heartbeat submit` could still report success
// when the replacement was only temporarily reachable, not confirmed.
//
// These tests exercise the real save path:
//
//   - The directory-open failure is produced for real, offline and without
//     root: chmod the directory to 0300 (write+execute, no read). Temp-file
//     creation and rename still succeed (they need write+execute), but
//     os.Open on the directory fails with EACCES — a genuine failure that
//     occurs AFTER the rename, exactly the stage the bug lived in. Files
//     inside stay readable by name (execute/search permission suffices), so
//     the "records already saved remain queryable" guarantee is verified
//     through the normal History interface while the fault is in effect.
//   - The directory-fsync failure is injected at the syncDirectoryDurably
//     seam, again after the real rename, so the file on disk is genuinely the
//     replacement when the error is returned.
//
// Both layouts are covered: short ids under nodes/ and long (and multi-byte)
// ids under slots/.

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// errInjectedDirSync stands in for a directory fsync that fails (I/O error
// reported while syncing the directory entry to disk).
var errInjectedDirSync = errors.New("injected directory sync failure")

// makeDirUnopenable chmods dir to 0300: files inside can still be created,
// renamed and opened by exact name, but opening the directory itself fails —
// so it fails the post-rename confirmation open while leaving the rename
// reachable. It restores normal permissions during test cleanup (before the
// temp directory is removed).
func makeDirUnopenable(t *testing.T, dir string) {
	t.Helper()
	if err := os.Chmod(dir, 0o300); err != nil {
		t.Fatalf("chmod %s 0300: %v", dir, err)
	}
	t.Cleanup(func() {
		if err := os.Chmod(dir, 0o755); err != nil {
			t.Errorf("restore %s permissions: %v", dir, err)
		}
	})
}

// dirSyncFault temporarily replaces syncDirectoryDurably. failOnCall is the
// 1-based call that must return a sync SaveConfirmError; other calls run the
// real implementation. It returns a pointer to the number of confirmation
// calls attempted.
func dirSyncFault(t *testing.T, failOnCall int) *int {
	t.Helper()
	calls := 0
	orig := syncDirectoryDurably
	syncDirectoryDurably = func(dir string) error {
		calls++
		if calls == failOnCall {
			return &SaveConfirmError{Dir: dir, Op: "sync", Err: errInjectedDirSync}
		}
		return orig(dir)
	}
	t.Cleanup(func() { syncDirectoryDurably = orig })
	return &calls
}

// dirSyncSpy counts directory-confirmation calls without failing them.
func dirSyncSpy(t *testing.T) *int {
	t.Helper()
	calls := 0
	orig := syncDirectoryDurably
	syncDirectoryDurably = func(dir string) error {
		calls++
		return orig(dir)
	}
	t.Cleanup(func() { syncDirectoryDurably = orig })
	return &calls
}

// assertSaveConfirmError asserts err is the batch save-confirmation failure:
// it wraps a SaveConfirmError naming wantDir and op, it is not classified as
// corruption, and none of the words that would misdirect the operator (a
// heartbeat content conflict, an illegal field, stored corruption) appear.
func assertSaveConfirmError(t *testing.T, err error, wantDir, op string) *SaveConfirmError {
	t.Helper()
	if err == nil {
		t.Fatal("submit must report a save failure, got nil")
	}
	var sce *SaveConfirmError
	if !errors.As(err, &sce) {
		t.Fatalf("error must wrap a SaveConfirmError, got %T: %v", err, err)
	}
	if !IsSaveConfirm(err) {
		t.Errorf("IsSaveConfirm must be true for %v", err)
	}
	if IsCorrupt(err) {
		t.Errorf("a save-confirmation failure must not be reported as data corruption: %v", err)
	}
	if sce.Op != op {
		t.Errorf("SaveConfirmError.Op = %q, want %q", sce.Op, op)
	}
	if wantDir != "" && sce.Dir != wantDir {
		t.Errorf("SaveConfirmError.Dir = %q, want %q", sce.Dir, wantDir)
	}
	msg := err.Error()
	for _, want := range []string{"failed to persist heartbeat batch", "save confirmation failed", wantDir} {
		if want != "" && !strings.Contains(msg, want) {
			t.Errorf("error %q must mention %q", msg, want)
		}
	}
	for _, banned := range []string{"conflict", "corrupt", "invalid", "illegal", "must be"} {
		if strings.Contains(strings.ToLower(msg), banned) {
			t.Errorf("save failure must not be described with %q (would misdirect handling): %q", banned, msg)
		}
	}
	return sce
}

// TestSubmitDirectoryOpenFailureAfterRenameReportsSaveError is the single-node
// case for short ids: the replacement file is renamed into nodes/, then
// opening nodes/ to confirm it fails. The batch must be reported as a save
// failure with zero counts — not success — while the complete new file stays
// in place and queryable, and no half-written temp file is left behind.
func TestSubmitDirectoryOpenFailureAfterRenameReportsSaveError(t *testing.T) {
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

	nodesDir := filepath.Join(dir, "nodes")
	makeDirUnopenable(t, nodesDir)

	second := hb("n1", 2, receive.Add(-time.Minute), "1.0", 101, 1)
	newC, dupC, err := store.Submit([]Heartbeat{second}, receive)
	assertSaveConfirmError(t, err, nodesDir, "open")
	if newC != 0 || dupC != 0 {
		t.Errorf("failed submit must report new=0 duplicate=0, got new=%d duplicate=%d", newC, dupC)
	}

	// While the fault persists, the replacement is still readable by name
	// (History takes the lock in the parent dir and opens the file directly):
	// both records are complete, in order, with a valid checksum — the file is
	// not half-written and the old seq-1 content was not lost or altered.
	assertHistory(t, dir, "n1", []Heartbeat{first, second})

	// No temp file was left in the directory (listing needs read permission,
	// so restore it first).
	if err := os.Chmod(nodesDir, 0o755); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(nodesDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".tmp-") {
			t.Errorf("a failed confirmation must not leave a half-written temp file: found %s", e.Name())
		}
	}

	// Verbatim resubmit after the fault clears: the landed record is now a
	// duplicate, nothing is added twice.
	newC, dupC, err = store.Submit([]Heartbeat{second}, receive)
	if err != nil {
		t.Fatalf("verbatim resubmit must converge: %v", err)
	}
	if newC != 0 || dupC != 1 {
		t.Errorf("resubmit: new=%d duplicate=%d, want new=0 duplicate=1", newC, dupC)
	}
	assertHistory(t, dir, "n1", []Heartbeat{first, second})
}

// TestSubmitDirectoryOpenFailureTwoNodePartialSave follows the product
// example exactly: two nodes each already hold seq 1; the batch adds seq 2 to
// both. The directory confirmation fails on the first node written, so one
// node's new file is already replaced and the other node is never written.
// The whole batch reports failure with zero counts; history shows the real,
// order-independent progress; the verbatim batch then converges.
func TestSubmitDirectoryOpenFailureTwoNodePartialSave(t *testing.T) {
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

	nodesDir := filepath.Join(dir, "nodes")
	makeDirUnopenable(t, nodesDir)

	newC, dupC, err := store.Submit(batch, receive)
	assertSaveConfirmError(t, err, nodesDir, "open")
	if newC != 0 || dupC != 0 {
		t.Errorf("failed batch must report new=0 duplicate=0, got new=%d duplicate=%d", newC, dupC)
	}

	// Exactly one node landed (write order is map-driven and not promised);
	// identify it from the real on-disk state instead of naming it.
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
	var landed, pending string
	switch {
	case len(alphaHist) == 2 && len(betaHist) == 1:
		landed, pending = "alpha", "beta"
	case len(alphaHist) == 1 && len(betaHist) == 2:
		landed, pending = "beta", "alpha"
	default:
		t.Fatalf("after the failure one node must have 2 records and the other 1, got alpha=%d beta=%d",
			len(alphaHist), len(betaHist))
	}
	t.Logf("node %q was renamed in before the confirmation failure; %q was not written", landed, pending)

	// The landed node keeps both records complete; the pending node keeps its
	// original seq 1 exactly, and no file for it is half-written.
	if landed == "alpha" {
		assertHistory(t, dir, "alpha", []Heartbeat{alpha1, alpha2})
		assertHistory(t, dir, "beta", []Heartbeat{beta1})
	} else {
		assertHistory(t, dir, "beta", []Heartbeat{beta1, beta2})
		assertHistory(t, dir, "alpha", []Heartbeat{alpha1})
	}

	// Clear the fault and resubmit the same two records verbatim: the landed
	// seq 2 is a duplicate, the pending one is added once.
	if err := os.Chmod(nodesDir, 0o755); err != nil {
		t.Fatal(err)
	}
	newC, dupC, err = store.Submit(batch, receive)
	if err != nil {
		t.Fatalf("verbatim resubmit after the fault clears must succeed: %v", err)
	}
	if newC != 1 || dupC != 1 {
		t.Errorf("resubmit: new=%d duplicate=%d, want new=1 duplicate=1", newC, dupC)
	}
	assertHistory(t, dir, "alpha", []Heartbeat{alpha1, alpha2})
	assertHistory(t, dir, "beta", []Heartbeat{beta1, beta2})
}

// TestSubmitDirectoryOpenFailureLongIDSlotsLayout checks the slots/ layout
// used by long node ids behaves identically: the failing directory is the
// slots directory, named in the error, and the replaced slot's complete
// records stay queryable. Both a long ASCII id and a multi-byte (Hanzi) id are
// covered, so long and short ids give consistent results.
func TestSubmitDirectoryOpenFailureLongIDSlotsLayout(t *testing.T) {
	receive := testBase
	for _, node := range []string{
		strings.Repeat("a", 126), // 252 hex chars + ".json" -> slots/
		strings.Repeat("汉", 42),  // 126 UTF-8 bytes -> slots/
	} {
		t.Run("long id len="+itoa(len(node)), func(t *testing.T) {
			dir := t.TempDir()
			store, err := OpenStore(dir)
			if err != nil {
				t.Fatal(err)
			}
			loc := store.nodeLocationFor(node)
			if filepath.Dir(loc.path) != store.slotsDir {
				t.Fatalf("test setup: node must use slots/, got %s", loc.path)
			}
			first := hb(node, 1, receive.Add(-2*time.Minute), "1.0", 100, 0)
			if _, _, err := store.Submit([]Heartbeat{first}, receive); err != nil {
				t.Fatal(err)
			}

			makeDirUnopenable(t, store.slotsDir)
			second := hb(node, 2, receive.Add(-time.Minute), "1.0", 101, 0)
			newC, dupC, err := store.Submit([]Heartbeat{second}, receive)
			assertSaveConfirmError(t, err, store.slotsDir, "open")
			if newC != 0 || dupC != 0 {
				t.Errorf("failed submit must report new=0 duplicate=0, got new=%d duplicate=%d", newC, dupC)
			}

			// The complete slot file (two records, ownership and checksum
			// verified by History) stays on disk.
			assertHistory(t, dir, node, []Heartbeat{first, second})

			newC, dupC, err = store.Submit([]Heartbeat{second}, receive)
			if err != nil {
				t.Fatalf("verbatim resubmit must converge: %v", err)
			}
			if newC != 0 || dupC != 1 {
				t.Errorf("resubmit: new=%d duplicate=%d, want new=0 duplicate=1", newC, dupC)
			}
		})
	}
}

// TestSubmitDirectorySyncFailureAfterRenameReportsSaveError covers the second
// confirmation failure named by the product: the directory opens fine but
// syncing it to disk returns an error. The injected failure runs after the
// real rename, so the replacement is genuinely on disk when the save is
// reported as failed.
func TestSubmitDirectorySyncFailureAfterRenameReportsSaveError(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase
	first := hb("n1", 1, receive.Add(-2*time.Minute), "1.0", 100, 0)
	second := hb("n1", 2, receive.Add(-time.Minute), "1.0", 101, 0)
	if _, _, err := store.Submit([]Heartbeat{first}, receive); err != nil {
		t.Fatal(err)
	}

	calls := dirSyncFault(t, 1)
	newC, dupC, err := store.Submit([]Heartbeat{second}, receive)
	sce := assertSaveConfirmError(t, err, filepath.Join(dir, "nodes"), "sync")
	if !errors.Is(err, errInjectedDirSync) || !errors.Is(sce, errInjectedDirSync) {
		t.Errorf("the underlying sync reason must be reachable via errors.Is, got %v", err)
	}
	if !strings.Contains(err.Error(), "cannot sync directory to disk") {
		t.Errorf("error must explain the directory sync failed, got %v", err)
	}
	if newC != 0 || dupC != 0 {
		t.Errorf("failed submit must report new=0 duplicate=0, got new=%d duplicate=%d", newC, dupC)
	}
	if *calls != 1 {
		t.Errorf("exactly one directory confirmation must be attempted, got %d", *calls)
	}

	// The rename already happened: the complete two-record file is the one on
	// disk, never rolled back to the single-record version.
	assertHistory(t, dir, "n1", []Heartbeat{first, second})

	// Resubmitting verbatim converges: the landed record counts once as a
	// duplicate.
	newC, dupC, err = store.Submit([]Heartbeat{second}, receive)
	if err != nil {
		t.Fatalf("verbatim resubmit must converge: %v", err)
	}
	if newC != 0 || dupC != 1 {
		t.Errorf("resubmit: new=%d duplicate=%d, want new=0 duplicate=1", newC, dupC)
	}
}

// TestSubmitDirectorySyncFailureMidBatchLeavesEarlierSaveIntact places the
// injected sync failure on the second node written in a two-node batch: the
// first node's replacement is confirmed normally, the second is renamed and
// then fails confirmation. Both nodes' complete records survive and the
// verbatim resubmit converges.
func TestSubmitDirectorySyncFailureMidBatchLeavesEarlierSaveIntact(t *testing.T) {
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

	calls := dirSyncFault(t, 2)
	newC, dupC, err := store.Submit(batch, receive)
	assertSaveConfirmError(t, err, filepath.Join(dir, "nodes"), "sync")
	if newC != 0 || dupC != 0 {
		t.Errorf("failed batch must report new=0 duplicate=0, got new=%d duplicate=%d", newC, dupC)
	}
	if *calls != 2 {
		t.Fatalf("both node saves must reach confirmation, got %d call(s)", *calls)
	}

	// Both nodes' seq 2 files were renamed; the failure only stops the success
	// report, never removes a save. Which order the map chose is irrelevant:
	// both histories must already be complete.
	assertHistory(t, dir, "alpha", []Heartbeat{alpha1, alpha2})
	assertHistory(t, dir, "beta", []Heartbeat{beta1, beta2})

	// Verbatim resubmit: both landed records are duplicates.
	newC, dupC, err = store.Submit(batch, receive)
	if err != nil {
		t.Fatalf("verbatim resubmit must converge: %v", err)
	}
	if newC != 0 || dupC != 2 {
		t.Errorf("resubmit: new=%d duplicate=%d, want new=0 duplicate=2", newC, dupC)
	}
}

// TestSubmitAllDuplicateBatchSkipsDirectoryConfirmation pins the narrow scope
// of the fix: the directory confirmation is part of saving NEW records only.
// A batch made entirely of already-saved duplicates must not open or sync
// either directory, must not fail even when the directory cannot be opened,
// and keeps counting duplicates per input occurrence.
func TestSubmitAllDuplicateBatchSkipsDirectoryConfirmation(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase
	rec := hb("dup-node", 1, receive.Add(-time.Minute), "1.0", 100, 0)
	if _, _, err := store.Submit([]Heartbeat{rec}, receive); err != nil {
		t.Fatal(err)
	}

	// With the directory genuinely unopenable, an all-duplicate batch must
	// still succeed (no rewrite, hence no confirmation attempt at all).
	nodesDir := filepath.Join(dir, "nodes")
	makeDirUnopenable(t, nodesDir)
	newC, dupC, err := store.Submit([]Heartbeat{rec, rec, rec}, receive)
	if err != nil {
		t.Fatalf("an all-duplicate batch needs no save confirmation and must succeed: %v", err)
	}
	if newC != 0 || dupC != 3 {
		t.Errorf("new=%d duplicate=%d, want new=0 duplicate=3", newC, dupC)
	}
	assertHistory(t, dir, "dup-node", []Heartbeat{rec})
}

// TestSubmitAllDuplicateBatchMakesNoConfirmationCall proves the skip at the
// seam itself: with the directory healthy, a duplicate-only submit never
// reaches the directory-confirmation step, whereas a normal new-record submit
// confirms once per updated node.
func TestSubmitAllDuplicateBatchMakesNoConfirmationCall(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase
	rec := hb("dup-node", 1, receive.Add(-time.Minute), "1.0", 100, 0)
	if _, _, err := store.Submit([]Heartbeat{rec}, receive); err != nil {
		t.Fatal(err)
	}

	syncs := dirSyncSpy(t)
	if _, _, err := store.Submit([]Heartbeat{rec, rec}, receive); err != nil {
		t.Fatal(err)
	}
	if *syncs != 0 {
		t.Errorf("a duplicate-only submit must not attempt a directory confirmation, got %d call(s)", *syncs)
	}

	// A genuinely new record for another node confirms its own directory once.
	other := hb("other-node", 1, receive.Add(-30*time.Second), "1.0", 1, 0)
	if _, _, err := store.Submit([]Heartbeat{rec, other}, receive); err != nil {
		t.Fatal(err)
	}
	if *syncs != 1 {
		t.Errorf("exactly one directory confirmation for the one new node, got %d call(s)", *syncs)
	}
}

// TestSubmitInvalidAndConflictingBatchesRejectedBeforeConfirmation makes sure
// the new save-stage failure never masks the earlier, batch-wide rejections:
// with the directory unopenable and a sync-fault seam installed, an illegal
// record and a same-node/same-seq content conflict must still be rejected as
// such before any file write or directory confirmation, leave no record, and
// never be reported as a save failure.
func TestSubmitInvalidAndConflictingBatchesRejectedBeforeConfirmation(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase
	good := hb("n1", 1, receive.Add(-time.Minute), "1.0", 100, 0)
	if _, _, err := store.Submit([]Heartbeat{good}, receive); err != nil {
		t.Fatal(err)
	}

	nodesDir := filepath.Join(dir, "nodes")
	makeDirUnopenable(t, nodesDir)
	syncs := dirSyncSpy(t)

	// Illegal field (negative height) in a batch with an otherwise-legal new
	// node: rejected as an invalid heartbeat, nothing saved.
	illegal := hb("n1", 2, receive.Add(-time.Minute), "1.0", -1, 0)
	legalNew := hb("fresh-node", 1, receive.Add(-30*time.Second), "1.0", 1, 0)
	newC, dupC, err := store.Submit([]Heartbeat{illegal, legalNew}, receive)
	if err == nil || !strings.Contains(err.Error(), "invalid heartbeat") {
		t.Fatalf("illegal input must be rejected before saving, got %v", err)
	}
	if IsSaveConfirm(err) {
		t.Errorf("an input rejection must not be reported as a save-confirmation failure: %v", err)
	}
	if newC != 0 || dupC != 0 {
		t.Errorf("rejected batch must report zero counts, got new=%d duplicate=%d", newC, dupC)
	}

	// Same node+seq with different content: a conflict, not a save failure.
	conflict := hb("n1", 1, receive.Add(-time.Minute), "2.0", 100, 0)
	newC, dupC, err = store.Submit([]Heartbeat{conflict, legalNew}, receive)
	if err == nil || !strings.Contains(err.Error(), "conflicting record") {
		t.Fatalf("content conflict must be rejected before saving, got %v", err)
	}
	if IsSaveConfirm(err) {
		t.Errorf("a conflict must not be reported as a save-confirmation failure: %v", err)
	}
	if newC != 0 || dupC != 0 {
		t.Errorf("rejected conflict batch must report zero counts, got new=%d duplicate=%d", newC, dupC)
	}

	if *syncs != 0 {
		t.Errorf("nothing may reach directory confirmation on pre-save rejection, got %d call(s)", *syncs)
	}

	// Restore read permission so the state checks can list and read normally.
	if err := os.Chmod(nodesDir, 0o755); err != nil {
		t.Fatal(err)
	}
	assertHistory(t, dir, "n1", []Heartbeat{good})
	if _, err := os.Stat(store.nodePath("fresh-node")); !os.IsNotExist(err) {
		t.Errorf("the legal companion record must not be saved, stat err=%v", err)
	}
}

// itoa is a tiny local int-to-string to avoid pulling strconv into one line.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
