package edgefleet

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// This file is the concurrency regression for the batch-visibility guarantee
// of History against Submit:
//
// When one caller backfills several legal heartbeats for a node in a single
// Submit, every other caller querying history through the same local data
// directory while that submit is in flight must see EITHER
//
//   - the complete history as it was before the batch, OR
//   - the complete history after the whole batch was saved,
//
// never only part of the batch (e.g. seq 3 alone, or seqs 3 and 6 but not 9).
// Each returned record must keep its original collected_at, version, height
// and cumulative missed count; old records must not be lost or replaced and
// new seqs must not be duplicated. Different queries may land on different
// sides of the submit, but every single returned snapshot must be whole. A
// query issued after Submit returned successfully must contain the whole
// batch. A node that had no heartbeats yet follows the same rule: the first
// backfill overlapping a query may surface as the existing empty history or
// the complete sorted batch, never a partial one, and waiting for the save to
// finish must not look like corruption or a query failure.
//
// A saved file is always internally well-formed (temp file + fsync + atomic
// rename, and a checksum over the records), so a checksum can never tell a
// half-batch from a whole one — the batch boundary is not encoded on disk.
// The guarantee therefore comes from the directory flock: Submit holds the
// exclusive lock across the whole save and History takes a shared lock for
// the whole read. These tests prove that guarantee at BOTH ends:
//
//   - positively, with genuine submit/query overlap forced deterministically
//     (blocked locks, not sleeping and hoping for a race), asserting every
//     overlapping snapshot is one of the two complete histories — including
//     the case where the complete new batch is already readable while the
//     submitter has not yet processed its own success result (save
//     completion and result delivery are different moments; a fully saved
//     batch seen early is legal, never a "never submitted" heartbeat);
//   - negatively, by forging checksum-valid files containing only part of a
//     batch and by driving a deliberately lock-free staged writer that makes
//     the intermediate states reachable, proving the snapshot classifier
//     this regression relies on actually recognises a real half batch.
//
// Everything runs offline against a local temp directory and synchronises on
// channels and real flock state, so the result cannot depend on machine speed
// or on which caller happens to process its success result first.

// batchRecord builds one legal heartbeat whose telemetry is distinct for its
// seq, so a replaced or mixed-up record cannot pass unnoticed: each seq has
// its own collection instant, version, height and missed count.
func batchRecord(node string, seq int64, receive time.Time) Heartbeat {
	return hb(node, seq,
		receive.Add(-time.Duration(seq)*time.Minute),
		fmt.Sprintf("v%d", seq),
		seq*100,
		seq)
}

// batchSaveGate parks a Submit inside writeNodeFile AFTER the real atomic
// save completed (rename and directory fsync done: the new file is fully
// reachable) but BEFORE Submit releases the exclusive lock and returns its
// success result. That is exactly the window in which the save is complete
// while the submitter has not yet processed success.
type batchSaveGate struct {
	mu      sync.Mutex
	entered chan struct{} // closed once the writer is parked after the finished save
	release chan struct{} // closed to let the writer finish Submit
}

func newBatchSaveGate(t *testing.T) *batchSaveGate {
	t.Helper()
	g := &batchSaveGate{}
	g.reset()
	orig := writeNodeFile
	writeNodeFile = func(path string, records []Heartbeat) error {
		err := orig(path, records) // real temp-file/fsync/rename/dir-fsync path
		g.mu.Lock()
		entered, release := g.entered, g.release
		g.mu.Unlock()
		close(entered)
		<-release
		return err
	}
	t.Cleanup(func() { writeNodeFile = orig })
	return g
}

func (g *batchSaveGate) reset() {
	g.mu.Lock()
	g.entered = make(chan struct{})
	g.release = make(chan struct{})
	g.mu.Unlock()
}

// holdSharedLock takes a shared flock on the directory lock file and returns
// a release function. While held, a Submit waits for the exclusive lock, so a
// query that completes in this interval genuinely overlaps a submit that is
// in flight and waiting — the deterministic pre-batch overlap window.
func holdSharedLock(t *testing.T, dir string) func() {
	t.Helper()
	path := filepath.Join(dir, "lock")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		t.Fatalf("open lock file: %v", err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_SH); err != nil {
		t.Fatalf("acquire shared lock: %v", err)
	}
	return func() {
		syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		f.Close()
	}
}

// assertExclusiveLocked proves, without sleeping, that the writer currently
// owns the exclusive lock: another non-blocking exclusive attempt must be
// refused by the kernel.
func assertExclusiveLocked(t *testing.T, dir string) {
	t.Helper()
	f, err := os.OpenFile(filepath.Join(dir, "lock"), os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		t.Fatalf("open lock file: %v", err)
	}
	defer f.Close()
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err == nil {
		syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		t.Fatal("expected the in-flight Submit to hold the exclusive lock, but a non-blocking lock attempt succeeded")
	}
}

// assertNotDone fails the test if a result that should be blocked by the lock
// window has already arrived. Completion is structurally impossible while the
// gate/lock conditions hold, so the non-blocking check is race-free and needs
// no timing margin.
func assertNotDone[T any](t *testing.T, ch <-chan T, what string) {
	t.Helper()
	select {
	case <-ch:
		t.Fatalf("%s completed inside the overlap window; the test did not actually overlap submit and query", what)
	default:
	}
}

// snapshotClassify judges one History result against the only two legal
// states for a query overlapping the batch:
//
//   - pre:    the exact complete history before this batch;
//   - post:   the exact complete history after the whole batch.
//
// A snapshot containing some but not all of the batch's new seqs is the
// forbidden half batch. A never-submitted seq, a duplicated or out-of-order
// seq, and a seq whose content differs from what was submitted are likewise
// rejected — completeness covers the records' original content, not just
// their count. The classification never consults the submitter's success
// flag: once the whole batch is durably saved, seeing it before the submitter
// has processed its result is legal.
//
// newSeqs is the set of seqs introduced by this batch; content maps every seq
// that may legitimately appear (pre and post) to its exact original record.
func snapshotClassify(t *testing.T, got, pre, post []Heartbeat, newSeqs map[int64]bool, content map[int64]Heartbeat) (bool, bool, string) {
	t.Helper()
	seen := map[int64]bool{}
	gotSeqs := make([]int64, 0, len(got))
	for i, r := range got {
		gotSeqs = append(gotSeqs, r.Seq)
		if seen[r.Seq] {
			return false, false, fmt.Sprintf("duplicate seq %d in query result %v", r.Seq, gotSeqs)
		}
		seen[r.Seq] = true
		if i > 0 && got[i-1].Seq >= r.Seq {
			return false, false, fmt.Sprintf("history not strictly ascending by seq: %v", gotSeqs)
		}
		want, ok := content[r.Seq]
		if !ok {
			return false, false, fmt.Sprintf("query result contains seq %d that was never submitted: %v", r.Seq, gotSeqs)
		}
		if !r.Equal(want) {
			return false, false, fmt.Sprintf("seq %d returned with content %+v, want its original content %+v", r.Seq, r, want)
		}
	}

	present := 0
	for seq := range newSeqs {
		if seen[seq] {
			present++
		}
	}
	switch present {
	case 0:
		if !historiesEqual(got, pre) {
			return false, false, fmt.Sprintf("none of the new batch visible but result is not the complete pre-batch history: got %v", gotSeqs)
		}
		return true, false, ""
	case len(newSeqs):
		if !historiesEqual(got, post) {
			return false, false, fmt.Sprintf("all new seqs visible but result is not the complete post-batch history: got %v", gotSeqs)
		}
		return false, true, ""
	default:
		missing := make([]int64, 0)
		for seq := range newSeqs {
			if !seen[seq] {
				missing = append(missing, seq)
			}
		}
		return false, false, fmt.Sprintf("HALF BATCH visible: %d of %d new records present (got %v), missing new seqs %v", present, len(newSeqs), gotSeqs, missing)
	}
}

func historiesEqual(a, b []Heartbeat) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !a[i].Equal(b[i]) {
			return false
		}
	}
	return true
}

// submitOutcome is the submitter's processed result.
type submitOutcome struct {
	newCount int
	dupCount int
	err      error
}

// runOverlapRound executes one deterministically overlapped backfill against
// the node and checks every rule described on this file:
//
//   - while Submit is waiting for the lock, a real concurrent query must
//     return the complete pre-batch history (empty history included);
//   - while Submit is parked with the save finished and the lock held,
//     concurrent queries cannot read a half state: they block and then all
//     return the complete post-batch history;
//   - the complete post-batch history is already readable after Submit
//     returns but BEFORE the submitter processes/registers its success
//     result, and that snapshot is legal;
//   - a query after the submitter processed success contains the whole batch;
//   - the submit reports exactly len(batch) new and zero duplicates.
//
// pre is the complete history before the batch (nil/empty for a node with no
// heartbeats yet), batch the submitted records in their given input order,
// and post the exact complete history that a saved query must show.
func runOverlapRound(t *testing.T, dir, node string, pre, batch, post []Heartbeat, newSeqs map[int64]bool, content map[int64]Heartbeat, receive time.Time, gate *batchSaveGate) {
	t.Helper()
	gate.reset()

	// A long-held shared read lock parks the writer before it can enter the
	// save critical section, while real queries proceed (shared locks share).
	releaseShared := holdSharedLock(t, dir)

	submitStarted := make(chan struct{}, 1)
	submitReturned := make(chan struct{}, 1)
	allowProcessResult := make(chan struct{})
	submitDone := make(chan submitOutcome, 1)
	go func() {
		submitter, err := OpenStore(dir)
		if err != nil {
			submitDone <- submitOutcome{err: err}
			return
		}
		submitStarted <- struct{}{}
		n, d, err := submitter.Submit(batch, receive)
		// The save is complete and the success result is in hand, but the
		// submitter has not "processed/registered" it yet — a distinct later
		// moment from save completion.
		submitReturned <- struct{}{}
		<-allowProcessResult
		submitDone <- submitOutcome{newCount: n, dupCount: d, err: err}
	}()
	<-submitStarted

	// Submit cannot finish while the shared lock is held and the gate is
	// closed; prove it is genuinely in flight behind the query.
	assertNotDone(t, submitDone, "Submit (while a read lock is held)")

	// A different caller using the same directory queries right now. Shared
	// locks coexist, so this query runs concurrently with the waiting Submit
	// and must return the complete pre-batch history.
	preCaller, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	gotPre, err := preCaller.History(node)
	if err != nil {
		t.Fatalf("query overlapping an in-flight submit must not fail: %v", err)
	}
	isPre, isPost, detail := snapshotClassify(t, gotPre, pre, post, newSeqs, content)
	if !isPre || isPost {
		t.Fatalf("query concurrent with a Submit waiting for the lock must show the complete pre-batch history; %s", detail)
	}
	assertNotDone(t, submitDone, "Submit (while the pre-batch query completes)")

	// Release the long read: the writer acquires the exclusive lock, merges
	// and reaches the parked point AFTER the whole file was atomically saved.
	releaseShared()
	<-gate.entered

	// The write lock really is held, and no one can read a half-saved file:
	// spawn independent callers (fresh Store instances, own lock handles).
	assertExclusiveLocked(t, dir)
	const readers = 6
	var readersStarted sync.WaitGroup
	results := make(chan []Heartbeat, readers)
	readErrs := make(chan error, readers)
	for i := 0; i < readers; i++ {
		readersStarted.Add(1)
		go func() {
			caller, err := OpenStore(dir)
			if err != nil {
				readErrs <- err
				readersStarted.Done()
				return
			}
			readersStarted.Done()
			// This blocks on the shared lock until the writer releases it.
			h, err := caller.History(node)
			if err != nil {
				readErrs <- err
				return
			}
			results <- h
		}()
	}
	readersStarted.Wait()

	// While the save is finished but the writer still holds the exclusive
	// lock, neither Submit nor any query may have produced a result yet.
	assertNotDone(t, submitDone, "Submit (while parked after the finished save)")
	select {
	case h := <-results:
		t.Fatalf("a reader read %v while the writer still held the exclusive lock; locking does not cover the save", h)
	case err := <-readErrs:
		t.Fatalf("a reader failed while waiting for the in-flight save: %v", err)
	default:
	}

	// Let Submit finish and return; queued readers now drain. Every snapshot
	// must be the complete post-batch history — a half result here is the bug.
	close(gate.release)
	for i := 0; i < readers; i++ {
		select {
		case err := <-readErrs:
			t.Fatalf("query overlapping the submit failed: %v", err)
		case got := <-results:
			isPre, isPost, detail := snapshotClassify(t, got, pre, post, newSeqs, content)
			if isPre || !isPost {
				t.Fatalf("query unblocked after a finished save must show the complete post-batch history; %s", detail)
			}
		}
	}

	// Submit has returned but its success result is not processed yet. A new
	// caller must already see the fully saved batch, and that must be treated
	// as legitimate — never as heartbeats that were never submitted.
	<-submitReturned
	midCaller, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	gotMid, err := midCaller.History(node)
	if err != nil {
		t.Fatalf("query after save completion but before result processing must not fail: %v", err)
	}
	isPre, isPost, detail = snapshotClassify(t, gotMid, pre, post, newSeqs, content)
	if isPre || !isPost {
		t.Fatalf("fully saved batch must be visible (and legal) before the submitter processes its success result; %s", detail)
	}

	// Let the submitter register its success, then verify its public counts.
	close(allowProcessResult)
	outcome := <-submitDone
	if outcome.err != nil {
		t.Fatalf("Submit failed: %v", outcome.err)
	}
	if outcome.newCount != len(batch) || outcome.dupCount != 0 {
		t.Fatalf("Submit counts = new %d dup %d, want new %d dup 0", outcome.newCount, outcome.dupCount, len(batch))
	}

	// A query issued only after the successful submit returned must contain
	// the whole batch, with every record's original content.
	afterCaller, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	gotAfter, err := afterCaller.History(node)
	if err != nil {
		t.Fatalf("post-commit query failed: %v", err)
	}
	isPre, isPost, detail = snapshotClassify(t, gotAfter, pre, post, newSeqs, content)
	if isPre || !isPost {
		t.Fatalf("query after successful Submit must contain the whole batch; %s", detail)
	}
}

// TestConcurrentBatchSubmitQueryVisibility is the headline regression: a node
// that already owns seq 1 gets one backfill per round of three new heartbeats
// whose INPUT ORDER differs from seq order (9, 3, 6 first, other shuffles
// later). Queries are forced to overlap each submit, and every snapshot must
// be one of the two complete histories.
func TestConcurrentBatchSubmitQueryVisibility(t *testing.T) {
	dir := t.TempDir()
	receive := testBase
	node := "node-alpha"

	// Seed the original seq-1 record before installing the save gate.
	seeder, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	origin := batchRecord(node, 1, receive)
	if _, _, err := seeder.Submit([]Heartbeat{origin}, receive); err != nil {
		t.Fatal(err)
	}

	gate := newBatchSaveGate(t)

	pre := []Heartbeat{origin}
	content := map[int64]Heartbeat{1: origin}
	const rounds = 50
	for round := 0; round < rounds; round++ {
		// Disjoint, gap-tolerant seq bands: 3/6/9, 13/16/19, ...
		base := int64(round*10 + 3)
		seqs := []int64{base, base + 3, base + 6}
		records := []Heartbeat{
			batchRecord(node, seqs[0], receive),
			batchRecord(node, seqs[1], receive),
			batchRecord(node, seqs[2], receive),
		}
		for _, r := range records {
			content[r.Seq] = r
		}
		// Input order deliberately differs from ascending seq order; rotate
		// the shuffle so no fixed order is assumed.
		var batch []Heartbeat
		switch round % 3 {
		case 0:
			batch = []Heartbeat{records[2], records[0], records[1]} // 9, 3, 6
		case 1:
			batch = []Heartbeat{records[2], records[1], records[0]} // 9, 6, 3
		default:
			batch = []Heartbeat{records[1], records[2], records[0]} // 6, 9, 3
		}

		post := append(append([]Heartbeat{}, pre...), records...)
		sortBySeq(post)
		newSeqs := map[int64]bool{}
		for _, r := range records {
			newSeqs[r.Seq] = true
		}

		runOverlapRound(t, dir, node, pre, batch, post, newSeqs, content, receive, gate)
		pre = post
	}

	// Final independent verification through the public query entry: the
	// whole history is present once, ascending and content-exact.
	checker, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	final, err := checker.History(node)
	if err != nil {
		t.Fatal(err)
	}
	if len(final) != 1+3*rounds {
		t.Fatalf("final history has %d records, want %d", len(final), 1+3*rounds)
	}
	for i, r := range final {
		want := content[r.Seq]
		if !r.Equal(want) {
			t.Fatalf("final record %d = %+v, want %+v", i, r, want)
		}
		if i > 0 && final[i-1].Seq >= r.Seq {
			t.Fatalf("final history not ascending at position %d: %d then %d", i, final[i-1].Seq, r.Seq)
		}
	}
}

// TestConcurrentFirstBackfillOnEmptyNodeVisibility covers a node with no
// heartbeats at all: the first backfill overlapping a query may surface as
// the existing EMPTY history or as the complete sorted batch, never a partial
// batch, and the empty result is an ordinary successful query — not
// corruption and not a failure.
func TestConcurrentFirstBackfillOnEmptyNodeVisibility(t *testing.T) {
	dir := t.TempDir()
	receive := testBase
	node := "node-fresh"

	gate := newBatchSaveGate(t)

	seqs := []int64{9, 3, 6}
	records := []Heartbeat{
		batchRecord(node, 3, receive),
		batchRecord(node, 6, receive),
		batchRecord(node, 9, receive),
	}
	content := map[int64]Heartbeat{}
	for _, r := range records {
		content[r.Seq] = r
	}
	batch := []Heartbeat{
		batchRecord(node, seqs[0], receive), // 9
		batchRecord(node, seqs[1], receive), // 3
		batchRecord(node, seqs[2], receive), // 6
	}
	post := append([]Heartbeat{}, records...)
	sortBySeq(post) // 3, 6, 9
	newSeqs := map[int64]bool{3: true, 6: true, 9: true}

	// Sanity: before anything is saved, history is a non-error empty result.
	preCaller, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	empty, err := preCaller.History(node)
	if err != nil {
		t.Fatalf("querying a node with no heartbeats must not fail: %v", err)
	}
	if len(empty) != 0 {
		t.Fatalf("new node history = %v, want empty", empty)
	}

	runOverlapRound(t, dir, node, []Heartbeat{}, batch, post, newSeqs, content, receive, gate)

	// After the commit, ascending 3/6/9 with original content and no dupes.
	after, err := preCaller.History(node)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != 3 || after[0].Seq != 3 || after[1].Seq != 6 || after[2].Seq != 9 {
		t.Fatalf("first backfill final history = %+v, want ascending 3,6,9", after)
	}
	for _, r := range after {
		if !r.Equal(content[r.Seq]) {
			t.Fatalf("first backfill record seq %d content changed: %+v", r.Seq, r)
		}
	}
}

// sortBySeq orders records ascending by seq in place (post-state shape).
func sortBySeq(records []Heartbeat) {
	for i := 1; i < len(records); i++ {
		for j := i; j > 0 && records[j-1].Seq > records[j].Seq; j-- {
			records[j-1], records[j] = records[j], records[j-1]
		}
	}
}

// forgeNodeFile writes a well-formed, checksum-valid node file directly to the
// node's storage path. It bypasses Submit and its lock on purpose: the point
// is that a file containing only part of a batch is a perfectly valid stored
// file — the checksum protects bytes, not batch boundaries — so the
// regression's classifier must be what recognises the half batch.
func forgeNodeFile(t *testing.T, store *Store, node string, records []Heartbeat) {
	t.Helper()
	if records == nil {
		records = []Heartbeat{}
	}
	nf := nodeFile{
		Format:   formatMarker,
		Checksum: checksumRecords(records),
		Records:  records,
	}
	data, err := json.MarshalIndent(nf, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(store.nodeLocationFor(node).path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

// TestHalfBatchSnapshotClassifierRejectsForgedPartialFiles proves the
// classifier rejects every shape a torn batch visibility could take, even
// though each forged file passes the store's own checksum and structural
// verification (History returns it without error): part of the new batch
// only, a partial first backfill on an empty node, a replaced old record, and
// an unannounced extra seq.
func TestHalfBatchSnapshotClassifierRejectsForgedPartialFiles(t *testing.T) {
	dir := t.TempDir()
	receive := testBase
	node := "node-forged"
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	origin := batchRecord(node, 1, receive)
	r3 := batchRecord(node, 3, receive)
	r6 := batchRecord(node, 6, receive)
	r9 := batchRecord(node, 9, receive)
	forgeNodeFile(t, store, node, []Heartbeat{origin})

	pre := []Heartbeat{origin}
	post := []Heartbeat{origin, r3, r6, r9}
	newSeqs := map[int64]bool{3: true, 6: true, 9: true}
	content := map[int64]Heartbeat{1: origin, 3: r3, 6: r6, 9: r9}

	cases := []struct {
		name    string
		forged  []Heartbeat
		wantSub string
	}{
		{"one of three new records", []Heartbeat{origin, r3}, "HALF BATCH"},
		{"two of three new records", []Heartbeat{origin, r3, r6}, "HALF BATCH"},
		{"new records without the old one", []Heartbeat{r3, r6, r9}, "post-batch history"},
		{"old record replaced by different content", []Heartbeat{
			hb(node, 1, receive.Add(-30*time.Second), "v9.9", 1, 1), r3, r6, r9,
		}, "content"},
		{"fully saved batch plus an unannounced seq", []Heartbeat{
			origin, r3, r6, r9, batchRecord(node, 12, receive),
		}, "never submitted"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			forgeNodeFile(t, store, node, tc.forged)
			caller, err := OpenStore(dir)
			if err != nil {
				t.Fatal(err)
			}
			got, err := caller.History(node)
			if err != nil {
				t.Fatalf("forged file is checksum-valid and must load, got %v", err)
			}
			isPre, isPost, detail := snapshotClassify(t, got, pre, post, newSeqs, content)
			if isPre || isPost {
				t.Fatalf("classifier accepted a non-whole snapshot %v", got)
			}
			if !strings.Contains(detail, tc.wantSub) {
				t.Fatalf("classifier detail = %q, want it to mention %q", detail, tc.wantSub)
			}
		})
	}

	// The complete pre and post snapshots themselves are accepted.
	for _, whole := range [][]Heartbeat{pre, post} {
		forgeNodeFile(t, store, node, whole)
		caller, err := OpenStore(dir)
		if err != nil {
			t.Fatal(err)
		}
		got, err := caller.History(node)
		if err != nil {
			t.Fatal(err)
		}
		if isPre, isPost, detail := snapshotClassify(t, got, pre, post, newSeqs, content); detail != "" || !(isPre || isPost) {
			t.Fatalf("whole snapshot rejected: %s", detail)
		}
	}
}

// TestUnlockedStagedWriterHalfBatchIsDetected drives a deliberately broken
// writer that bypasses the lock and exposes each stage of a multi-record
// save (first one record, then two, then three) via the same atomic file
// primitive. A real caller reads while the broken writer is parked mid-batch,
// so this is genuine concurrent partial visibility — and the regression must
// flag it. The correct implementation never exposes these stages because the
// flock keeps readers out of the whole save critical section.
func TestUnlockedStagedWriterHalfBatchIsDetected(t *testing.T) {
	dir := t.TempDir()
	receive := testBase
	node := "node-staged"
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	origin := batchRecord(node, 1, receive)
	r3 := batchRecord(node, 3, receive)
	r6 := batchRecord(node, 6, receive)
	r9 := batchRecord(node, 9, receive)
	forgeNodeFile(t, store, node, []Heartbeat{origin})

	pre := []Heartbeat{origin}
	post := []Heartbeat{origin, r3, r6, r9}
	newSeqs := map[int64]bool{3: true, 6: true, 9: true}
	content := map[int64]Heartbeat{1: origin, 3: r3, 6: r6, 9: r9}
	loc := store.nodeLocationFor(node)

	stageReached := make(chan struct{}, 2)
	proceed := make(chan struct{})
	stagedDone := make(chan struct{})
	go func() {
		defer close(stagedDone)
		// Stage 1: only seq 3 visible.
		writeNodeFile(loc.path, []Heartbeat{origin, r3})
		stageReached <- struct{}{}
		<-proceed
		// Stage 2: seqs 3 and 6 visible.
		writeNodeFile(loc.path, []Heartbeat{origin, r3, r6})
		stageReached <- struct{}{}
		<-proceed
		// Stage 3: the complete batch.
		writeNodeFile(loc.path, post)
	}()

	for stage := 1; stage <= 2; stage++ {
		<-stageReached
		// A different caller reads while the broken batch is genuinely open:
		// no lock is held, so the intermediate state is reachable.
		caller, err := OpenStore(dir)
		if err != nil {
			t.Fatal(err)
		}
		got, err := caller.History(node)
		if err != nil {
			t.Fatalf("intermediate staged file must load, got %v", err)
		}
		isPre, isPost, detail := snapshotClassify(t, got, pre, post, newSeqs, content)
		if isPre || isPost {
			t.Fatalf("stage %d: concurrent reader saw a half batch %v but the classifier accepted it", stage, got)
		}
		if !strings.Contains(detail, "HALF BATCH") {
			t.Fatalf("stage %d: detail = %q, want HALF BATCH", stage, detail)
		}
		proceed <- struct{}{}
	}

	// After the broken writer finishes, the complete state must read as post.
	<-stagedDone
	caller, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	got, err := caller.History(node)
	if err != nil {
		t.Fatal(err)
	}
	if isPre, isPost, _ := snapshotClassify(t, got, pre, post, newSeqs, content); isPre || !isPost {
		t.Fatalf("final staged state must be the complete post-batch history, got %v", got)
	}
}
