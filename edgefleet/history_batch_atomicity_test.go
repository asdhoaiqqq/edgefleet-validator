package edgefleet

import (
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"
)

// Regression guard for batch-visible atomicity of history queries.
//
// Product rule being protected: several callers share one local data
// directory. While one node's reconnect backfill of several legal heartbeats
// is being committed, every other caller's History query must observe EITHER
// the complete pre-backfill history OR the complete history after the whole
// batch landed — never a state containing only some of the batch's new seqs.
// Each observed record must carry its own original content; previously saved
// records must not vanish or be replaced; the new records must not be
// duplicated. Different queries may land on different sides of the commit,
// but each individual answer must be complete. A query issued after the
// submit returned successfully must contain every new heartbeat.
//
// This property cannot be established by checking only the final history
// after all goroutines stop: a store that wrote records one at a time and let
// readers observe intermediate states would still finish with the correct
// final file. Nor can it be established probabilistically by "many goroutines,
// many loops" — on a fast machine readers might never hit the save window, and
// an observed post-state snapshot taken while the submitter has not yet
// processed its success result must NOT be misread as "a heartbeat that was
// never submitted" (the file is fully saved at rename time; the submitter
// registering the success counters is a later, separate moment).
//
// The overlap here is therefore constructed, not hoped for, and every
// synchronization point is a channel or the kernel flock — no wall-clock
// sleep participates in any assertion:
//
//   - Pre side, guaranteed: the test holds the exact shared flock a real
//     History holds (store.lock(false)) and starts the submitting goroutine;
//     the kernel parks its LOCK_EX request for as long as any shared lock is
//     held. Real History calls on their own Store instances then run while
//     that Submit is genuinely in flight (queued on the lock) and are
//     guaranteed to read the pre-batch file.
//   - Commit window, guaranteed: writeNodeFile — the same package-level seam
//     used by the partial-persistence tests, wrapping the real atomic
//     write/fsync/rename/dir-sync — parks the writer AFTER it holds LOCK_EX,
//     inside the commit. Reader goroutines launched then overlap the
//     uncommitted save. Releasing the gate finishes the save; every queued
//     reader then sees the complete post-batch file (rename is atomic and the
//     exclusive lock is held until Submit returns).
//   - Classification never consults the submitter's success: a snapshot equal
//     to the complete post state is legal even if the submitting goroutine
//     has not returned yet; only a genuinely partial set is a failure.
//
// The same cross-process flock object is what independent OS processes
// contend on (covered end-to-end by the CLI concurrent-submit tests); these
// tests pin the query-side atomicity semantics deterministically.

// atomicBackfillGate stalls the writer at one point inside a real
// writeNodeFile call while it holds the exclusive directory lock.
type atomicBackfillGate struct {
	reached chan struct{} // closed once the writer has entered writeNodeFile holding LOCK_EX
	release chan struct{} // closed to let the commit finish
	once    sync.Once
}

func newAtomicBackfillGate() *atomicBackfillGate {
	return &atomicBackfillGate{reached: make(chan struct{}), release: make(chan struct{})}
}

// install replaces writeNodeFile for one round: the first call parks between
// signalling "commit in progress" and performing the real persistence; every
// call still performs the genuine atomic write.
func (g *atomicBackfillGate) install(t *testing.T) {
	t.Helper()
	orig := writeNodeFile
	writeNodeFile = func(path string, records []Heartbeat) error {
		g.once.Do(func() { close(g.reached) })
		<-g.release
		return orig(path, records)
	}
	t.Cleanup(func() { writeNodeFile = orig })
}

// waitReached fails the round if the writer does not reach the commit gate
// promptly. The timeout only detects a deadlocked harness; no passing
// assertion depends on timing — the channel is closed from inside the real
// write path.
func (g *atomicBackfillGate) waitReached(t *testing.T, where string) {
	t.Helper()
	select {
	case <-g.reached:
	case <-time.After(5 * time.Second):
		t.Fatalf("%s: writer never reached the commit gate", where)
	}
}

// checkAtomicSnapshot is the single verdict for one History answer observed
// while one backfill batch is being committed. pre and post are the exact
// record sets (keyed by seq) immediately before and after that batch; post is
// pre plus the batch's new records.
//
// The snapshot is legal only when it equals the complete pre set or the
// complete post set, in strictly ascending seq order, with every record
// byte-for-byte equal (collection instant, version, height, cumulative missed
// count and node id) to the content of that seq. Notably the verdict has no
// input for "has the submitter processed its success result yet": a complete
// post-batch snapshot is legal as soon as the file landed, before the
// submitting caller registers success. A set containing some but not all of
// the batch's new seqs, a snapshot missing pre-batch records, a seq that is
// in neither set, duplicated/unordered seqs, or altered record content is a
// failure.
func checkAtomicSnapshot(got []Heartbeat, pre, post map[int64]Heartbeat) error {
	seen := make(map[int64]Heartbeat, len(got))
	var prevSeq int64
	for i, r := range got {
		want, ok := post[r.Seq]
		if !ok {
			return fmt.Errorf("query exposed seq %d which is neither in the pre-batch history nor part of the submitted batch (a heartbeat that was never saved)", r.Seq)
		}
		if !r.Equal(want) {
			return fmt.Errorf("record for seq %d carries different content than the originally submitted heartbeat: got %+v want %+v", r.Seq, r, want)
		}
		if i > 0 && r.Seq <= prevSeq {
			return fmt.Errorf("history is not strictly ascending/duplicate-free at seq %d: %v", r.Seq, seqListOf(got))
		}
		prevSeq = r.Seq
		seen[r.Seq] = r
	}

	matches := func(want map[int64]Heartbeat) bool {
		if len(seen) != len(want) {
			return false
		}
		for seq := range want {
			if _, ok := seen[seq]; !ok {
				return false
			}
		}
		return true
	}
	if matches(pre) || matches(post) {
		return nil
	}

	// Neither complete state: name the exact violation so a real half-batch
	// leak cannot be confused with anything else.
	var lost, present, missing []int64
	for seq := range pre {
		if _, ok := seen[seq]; !ok {
			lost = append(lost, seq)
		}
	}
	for seq := range post {
		if _, isPre := pre[seq]; isPre {
			continue
		}
		if _, ok := seen[seq]; ok {
			present = append(present, seq)
		} else {
			missing = append(missing, seq)
		}
	}
	sortInts(lost)
	sortInts(present)
	sortInts(missing)
	switch {
	case len(lost) > 0:
		return fmt.Errorf("query lost previously saved record(s) %v that the backfill must keep; observed seqs %v", lost, seqListOf(got))
	case len(present) > 0 && len(missing) > 0:
		return fmt.Errorf("query exposed a HALF BATCH: new seq(s) %v visible but %v missing; a query must show the whole backfill batch or none of it; observed seqs %v", present, missing, seqListOf(got))
	default:
		return fmt.Errorf("query snapshot is neither the complete pre-batch nor the complete post-batch history; observed seqs %v", seqListOf(got))
	}
}

func seqListOf(records []Heartbeat) []int64 {
	out := make([]int64, len(records))
	for i, r := range records {
		out[i] = r.Seq
	}
	return out
}

func sortInts(xs []int64) { sort.Slice(xs, func(i, j int) bool { return xs[i] < xs[j] }) }

func seqSetOf(records []Heartbeat) map[int64]Heartbeat {
	m := make(map[int64]Heartbeat, len(records))
	for _, r := range records {
		m[r.Seq] = r
	}
	return m
}

// backfillRecords builds the scenario from the task: an existing seq 1 and a
// one-shot backfill of seq 9, 3, 6 submitted in an order different from seq
// order. Each seq carries distinct, fixed telemetry so a swapped or
// half-merged record cannot pass the content comparison.
func backfillRecords(node string) (existing, batch []Heartbeat, bySeq map[int64]Heartbeat) {
	r1 := hb(node, 1, testBase.Add(-9*time.Minute), "1.25.0", 101, 1)
	r3 := hb(node, 3, testBase.Add(-7*time.Minute), "1.26.0", 103, 3)
	r6 := hb(node, 6, testBase.Add(-4*time.Minute), "1.27.0", 106, 6)
	r9 := hb(node, 9, testBase.Add(-1*time.Minute), "1.28.0", 109, 9)
	existing = []Heartbeat{r1}
	batch = []Heartbeat{r9, r3, r6} // deliberately: input order 9, 3, 6
	bySeq = map[int64]Heartbeat{1: r1, 3: r3, 6: r6, 9: r9}
	return
}

// runAtomicBackfillRound executes one fully deterministic overlap round
// against a fresh directory and checks every observed snapshot and the final
// committed state. prior may be empty for a node's first-ever backfill.
func runAtomicBackfillRound(t *testing.T, round int, node string, prior, batch []Heartbeat) {
	t.Helper()
	dir := t.TempDir()

	seeder, err := OpenStore(dir)
	if err != nil {
		t.Fatalf("round %d: %v", round, err)
	}
	if len(prior) > 0 {
		if n, d, err := seeder.Submit(prior, testBase); err != nil || n != len(prior) || d != 0 {
			t.Fatalf("round %d: seed submit: n=%d d=%d err=%v", round, n, d, err)
		}
	}

	pre := seqSetOf(prior)
	post := seqSetOf(append(append([]Heartbeat{}, prior...), batch...))

	gate := newAtomicBackfillGate()
	gate.install(t)

	// A separate Store holds the shared lock exactly the way a real in-flight
	// History query holds it while reading.
	queryInFlight, err := OpenStore(dir)
	if err != nil {
		t.Fatalf("round %d: %v", round, err)
	}
	releaseReadLock, err := queryInFlight.lock(false)
	if err != nil {
		t.Fatalf("round %d: acquire shared lock: %v", round, err)
	}

	// Writer side: one Submit call for the whole backfill. With the shared
	// lock held, the kernel parks its LOCK_EX request before any file is
	// touched.
	submitter, err := OpenStore(dir)
	if err != nil {
		t.Fatalf("round %d: %v", round, err)
	}
	type submitOutcome struct {
		n, d int
		err  error
	}
	done := make(chan submitOutcome, 1)
	go func() {
		n, d, err := submitter.Submit(batch, testBase)
		done <- submitOutcome{n: n, d: d, err: err}
	}()

	// Pre side: these real History calls overlap the Submit call (it is
	// queued on the held shared lock) and are guaranteed to observe the
	// complete pre-batch state.
	var (
		mu        sync.Mutex
		snapshots [][]Heartbeat
		wg        sync.WaitGroup
	)
	observe := func(store *Store) {
		defer wg.Done()
		hist, qerr := store.History(node)
		if qerr != nil {
			t.Errorf("round %d: overlapping history query failed: %v", round, qerr)
			return
		}
		mu.Lock()
		snapshots = append(snapshots, hist)
		mu.Unlock()
	}
	for i := 0; i < 3; i++ {
		reader, err := OpenStore(dir)
		if err != nil {
			t.Fatalf("round %d: %v", round, err)
		}
		wg.Add(1)
		observe(reader)
	}

	// Let the writer into its critical section. It takes LOCK_EX, merges, and
	// parks inside writeNodeFile (the gate) with the lock held — the commit is
	// provably in flight before post-side readers start.
	releaseReadLock()
	gate.waitReached(t, fmt.Sprintf("round %d", round))

	// Post side: launched while the writer is parked mid-commit holding
	// LOCK_EX. Their shared-lock requests overlap the in-flight save and are
	// served only once the whole atomic replacement is complete, so each is a
	// guaranteed complete post-batch answer.
	for i := 0; i < 8; i++ {
		reader, err := OpenStore(dir)
		if err != nil {
			t.Fatalf("round %d: %v", round, err)
		}
		wg.Add(1)
		go observe(reader)
	}

	close(gate.release)

	outcome := <-done
	if outcome.err != nil {
		t.Fatalf("round %d: backfill submit failed: %v", round, outcome.err)
	}
	if outcome.n != len(batch) || outcome.d != 0 {
		t.Errorf("round %d: submit counts new=%d duplicate=%d, want new=%d duplicate=0", round, outcome.n, outcome.d, len(batch))
	}
	wg.Wait()

	// Every overlapping answer must be one complete state or the other, with
	// exact per-record content.
	sawPre, sawPost := false, false
	for i, snap := range snapshots {
		if err := checkAtomicSnapshot(snap, pre, post); err != nil {
			t.Errorf("round %d snapshot %d: %v", round, i, err)
			continue
		}
		switch len(snap) {
		case len(pre):
			sawPre = true
		case len(post):
			sawPost = true
		}
	}
	if !sawPre {
		t.Errorf("round %d: no overlapping query observed the complete pre-batch state", round)
	}
	if !sawPost {
		t.Errorf("round %d: no overlapping query observed the complete post-batch state", round)
	}

	// After Submit returned successfully, a new caller must see all new
	// heartbeats, ascending, content-exact, with the original record intact.
	after, err := OpenStore(dir)
	if err != nil {
		t.Fatalf("round %d: %v", round, err)
	}
	final, err := after.History(node)
	if err != nil {
		t.Fatalf("round %d: post-commit history failed: %v", round, err)
	}
	if err := checkAtomicSnapshot(final, pre, post); err != nil {
		t.Fatalf("round %d: post-commit history: %v", round, err)
	}
	if len(final) != len(post) {
		t.Fatalf("round %d: post-commit history has %d records, want %d", round, len(final), len(post))
	}
}

// TestHistoryAtomicAcrossReconnectBackfill is the exact task scenario,
// repeated against fresh directories: existing seq 1; one backfill submit of
// seq 9, 3, 6 (input order differs from seq order); queries overlapping that
// submit may answer only [1] or ascending [1,3,6,9], each record carrying its
// own collected_at/version/height/missed values.
func TestHistoryAtomicAcrossReconnectBackfill(t *testing.T) {
	const rounds = 4
	for round := 1; round <= rounds; round++ {
		node := fmt.Sprintf("backfill-node-r%d", round)
		existing, batch, _ := backfillRecords(node)
		runAtomicBackfillRound(t, round, node, existing, batch)
	}
}

// TestHistoryAtomicAcrossFirstBackfill covers a node that had no heartbeats at
// all: queries overlapping its first-ever backfill may return the existing
// empty history (not a failure and not "corruption while waiting for the
// save") or the complete batch in seq order — never part of the new records.
func TestHistoryAtomicAcrossFirstBackfill(t *testing.T) {
	const rounds = 4
	for round := 1; round <= rounds; round++ {
		node := fmt.Sprintf("fresh-node-r%d", round)
		_, batch, _ := backfillRecords(node)
		runAtomicBackfillRound(t, round, node, nil, batch)
	}
}

// TestAtomicSnapshotVerdict pins the classifier itself: it must accept both
// complete states (post even before the submitter has processed success),
// reject every half-batch shape, lost pre-records, unknown seqs, ordering
// violations and altered content, and do the same for an initially empty
// node. This is the negative control proving a genuine half-batch exposure
// would be reported, while a save-complete-but-success-not-yet-registered
// answer is not mistaken for an unsubmitted heartbeat.
func TestAtomicSnapshotVerdict(t *testing.T) {
	_, _, warmBySeq := backfillRecords("warm")
	warmPre := map[int64]Heartbeat{1: warmBySeq[1]}
	warmPost := warmBySeq
	warmRecords := func(seqs ...int64) []Heartbeat {
		out := make([]Heartbeat, 0, len(seqs))
		for _, seq := range seqs {
			out = append(out, warmBySeq[seq])
		}
		return out
	}

	type verdictCase struct {
		name    string
		pre     map[int64]Heartbeat
		post    map[int64]Heartbeat
		got     []Heartbeat
		wantErr bool
	}
	cases := []verdictCase{
		{"warm pre state", warmPre, warmPost, warmRecords(1), false},
		{"warm post state before submitter processed success", warmPre, warmPost, warmRecords(1, 3, 6, 9), false},
		{"warm half batch just seq 3", warmPre, warmPost, warmRecords(1, 3), true},
		{"warm half batch seqs 3 and 6", warmPre, warmPost, warmRecords(1, 3, 6), true},
		{"warm non-prefix half batch", warmPre, warmPost, warmRecords(1, 6, 9), true},
		{"warm only the last new seq", warmPre, warmPost, warmRecords(1, 9), true},
		{"warm snapshot lost original seq 1", warmPre, warmPost, warmRecords(3, 6, 9), true},
		{"warm empty snapshot after history existed", warmPre, warmPost, nil, true},
		{
			"warm seq never part of the batch",
			warmPre, warmPost,
			// A complete-looking answer that smuggles in an unsubmitted seq.
			[]Heartbeat{
				warmBySeq[1], warmBySeq[3], warmBySeq[6], warmBySeq[9],
				hb("warm", 12, testBase.Add(-30*time.Second), "1.28.0", 112, 12),
			},
			true,
		},
		{"warm duplicated seq in snapshot", warmPre, warmPost, append(warmRecords(1, 3, 6), warmBySeq[3], warmBySeq[9]), true},
		{
			"warm record content replaced",
			warmPre, warmPost,
			[]Heartbeat{
				warmBySeq[1],
				hb("warm", 3, warmBySeq[3].CollectedAt, "9.9.9", warmBySeq[3].Height, warmBySeq[3].Missed),
				warmBySeq[6], warmBySeq[9],
			},
			true,
		},
	}

	// Mirror for the node with no prior history: empty history and the whole
	// first batch are both legal; any non-empty proper subset is not.
	_, freshBatch, freshBySeq := backfillRecords("fresh")
	freshPre := map[int64]Heartbeat{}
	freshPost := seqSetOf(freshBatch)
	freshRecords := func(seqs ...int64) []Heartbeat {
		out := make([]Heartbeat, 0, len(seqs))
		for _, seq := range seqs {
			out = append(out, freshBySeq[seq])
		}
		return out
	}
	cases = append(cases,
		verdictCase{"fresh empty pre state", freshPre, freshPost, nil, false},
		verdictCase{"fresh full post state", freshPre, freshPost, freshRecords(3, 6, 9), false},
		verdictCase{"fresh only first new seq", freshPre, freshPost, freshRecords(3), true},
		verdictCase{"fresh half batch", freshPre, freshPost, freshRecords(3, 6), true},
		verdictCase{"fresh non-prefix half batch", freshPre, freshPost, freshRecords(3, 9), true},
	)

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := checkAtomicSnapshot(tc.got, tc.pre, tc.post)
			if tc.wantErr && err == nil {
				t.Errorf("partial/illegal snapshot %v accepted as atomic", seqListOf(tc.got))
			}
			if !tc.wantErr && err != nil {
				t.Errorf("complete snapshot %v rejected: %v", seqListOf(tc.got), err)
			}
		})
	}
}
