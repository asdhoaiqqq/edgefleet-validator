package edgefleet

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func openTestStore(t *testing.T) *Store {
	t.Helper()
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

var t0 = time.Date(2026, 10, 1, 12, 0, 0, 0, time.FixedZone("+08:00", 8*3600))

func rec(node string, seq, height, missed int64, collOffsetSec int) Heartbeat {
	return Heartbeat{
		NodeID:      node,
		Seq:         seq,
		CollectedAt: t0.Add(time.Duration(collOffsetSec) * time.Second),
		Version:     "1.26.0",
		Height:      height,
		Missed:      missed,
	}
}

func TestReceiveInsertDuplicateConflict(t *testing.T) {
	s := openTestStore(t)

	ins, dup, err := s.Receive([]Heartbeat{rec("a", 1, 100, 0, 0), rec("a", 3, 103, 0, 2)}, t0.Add(10*time.Second))
	if err != nil || ins != 2 || dup != 0 {
		t.Fatalf("first batch: ins=%d dup=%d err=%v", ins, dup, err)
	}

	// Same content again: both duplicates, no error.
	ins, dup, err = s.Receive([]Heartbeat{rec("a", 1, 100, 0, 0), rec("a", 3, 103, 0, 2)}, t0.Add(20*time.Second))
	if err != nil || ins != 0 || dup != 2 {
		t.Fatalf("dup batch: ins=%d dup=%d err=%v", ins, dup, err)
	}

	// Same key different height: conflict, whole batch rejected.
	_, _, err = s.Receive([]Heartbeat{
		rec("a", 1, 100, 0, 0),
		rec("a", 3, 999, 0, 2), // conflicts
	}, t0.Add(30*time.Second))
	var ce *ConflictError
	if !errors.As(err, &ce) || ce.Seq != 3 {
		t.Fatalf("want ConflictError seq=3, got %v", err)
	}
}

func TestReceiveBatchInternalDuplicateAndConflict(t *testing.T) {
	s := openTestStore(t)

	// Identical repeat inside the same batch: one insert, one duplicate.
	ins, dup, err := s.Receive([]Heartbeat{rec("a", 1, 100, 0, 0), rec("a", 1, 100, 0, 0)}, t0)
	if err != nil || ins != 1 || dup != 1 {
		t.Fatalf("internal dup: ins=%d dup=%d err=%v", ins, dup, err)
	}

	// Same key, differing content inside one batch: conflict, nothing writes.
	_, _, err = s.Receive([]Heartbeat{
		rec("b", 5, 1, 0, 0),
		rec("b", 5, 2, 0, 0),
	}, t0)
	var ce *ConflictError
	if !errors.As(err, &ce) {
		t.Fatalf("want conflict, got %v", err)
	}
	if got := s.History("b"); len(got) != 0 {
		t.Fatalf("conflicted batch must not create node b, got %v", got)
	}
}

func TestInvalidBatchIsAtomicAndDoesNotCreateNode(t *testing.T) {
	s := openTestStore(t)
	bad := rec("new", 2, 5, 0, 0)
	bad.Missed = -4
	_, _, err := s.Receive([]Heartbeat{rec("new", 1, 5, 0, 0), bad}, t0)
	var ie *InvalidRecordError
	if !errors.As(err, &ie) || ie.Index != 1 {
		t.Fatalf("want InvalidRecordError index=1, got %v", err)
	}
	if s.Latest("new") != nil {
		t.Fatal("invalid batch must not create the node or insert record 1")
	}
	if len(s.Nodes()) != 0 {
		t.Fatalf("no nodes should exist, got %v", s.Nodes())
	}
}

func TestDifferentNodesReuseSeq(t *testing.T) {
	s := openTestStore(t)
	if _, _, err := s.Receive([]Heartbeat{rec("a", 1, 1, 0, 0), rec("b", 1, 2, 0, 0)}, t0); err != nil {
		t.Fatal(err)
	}
	if got := s.Latest("a"); got.Height != 1 {
		t.Fatalf("a height=%d", got.Height)
	}
	if got := s.Latest("b"); got.Height != 2 {
		t.Fatalf("b height=%d", got.Height)
	}
}

func TestLatestUsesMaxSeqNotArrival(t *testing.T) {
	s := openTestStore(t)
	// Arrive out of order: seq 10 first, then late seq 4...
	if _, _, err := s.Receive([]Heartbeat{rec("a", 10, 110, 0, 10)}, t0.Add(20*time.Second)); err != nil {
		t.Fatal(err)
	}
	// ...then an even later seq 11 arrives with OLDER collection time.
	old := rec("a", 11, 111, 0, -300) // collected 5 minutes before t0
	if _, _, err := s.Receive([]Heartbeat{old}, t0.Add(21*time.Second)); err != nil {
		t.Fatal(err)
	}
	got := s.Latest("a")
	if got.Seq != 11 {
		t.Fatalf("latest must be max seq 11, got %d", got.Seq)
	}
	// That max-seq record is stale telemetry: offline even though just received.
	st, err := s.Health("a", t0.Add(21*time.Second), "1.26.0", 0)
	if err != nil {
		t.Fatal(err)
	}
	if st.Online || st.Healthy {
		t.Fatalf("freshly back-filled stale telemetry must not be healthy: %+v", st)
	}
}

func TestHistoryAscendingUnique(t *testing.T) {
	s := openTestStore(t)
	batch := []Heartbeat{rec("a", 5, 5, 0, 5), rec("a", 2, 2, 0, 2), rec("a", 9, 9, 0, 9)}
	if _, _, err := s.Receive(batch, t0.Add(20*time.Second)); err != nil {
		t.Fatal(err)
	}
	// Resubmit out of order; must stay unique.
	if _, _, err := s.Receive([]Heartbeat{rec("a", 2, 2, 0, 2), rec("a", 9, 9, 0, 9)}, t0.Add(20*time.Second)); err != nil {
		t.Fatal(err)
	}
	hist := s.History("a")
	want := []int64{2, 5, 9}
	if len(hist) != len(want) {
		t.Fatalf("history len=%d, want %d (%+v)", len(hist), len(want), hist)
	}
	for i, h := range hist {
		if h.Seq != want[i] {
			t.Fatalf("history[%d]=%d want %d", i, h.Seq, want[i])
		}
	}
}

func TestRestartPreservesDataAndDedupAndLatest(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Receive([]Heartbeat{rec("a", 1, 1, 0, 0), rec("a", 4, 4, 0, 4)}, t0.Add(20*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(dir) // platform "restart"
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()

	if got := s2.Latest("a"); got.Seq != 4 || got.Height != 4 {
		t.Fatalf("after restart latest=%+v", got)
	}
	ins, dup, err := s2.Receive([]Heartbeat{rec("a", 1, 1, 0, 0)}, t0.Add(5*time.Second))
	if err != nil || ins != 0 || dup != 1 {
		t.Fatalf("dedup after restart: ins=%d dup=%d err=%v", ins, dup, err)
	}
	// Conflict still detected after restart.
	_, _, err = s2.Receive([]Heartbeat{rec("a", 4, 40, 0, 4)}, t0.Add(6*time.Second))
	var ce *ConflictError
	if !errors.As(err, &ce) {
		t.Fatalf("conflict must survive restart, got %v", err)
	}
	if len(s2.History("a")) != 2 {
		t.Fatalf("history after restart = %v", s2.History("a"))
	}
}

func TestCrashTailIsTruncated(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Receive([]Heartbeat{rec("a", 1, 1, 0, 0)}, t0); err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(dir, logFileName)
	good, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Simulate a crash mid-append: a torn, uncommitted suffix with no COMMIT.
	torn := append(append([]byte{}, good...), []byte(frameMagic+"\x01\x00\x00\x00\x10partial-torn")...)
	if err := os.WriteFile(logPath, torn, 0o644); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(dir)
	if err != nil {
		t.Fatalf("torn tail should reopen after truncation: %v", err)
	}
	defer s2.Close()
	if got := s2.Latest("a"); got == nil || got.Seq != 1 {
		t.Fatalf("committed data lost after tail recovery: %+v", got)
	}
	after, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(good) {
		t.Fatalf("tail not truncated: %d != %d", len(after), len(good))
	}
}

func TestCorruptCommittedDataIsRejected(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Receive([]Heartbeat{rec("a", 1, 1, 0, 0), rec("a", 2, 2, 0, 2)}, t0.Add(20*time.Second)); err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(dir, logFileName)
	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Flip a byte inside the first committed frame (well before the end).
	idx := frameHeaderSize + 8 + 5
	corrupted := append([]byte{}, data...)
	corrupted[idx] ^= 0xFF
	if err := os.WriteFile(logPath, corrupted, 0o644); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(dir)
	if err == nil {
		_ = s2.Close()
		t.Fatal("corrupt committed data must be rejected, Open succeeded")
	}
	var ce *CorruptionError
	if !errors.As(err, &ce) {
		t.Fatalf("want CorruptionError, got %T: %v", err, err)
	}

	// And writes must not continue against the damaged directory either:
	// reopening still fails rather than treating it as an empty database.
	if _, err := Open(dir); err == nil {
		t.Fatal("damaged store must keep refusing")
	}
	if _, err := os.Stat(filepath.Join(dir, logFileName)); err != nil {
		t.Fatalf("corrupt log must not be deleted: %v", err)
	}
}

func TestEmptyAndTimingValidation(t *testing.T) {
	s := openTestStore(t)
	if _, _, err := s.Receive(nil, t0); err == nil {
		t.Fatal("empty batch must error")
	}
	future := rec("a", 1, 1, 0, 100)
	if _, _, err := s.Receive([]Heartbeat{future}, t0); err == nil {
		t.Fatal("collected_at after received_at must error")
	}
}

func TestInProcessConcurrentIdenticalAndConflicting(t *testing.T) {
	s := openTestStore(t)
	const n = 16
	var wg sync.WaitGroup

	// Identical record hammered concurrently: exactly one insert.
	start := make(chan struct{})
	insertCounts := make([]int, n)
	errs := make([]error, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			<-start
			ins, _, err := s.Receive([]Heartbeat{rec("same", 1, 1, 0, 0)}, t0)
			insertCounts[i], errs[i] = ins, err
		}(i)
	}
	close(start)
	wg.Wait()
	totalIns, totalDup := 0, 0
	for i := range insertCounts {
		if errs[i] != nil {
			t.Fatalf("worker %d: %v", i, errs[i])
		}
		totalIns += insertCounts[i]
	}
	_ = totalDup
	if totalIns != 1 {
		t.Fatalf("identical concurrent inserts: total new=%d want 1", totalIns)
	}
	if len(s.History("same")) != 1 {
		t.Fatal("duplicate record materialised more than once")
	}

	// Conflicting content hammered concurrently: exactly one winner.
	start2 := make(chan struct{})
	winner := make(chan int64, n)
	var conflicted int
	var mu sync.Mutex
	wg.Add(n)
	for i := 0; i < n; i++ {
		height := int64(i + 1) // distinct content per worker
		go func() {
			defer wg.Done()
			<-start2
			_, _, err := s.Receive([]Heartbeat{rec("race", 1, height, 0, 0)}, t0)
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				winner <- height
			} else {
				conflicted++
			}
		}()
	}
	close(start2)
	wg.Wait()
	close(winner)
	if len(winner) != 1 {
		t.Fatalf("conflicting commits: winners=%d conflicts=%d, want exactly 1 winner", len(winner), conflicted)
	}
	if h := len(s.History("race")); h != 1 {
		t.Fatalf("race node history len=%d, want 1", h)
	}
}

func TestReceiveFailureKeepsPriorDataQueryable(t *testing.T) {
	s := openTestStore(t)
	if _, _, err := s.Receive([]Heartbeat{rec("a", 1, 1, 0, 0)}, t0); err != nil {
		t.Fatal(err)
	}
	// A rejected conflict must not touch prior data.
	if _, _, err := s.Receive([]Heartbeat{rec("a", 1, 9, 0, 0)}, t0); err == nil {
		t.Fatal("expected conflict")
	}
	hist := s.History("a")
	if len(hist) != 1 || hist[0].Height != 1 {
		t.Fatalf("prior data altered: %+v", hist)
	}
}

func TestCompactionRoundTrip(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	// Force compaction with many small records (threshold is not exported).
	// Instead, call compact after inserting a modest set directly.
	for i := 1; i <= 50; i++ {
		if _, _, err := s.Receive([]Heartbeat{rec("a", int64(i), int64(i), 0, i)},
			t0.Add(time.Duration(i+1)*time.Second)); err != nil {
			t.Fatal(err)
		}
	}
	s.mu.Lock()
	if err := s.compact(); err != nil {
		s.mu.Unlock()
		t.Fatal(err)
	}
	s.mu.Unlock()
	if got := s.Latest("a"); got.Seq != 50 || got.Height != 50 {
		t.Fatalf("after compact latest=%+v", got)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen after compact: %v", err)
	}
	defer s2.Close()
	if h := s2.History("a"); len(h) != 50 || h[0].Seq != 1 || h[49].Seq != 50 {
		t.Fatalf("history after compact+restart wrong: len=%d first/last=%+v", len(h), h)
	}
	// New batches append fine and batch ids do not collide.
	if _, _, err := s2.Receive([]Heartbeat{rec("a", 51, 51, 0, 51)}, t0.Add(time.Minute)); err != nil {
		t.Fatalf("append after compact: %v", err)
	}
}

func TestLockSerialisesTwoHandles(t *testing.T) {
	dir := t.TempDir()
	s1, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	// While s1 holds the flock, a second Open blocks rather than returning.
	done := make(chan error, 1)
	go func() {
		s2, err := Open(dir)
		if err == nil {
			_ = s2.Close()
		}
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatalf("second Open must block on flock while first is held, returned err=%v", err)
	case <-time.After(200 * time.Millisecond):
		// Expected: still queued behind the exclusive lock.
	}

	// Releasing s1 lets the waiter through; join it so no handle outlives the
	// test (otherwise its lock file races TempDir cleanup).
	if err := s1.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("second Open should succeed after release: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("second Open still blocked after release")
	}
}
