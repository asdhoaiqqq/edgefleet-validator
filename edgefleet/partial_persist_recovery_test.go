package edgefleet

import (
	"errors"
	"strings"
	"testing"
	"time"
)

// These tests protect the recovery workflow documented in the README: a write
// failure can strike in the middle of persisting a multi-node batch, leaving
// some nodes' new records durably saved and others not yet saved. The command
// must report the failure with new=0 duplicate=0 (it must not present the
// already-persisted part as a successful batch), and resubmitting the very
// same JSON verbatim after the fault is cleared must converge: saved records
// count as duplicates, the one record still missing is added exactly once,
// and the batch-internal repeat still counts as a duplicate.
//
// The fault is injected only at the persistence boundary (writeNodeFile),
// after all validation and merging has run, so the exercise goes through the
// real code path — real atomic file writes, real on-disk state afterwards —
// without filling a disk or relying on wall-clock timing. The same two nodes
// run under both failure positions, so the result cannot depend on which node
// happens to be written first (that order is deliberately not promised).

// persistFault temporarily replaces writeNodeFile. failOnCall is the
// 1-based number of the node-file write that must return an error; every
// other write performs the real atomic persistence. It returns a pointer to
// the number of writes actually attempted.
func persistFault(t *testing.T, failOnCall int) *int {
	t.Helper()
	calls := 0
	orig := writeNodeFile
	writeNodeFile = func(path string, records []Heartbeat) error {
		calls++
		if calls == failOnCall {
			return errors.New("injected persistence failure")
		}
		return orig(path, records)
	}
	t.Cleanup(func() { writeNodeFile = orig })
	return &calls
}

// assertHistory loads a node's history through the normal query interface on
// a freshly opened store (so the assertion is against data durably on disk,
// not in-memory state) and checks the records exactly.
func assertHistory(t *testing.T, dir, node string, want []Heartbeat) {
	t.Helper()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	got, err := store.History(node)
	if err != nil {
		t.Fatalf("history for %q after failure must be readable: %v", node, err)
	}
	if len(got) != len(want) {
		t.Fatalf("node %q history has %d records, want %d: %+v", node, len(got), len(want), got)
	}
	for i := range want {
		if !got[i].Equal(want[i]) {
			t.Errorf("node %q record %d = %+v, want %+v", node, i, got[i], want[i])
		}
	}
	// History must be ascending by seq with no duplicate seq.
	for i := 1; i < len(got); i++ {
		if got[i-1].Seq >= got[i].Seq {
			t.Errorf("node %q history not strictly ascending: %+v", node, got)
		}
	}
}

// TestSubmitPartialPersistFailureThenVerbatimResubmit drives the full
// sequence: partial save during a failed submit, verification through history
// of exactly what landed, then a verbatim resubmit that converges to
// new=1 duplicate=3 with no record replaced or lost.
func TestSubmitPartialPersistFailureThenVerbatimResubmit(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase

	// Fixed instants: both nodes already have seq 1 before the batch.
	alpha1 := hb("alpha", 1, receive.Add(-2*time.Minute), "1.0", 100, 0)
	beta1 := hb("beta", 1, receive.Add(-2*time.Minute), "1.0", 200, 0)
	if _, _, err := store.Submit([]Heartbeat{alpha1, beta1}, receive); err != nil {
		t.Fatal(err)
	}

	// The batch adds seq 2 to both nodes. One node's seq 2 is present twice
	// with identical content, and one existing seq 1 is carried along: four
	// input records in total, all legal.
	alpha2 := hb("alpha", 2, receive.Add(-time.Minute), "1.0", 101, 0)
	beta2 := hb("beta", 2, receive.Add(-time.Minute), "1.0", 201, 1)
	batch := []Heartbeat{alpha2, alpha2, beta2, beta1}

	// Fail the SECOND node-file write. Both nodes gain a record, so exactly
	// one real write completes and durably renames a file into place before
	// the failing one: this is a genuine partial save, not an error returned
	// before anything was persisted.
	calls := persistFault(t, 2)
	newC, dupC, err := store.Submit(batch, receive)
	if err == nil {
		t.Fatal("submit must report a failure when a node-file write fails midway")
	}
	if !strings.Contains(err.Error(), "failed to persist heartbeat batch") {
		t.Fatalf("error must be the persistence failure, got: %v", err)
	}
	if newC != 0 || dupC != 0 {
		t.Errorf("failed submit must report new=0 duplicate=0, got new=%d duplicate=%d (the already-saved part must not be reported as success)", newC, dupC)
	}
	if *calls != 2 {
		t.Fatalf("test setup: expected two node-file writes (one success, one failure), got %d; a failure that saved no new record is a different scenario", *calls)
	}

	// Observe the real, on-disk outcome through the history query interface.
	// Which node was persisted first is not promised and varies with map
	// iteration, so identify the persisted node from the data instead of
	// naming it. Exactly one node must have gained seq 2.
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
		t.Fatalf("after the partial failure exactly one node must have 2 records and the other 1, got alpha=%d beta=%d", len(alphaHist), len(betaHist))
	}
	t.Logf("partial save: node %q persisted seq 2 first; node %q did not", persistedNode, missingNode)

	// The node that wrote first has its original seq 1 intact plus the new
	// seq 2 with the submitted content; the other node has only its seq 1.
	wantPersisted := []Heartbeat{alpha1, alpha2}
	wantMissing := []Heartbeat{beta1}
	if persistedNode == "beta" {
		wantPersisted = []Heartbeat{beta1, beta2}
		wantMissing = []Heartbeat{alpha1}
	}
	assertHistory(t, dir, persistedNode, wantPersisted)
	assertHistory(t, dir, missingNode, wantMissing)

	// Clear the fault and resubmit the SAME four records verbatim, reusing
	// the original legal receive time — duplicate detection must not be
	// dodged by altering content or timestamps.
	newC, dupC, err = store.Submit(batch, receive)
	if err != nil {
		t.Fatalf("verbatim resubmit after the fault is cleared must succeed: %v", err)
	}
	if newC != 1 || dupC != 3 {
		t.Errorf("resubmit: new=%d duplicate=%d, want new=1 duplicate=3 (the landed record is a duplicate, the missing one is added once, and the in-batch repeat is a duplicate)", newC, dupC)
	}

	// Final state: both nodes end with exactly seq 1 and seq 2, ascending,
	// and same seq numbers across the two nodes stay independent records.
	// Neither the old seq-1 content nor the new seq-2 content was replaced
	// or lost.
	assertHistory(t, dir, "alpha", []Heartbeat{alpha1, alpha2})
	assertHistory(t, dir, "beta", []Heartbeat{beta1, beta2})
}

// TestSubmitPersistenceFailureBeforeAnyNewSaveLeavesNothingNew is the
// contrasting case: a write failure on the FIRST node-file write, before any
// new record has been persisted. It must not be mistaken for the partial-save
// recovery scenario — no node gains anything, and the subsequent resubmit
// adds both nodes' new records.
func TestSubmitPersistenceFailureBeforeAnyNewSaveLeavesNothingNew(t *testing.T) {
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
	batch := []Heartbeat{alpha2, alpha2, beta2, beta1}

	calls := persistFault(t, 1)
	newC, dupC, err := store.Submit(batch, receive)
	if err == nil {
		t.Fatal("expected a persistence failure")
	}
	if !strings.Contains(err.Error(), "failed to persist heartbeat batch") {
		t.Fatalf("wrong error: %v", err)
	}
	if newC != 0 || dupC != 0 {
		t.Errorf("failed submit must report new=0 duplicate=0, got new=%d duplicate=%d", newC, dupC)
	}
	if *calls != 1 {
		t.Fatalf("expected the failure on the first and only attempted write, got %d calls", *calls)
	}

	// No new record landed for either node; both seq-1 records are intact.
	assertHistory(t, dir, "alpha", []Heartbeat{alpha1})
	assertHistory(t, dir, "beta", []Heartbeat{beta1})

	// After clearing the fault, the verbatim batch adds both seq-2 records:
	// two new, two duplicates (one in-batch repeat, one existing seq 1).
	newC, dupC, err = store.Submit(batch, receive)
	if err != nil {
		t.Fatalf("resubmit must succeed: %v", err)
	}
	if newC != 2 || dupC != 2 {
		t.Errorf("resubmit: new=%d duplicate=%d, want new=2 duplicate=2", newC, dupC)
	}
	assertHistory(t, dir, "alpha", []Heartbeat{alpha1, alpha2})
	assertHistory(t, dir, "beta", []Heartbeat{beta1, beta2})
}
