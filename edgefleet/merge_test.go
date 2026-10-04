package edgefleet

import (
	"errors"
	"os"
	"strings"
	"testing"
	"time"
)

// These tests pin the single duplicate/conflict rule used by Submit: the
// "same identity (node+seq), same full content?" decision is made once in
// mergeHeartbeats and applies identically to repeats within a batch and
// repeats against stored history.

func TestMergeCountsByOccurrenceNotDistinctIdentity(t *testing.T) {
	receive := testBase
	r := hb("n1", 1, receive.Add(-time.Minute), "1.0", 100, 0)

	// A previously unknown record arriving three times: one new, two dups.
	merged, newC, dupC, err := mergeHeartbeats([]Heartbeat{r, r, r}, nil)
	if err != nil {
		t.Fatalf("merge failed: %v", err)
	}
	if newC != 1 || dupC != 2 {
		t.Errorf("new identity three times: new=%d dup=%d, want 1/2", newC, dupC)
	}
	if got := merged["n1"]; len(got) != 1 || !got[0].Equal(r) {
		t.Errorf("history keeps one record, got %+v", got)
	}

	// The same identity already saved: all three are duplicates.
	history := map[string][]Heartbeat{"n1": {r}}
	merged, newC, dupC, err = mergeHeartbeats([]Heartbeat{r, r, r}, history)
	if err != nil {
		t.Fatalf("merge against history failed: %v", err)
	}
	if newC != 0 || dupC != 3 {
		t.Errorf("saved identity three times: new=%d dup=%d, want 0/3", newC, dupC)
	}
	if len(merged["n1"]) != 1 {
		t.Errorf("history must still hold one record, got %+v", merged["n1"])
	}
}

func TestMergeNewHistoryAndIntraBatchDuplicatesCoexist(t *testing.T) {
	receive := testBase
	x := hb("n1", 1, receive.Add(-3*time.Minute), "1.0", 100, 0) // new, 3x
	y := hb("n2", 1, receive.Add(-2*time.Minute), "1.0", 200, 1) // saved, 2x
	z := hb("n3", 1, receive.Add(-time.Minute), "1.0", 300, 2)   // new, 1x
	history := map[string][]Heartbeat{"n2": {y}}

	batch := []Heartbeat{x, x, y, z, x, y}
	merged, newC, dupC, err := mergeHeartbeats(batch, history)
	if err != nil {
		t.Fatalf("merge failed: %v", err)
	}
	// New identities: x and z (2). Duplicates by occurrence: x twice, y twice
	// (4). The cases coexist in one batch.
	if newC != 2 || dupC != 4 {
		t.Errorf("new=%d dup=%d, want 2/4", newC, dupC)
	}
	for nodeID, want := range map[string]Heartbeat{"n1": x, "n2": y, "n3": z} {
		got := merged[nodeID]
		if len(got) != 1 || !got[0].Equal(want) {
			t.Errorf("node %s history = %+v, want exactly %+v", nodeID, got, want)
		}
	}
}

func TestMergeConflictTypesAndMessages(t *testing.T) {
	receive := testBase
	a := hb("n1", 1, receive.Add(-time.Minute), "1.0", 100, 0)
	bVersion := hb("n1", 1, receive.Add(-time.Minute), "2.0", 100, 0)

	// Conflict between two records of the batch itself.
	_, _, _, err := mergeHeartbeats([]Heartbeat{a, bVersion}, nil)
	if err == nil || !IsConflict(err) {
		t.Fatalf("expected ConflictError, got %v", err)
	}
	ce := asConflictErr(t, err)
	if !ce.InBatch {
		t.Errorf("InBatch=false, want true for an intra-batch conflict")
	}
	wantMsg := `conflicting records for node "n1" seq 1 in batch: content differs`
	if err.Error() != wantMsg {
		t.Errorf("in-batch message = %q, want %q", err.Error(), wantMsg)
	}

	// Conflict with stored history.
	history := map[string][]Heartbeat{"n1": {a}}
	_, _, _, err = mergeHeartbeats([]Heartbeat{bVersion}, history)
	if err == nil || !IsConflict(err) {
		t.Fatalf("expected ConflictError, got %v", err)
	}
	ce = asConflictErr(t, err)
	if ce.InBatch {
		t.Errorf("InBatch=true, want false for a conflict with stored history")
	}
	wantMsg = `conflicting record for node "n1" seq 1: content differs from stored record`
	if err.Error() != wantMsg {
		t.Errorf("stored message = %q, want %q", err.Error(), wantMsg)
	}
	if ce.NodeID != "n1" || ce.Seq != 1 {
		t.Errorf("conflict identity = %q/%d, want n1/1", ce.NodeID, ce.Seq)
	}
}

func asConflictErr(t *testing.T, err error) *ConflictError {
	t.Helper()
	var ce *ConflictError
	if !errors.As(err, &ce) {
		t.Fatalf("error %v does not wrap *ConflictError", err)
	}
	return ce
}

func TestMergeAnyContentDifferenceConflicts(t *testing.T) {
	receive := testBase
	base := hb("n1", 1, receive.Add(-time.Minute), "1.0", 100, 5)
	cases := map[string]Heartbeat{
		"version":      hb("n1", 1, receive.Add(-time.Minute), "2.0", 100, 5),
		"height":       hb("n1", 1, receive.Add(-time.Minute), "1.0", 101, 5),
		"missed":       hb("n1", 1, receive.Add(-time.Minute), "1.0", 100, 6),
		"collected_at": hb("n1", 1, receive.Add(-2*time.Minute), "1.0", 100, 5),
	}
	for name, other := range cases {
		// Within the batch.
		if _, _, _, err := mergeHeartbeats([]Heartbeat{base, other}, nil); !IsConflict(err) {
			t.Errorf("in-batch %s difference must conflict, got %v", name, err)
		}
		// Against history.
		history := map[string][]Heartbeat{"n1": {base}}
		if _, _, _, err := mergeHeartbeats([]Heartbeat{other}, history); !IsConflict(err) {
			t.Errorf("history %s difference must conflict, got %v", name, err)
		}
	}
}

func TestMergeIdentityIsExactNodeAndSeq(t *testing.T) {
	receive := testBase
	r := func(node string, seq int64) Heartbeat {
		return hb(node, seq, receive.Add(-time.Minute), "1.0", 100, 0)
	}

	// Different nodes sharing a seq are different identities: both new.
	merged, newC, dupC, err := mergeHeartbeats([]Heartbeat{r("n1", 1), r("n2", 1)}, nil)
	if err != nil || newC != 2 || dupC != 0 {
		t.Fatalf("different nodes same seq: new=%d dup=%d err=%v", newC, dupC, err)
	}
	if len(merged["n1"]) != 1 || len(merged["n2"]) != 1 {
		t.Errorf("each node keeps its own record: %+v", merged)
	}

	// Node ids compare as exact values: casing and surrounding spaces never
	// merge identities.
	merged, newC, _, err = mergeHeartbeats([]Heartbeat{r("n1", 1), r("N1", 1), r(" n1", 1), r("n1 ", 1)}, nil)
	if err != nil {
		t.Fatalf("distinct exact node ids must not conflict: %v", err)
	}
	if newC != 4 {
		t.Errorf("new=%d, want 4 distinct node ids", newC)
	}
	for _, id := range []string{"n1", "N1", " n1", "n1 "} {
		if len(merged[id]) != 1 {
			t.Errorf("node %q not kept as its own identity: %+v", id, merged[id])
		}
	}
}

func TestMergeTimezoneZeroAndLargeIntegers(t *testing.T) {
	receive := testBase
	utc := hb("n1", 1, receive.Add(-time.Minute).UTC(), "1.0", 0, 0)
	plus8 := hb("n1", 1, receive.Add(-time.Minute).In(time.FixedZone("+08", 8*3600)), "1.0", 0, 0)

	// Same instant in a different timezone, explicit zero telemetry: a dup.
	_, newC, dupC, err := mergeHeartbeats([]Heartbeat{utc, plus8}, nil)
	if err != nil || newC != 1 || dupC != 1 {
		t.Fatalf("same instant different zone: new=%d dup=%d err=%v", newC, dupC, err)
	}

	// Large integers survive the comparison exactly.
	const big = int64(9223372036854775807)
	large := hb("big", 1, receive.Add(-time.Minute), "1.0", big, big-1)
	merged, _, _, err := mergeHeartbeats([]Heartbeat{large, large, large}, nil)
	if err != nil {
		t.Fatalf("large integer merge failed: %v", err)
	}
	got := merged["big"][0]
	if got.Height != big || got.Missed != big-1 {
		t.Errorf("large integers altered: height=%d missed=%d", got.Height, got.Missed)
	}
}

func TestMergeOrdersBySeqWithGapsAndKeepsMaxRecord(t *testing.T) {
	receive := testBase
	history := map[string][]Heartbeat{"n1": {
		hb("n1", 5, receive.Add(-time.Minute), "1.0", 105, 0),
	}}
	// A late, smaller seq and a gapped seq arrive together, each twice.
	batch := []Heartbeat{
		hb("n1", 1, receive.Add(-5*time.Minute), "1.0", 101, 0),
		hb("n1", 1, receive.Add(-5*time.Minute), "1.0", 101, 0),
		hb("n1", 3, receive.Add(-3*time.Minute), "1.0", 103, 0),
	}
	merged, newC, dupC, err := mergeHeartbeats(batch, history)
	if err != nil {
		t.Fatalf("merge failed: %v", err)
	}
	if newC != 2 || dupC != 1 {
		t.Errorf("new=%d dup=%d, want 2/1", newC, dupC)
	}
	got := merged["n1"]
	for i, wantSeq := range []int64{1, 3, 5} {
		if got[i].Seq != wantSeq {
			t.Errorf("position %d: seq=%d, want %d (records=%+v)", i, got[i].Seq, wantSeq, got)
		}
	}
	// The late insert must not replace the max-seq record health queries use.
	if got[len(got)-1].Seq != 5 || got[len(got)-1].Height != 105 {
		t.Errorf("max-seq record displaced by late record: %+v", got)
	}
}

func TestMergeDoesNotMutateInputHistory(t *testing.T) {
	receive := testBase
	existing := []Heartbeat{hb("n1", 1, receive.Add(-2*time.Minute), "1.0", 100, 0)}
	history := map[string][]Heartbeat{"n1": existing}

	// All-duplicate batch: returned content equals history, but the caller's
	// slice and map must stay untouched and share no backing array.
	dup := hb("n1", 1, receive.Add(-2*time.Minute), "1.0", 100, 0)
	merged, _, _, err := mergeHeartbeats([]Heartbeat{dup}, history)
	if err != nil {
		t.Fatal(err)
	}
	merged["n1"][0].Version = "tampered"
	if history["n1"][0].Version != "1.0" {
		t.Errorf("input history mutated through returned slice: %+v", history["n1"])
	}
	if len(history) != 1 || len(history["n1"]) != 1 {
		t.Errorf("input history map changed: %+v", history)
	}
}

func TestSubmitTripleOccurrenceAndMixedBatchCounts(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase
	x := hb("n1", 1, receive.Add(-3*time.Minute), "1.0", 100, 0)

	// Unknown record three times: 1 new, 2 duplicates; one saved record.
	newC, dupC, err := store.Submit([]Heartbeat{x, x, x}, receive)
	if err != nil {
		t.Fatalf("submit failed: %v", err)
	}
	if newC != 1 || dupC != 2 {
		t.Errorf("triple new: new=%d dup=%d, want 1/2", newC, dupC)
	}
	hist, err := store.History("n1")
	if err != nil || len(hist) != 1 {
		t.Fatalf("history len=%d err=%v, want 1", len(hist), err)
	}

	// Already saved, three more times: all duplicates, still one record.
	newC, dupC, err = store.Submit([]Heartbeat{x, x, x}, receive)
	if err != nil {
		t.Fatalf("resubmit failed: %v", err)
	}
	if newC != 0 || dupC != 3 {
		t.Errorf("triple saved: new=%d dup=%d, want 0/3", newC, dupC)
	}
	hist, _ = store.History("n1")
	if len(hist) != 1 {
		t.Errorf("history len=%d, want 1", len(hist))
	}

	// One batch mixing a brand-new identity (2x), a history duplicate (2x) and
	// another brand-new identity (1x).
	y := hb("n2", 1, receive.Add(-2*time.Minute), "2.0", 50, 3)
	z := hb("n3", 1, receive.Add(-time.Minute), "3.0", 70, 0)
	newC, dupC, err = store.Submit([]Heartbeat{y, y, x, z, x}, receive)
	if err != nil {
		t.Fatalf("mixed submit failed: %v", err)
	}
	if newC != 2 || dupC != 3 {
		t.Errorf("mixed: new=%d dup=%d, want 2/3", newC, dupC)
	}
}

func TestSubmitConflictErrorIsTyped(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase
	a := hb("n1", 1, receive.Add(-time.Minute), "1.0", 100, 0)
	if _, _, err := store.Submit([]Heartbeat{a}, receive); err != nil {
		t.Fatal(err)
	}

	// Against-history conflict is a typed ConflictError naming node and seq.
	b := hb("n1", 1, receive.Add(-time.Minute), "2.0", 100, 0)
	_, _, err = store.Submit([]Heartbeat{b}, receive)
	if err == nil || !IsConflict(err) {
		t.Fatalf("expected typed conflict, got %v", err)
	}
	ce := asConflictErr(t, err)
	if ce.InBatch || ce.NodeID != "n1" || ce.Seq != 1 {
		t.Errorf("unexpected conflict: %+v", ce)
	}
	if !strings.Contains(err.Error(), "n1") || !strings.Contains(err.Error(), "seq 1") {
		t.Errorf("conflict error must name node and seq: %v", err)
	}

	// Intra-batch conflict is the same type with InBatch set.
	_, _, err = store.Submit([]Heartbeat{
		hb("n2", 1, receive.Add(-time.Minute), "1.0", 100, 0),
		hb("n2", 1, receive.Add(-time.Minute), "2.0", 100, 0),
	}, receive)
	if err == nil || !IsConflict(err) {
		t.Fatalf("expected typed in-batch conflict, got %v", err)
	}
	if !asConflictErr(t, err).InBatch {
		t.Errorf("intra-batch conflict must carry InBatch=true: %v", err)
	}
}

func TestSubmitInBatchConflictPrecedesUnrelatedCorruption(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase
	// Give one touched node saved data, then corrupt its file.
	if _, _, err := store.Submit([]Heartbeat{
		hb("good", 1, receive.Add(-time.Minute), "1.0", 100, 0),
	}, receive); err != nil {
		t.Fatal(err)
	}
	tamperedFile(t, store.nodePath("good"),
		`[{"node":"good","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":100}]`)

	// The batch also contradicts itself on a different node. The in-batch
	// conflict must win: nothing is written and the error category stays
	// "conflict", because phase 1 never touches the corrupt file.
	_, _, err = store.Submit([]Heartbeat{
		hb("good", 2, receive.Add(-time.Second), "1.0", 102, 0), // would hit corruption
		hb("new", 1, receive.Add(-time.Second), "1.0", 1, 0),
		hb("new", 1, receive.Add(-time.Second), "2.0", 1, 0), // in-batch conflict
	}, receive)
	if err == nil || !IsConflict(err) {
		t.Fatalf("in-batch conflict must take precedence, got %v", err)
	}
	if !asConflictErr(t, err).InBatch {
		t.Errorf("expected InBatch conflict, got %v", err)
	}
	if _, err := os.Stat(store.nodePath("new")); !os.IsNotExist(err) {
		t.Errorf("new node file must not be created, stat err=%v", err)
	}

	// A self-consistent batch touching the same corrupt node is still refused
	// as corruption — phase 2 never hides data damage.
	_, _, err = store.Submit([]Heartbeat{
		hb("good", 2, receive.Add(-time.Second), "1.0", 102, 0),
	}, receive)
	if err == nil || !IsCorrupt(err) {
		t.Fatalf("corrupt touched node must still be refused, got %v", err)
	}
}

func TestSubmitRejectsWholeBatchOnConflictAcrossSources(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase
	a := hb("n1", 1, receive.Add(-time.Minute), "1.0", 100, 0)
	if _, _, err := store.Submit([]Heartbeat{a}, receive); err != nil {
		t.Fatal(err)
	}

	// Conflict on n1 plus otherwise-valid records for other nodes: nothing in
	// the batch may be saved, and n1's original record survives.
	_, _, err = store.Submit([]Heartbeat{
		hb("n1", 1, receive.Add(-time.Minute), "9.9", 100, 0), // conflicts
		hb("fresh", 1, receive.Add(-time.Second), "1.0", 1, 0),
		hb("fresh", 1, receive.Add(-time.Second), "1.0", 1, 0), // intra dup
	}, receive)
	if err == nil || !IsConflict(err) {
		t.Fatalf("expected conflict rejection, got %v", err)
	}
	hist, _ := store.History("n1")
	if len(hist) != 1 || hist[0].Version != "1.0" {
		t.Errorf("stored record overwritten after conflict: %+v", hist)
	}
	if fresh, _ := store.History("fresh"); len(fresh) != 0 {
		t.Errorf("valid sibling records saved despite conflict: %+v", fresh)
	}
}
