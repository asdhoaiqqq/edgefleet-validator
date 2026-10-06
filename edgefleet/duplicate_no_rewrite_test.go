package edgefleet

import (
	"errors"
	"os"
	"strings"
	"testing"
	"time"
)

// These tests protect the reconnect-resubmit rule: a node whose whole batch
// contribution is already-saved duplicates is NOT re-saved. The platform may
// hold the data-directory lock and read the node's history, but it must not
// write the node's file again — a retransmit must not turn into a save
// failure when that node's file temporarily cannot be replaced.
//
// The proof is deterministic and offline: writeNodeFile is armed to fail any
// write to the duplicate-only node's file, so a successful submit directly
// proves no rewrite was attempted — comparing file bytes before and after
// could not tell a skipped write from a rewritten-identical one, and no file
// timestamp is ever consulted. Each arming is also proven live by driving a
// genuine new-record submit against it, which must fail.

// failWritesTo temporarily replaces writeNodeFile so that any write to path
// fails, while writes to every other path persist for real. It returns a
// pointer to the number of attempted writes to path.
func failWritesTo(t *testing.T, path string) *int {
	t.Helper()
	calls := 0
	orig := writeNodeFile
	writeNodeFile = func(p string, records []Heartbeat) error {
		if p == path {
			calls++
			return errors.New("injected persistence failure")
		}
		return orig(p, records)
	}
	t.Cleanup(func() { writeNodeFile = orig })
	return &calls
}

// readFileBytes loads a file's bytes for an exact before/after comparison.
func readFileBytes(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("test setup: read %s: %v", path, err)
	}
	return data
}

// TestDuplicateOnlyBatchIsNotResaved arms the duplicate-only node's file to
// fail on write, then resubmits saved records. The submit must succeed with
// new=0 and every input occurrence counted as a duplicate, the history must
// gain no rows, and the armed write must never have fired.
func TestDuplicateOnlyBatchIsNotResaved(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase

	r1 := hb("n1", 1, receive.Add(-2*time.Minute), "1.0", 100, 0)
	r2 := hb("n1", 2, receive.Add(-time.Minute), "1.0", 101, 1)
	if _, _, err := store.Submit([]Heartbeat{r1, r2}, receive); err != nil {
		t.Fatal(err)
	}
	path := store.nodePath("n1")
	before := readFileBytes(t, path)

	// The reconnect batch carries only saved records; r1 appears twice, so
	// the duplicate count is 3 — every occurrence in the input counts.
	calls := failWritesTo(t, path)
	newC, dupC, err := store.Submit([]Heartbeat{r1, r2, r1}, receive)
	if err != nil {
		t.Fatalf("duplicate-only resubmit must succeed even though the node file cannot be re-saved: %v", err)
	}
	if newC != 0 || dupC != 3 {
		t.Errorf("new=%d duplicate=%d, want new=0 duplicate=3", newC, dupC)
	}
	if *calls != 0 {
		t.Fatalf("the duplicate-only node's file was re-saved (%d write attempt); a retransmit must not touch it", *calls)
	}
	assertHistory(t, dir, "n1", []Heartbeat{r1, r2})
	if after := readFileBytes(t, path); string(after) != string(before) {
		t.Errorf("node file bytes changed although nothing was new")
	}

	// Prove the arming was live: a genuinely new record for the same node
	// must hit the injected failure, so the success above cannot be a
	// vacuous hook.
	if _, _, err := store.Submit([]Heartbeat{hb("n1", 3, receive.Add(-time.Minute), "1.0", 102, 1)}, receive); err == nil {
		t.Fatal("test setup: the armed write must fail a real re-save")
	}
	if *calls != 1 {
		t.Fatalf("test setup: expected exactly one armed write by now, got %d", *calls)
	}
}

// TestDuplicateEquivalentSpellingsAreNotResaved covers the equivalence rules
// of duplicate identity: the same instant in another timezone, and node or
// version text written with JSON Unicode escapes, decode to the same
// telemetry and must count as duplicates — without rewriting the stored
// original text or collection time.
func TestDuplicateEquivalentSpellingsAreNotResaved(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase

	// Saved literally: node "n1", version "1.0", collected at 11:58 UTC.
	saved := hb("n1", 1, receive.Add(-2*time.Minute), "1.0", 100, 0)
	if _, _, err := store.Submit([]Heartbeat{saved}, receive); err != nil {
		t.Fatal(err)
	}
	path := store.nodePath("n1")
	before := readFileBytes(t, path)

	// The same heartbeat with "n" and "." escaped and the same instant
	// expressed at +08:00. Every spelling decodes to the saved telemetry.
	bs := `\u`
	input := `[{"node":"` + bs + `006e1","seq":1,"collected_at":"2026-10-01T19:58:00+08:00","version":"1` + bs + `002e0","height":100,"missed":0}]`
	records, err := ParseHeartbeats([]byte(input), receive)
	if err != nil {
		t.Fatalf("equivalent spellings must parse: %v", err)
	}
	if records[0].NodeID != "n1" || records[0].Version != "1.0" || !records[0].CollectedAt.Equal(saved.CollectedAt) {
		t.Fatalf("test setup: spellings did not decode to the saved telemetry: %+v", records[0])
	}

	calls := failWritesTo(t, path)
	newC, dupC, err := store.Submit(records, receive)
	if err != nil {
		t.Fatalf("equivalent-spelling duplicate must succeed without a re-save: %v", err)
	}
	if newC != 0 || dupC != 1 {
		t.Errorf("new=%d duplicate=%d, want new=0 duplicate=1", newC, dupC)
	}
	if *calls != 0 {
		t.Fatalf("equivalent-spelling duplicate triggered %d re-save; the file must be left alone", *calls)
	}

	// The stored record keeps its original text and collection instant: the
	// escaped spellings must not have been normalised into the file.
	if after := readFileBytes(t, path); string(after) != string(before) {
		t.Errorf("stored text or collection time was rewritten by a duplicate")
	}
	assertHistory(t, dir, "n1", []Heartbeat{saved})
}

// TestDuplicateOnlyNodeDoesNotBlockOtherNode submits a mixed batch: node A
// contributes only saved duplicates and cannot be re-saved, node B has a
// legitimate new record. A's unwritable file must not drag the batch down.
func TestDuplicateOnlyNodeDoesNotBlockOtherNode(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase

	a1 := hb("alpha", 1, receive.Add(-2*time.Minute), "1.0", 100, 0)
	b1 := hb("beta", 1, receive.Add(-2*time.Minute), "1.0", 200, 0)
	if _, _, err := store.Submit([]Heartbeat{a1, b1}, receive); err != nil {
		t.Fatal(err)
	}
	aPath := store.nodePath("alpha")
	aBefore := readFileBytes(t, aPath)

	// alpha's file cannot be re-saved; beta's seq 2 is genuinely new.
	aCalls := failWritesTo(t, aPath)
	b2 := hb("beta", 2, receive.Add(-time.Minute), "1.0", 201, 1)
	newC, dupC, err := store.Submit([]Heartbeat{a1, a1, b2}, receive)
	if err != nil {
		t.Fatalf("beta's new record must save although alpha cannot be re-saved: %v", err)
	}
	if newC != 1 || dupC != 2 {
		t.Errorf("new=%d duplicate=%d, want new=1 duplicate=2", newC, dupC)
	}
	if *aCalls != 0 {
		t.Fatalf("alpha was re-saved (%d attempt) although it gained nothing", *aCalls)
	}

	// beta's new heartbeat is queryable; alpha's history is exactly as saved.
	assertHistory(t, dir, "beta", []Heartbeat{b1, b2})
	assertHistory(t, dir, "alpha", []Heartbeat{a1})
	if after := readFileBytes(t, aPath); string(after) != string(aBefore) {
		t.Errorf("alpha's file changed although all its records were duplicates")
	}
}

// TestDuplicateOnlyLongNodeIDIsNotResaved applies the same no-rewrite rule to
// the slot layout: a long id whose batch is all duplicates must not be
// re-saved, must not be mistaken for a new node, and must not gain a legacy
// nodes/ file.
func TestDuplicateOnlyLongNodeIDIsNotResaved(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase

	id := strings.Repeat("长节点-", 32) // 128 bytes: a slot, not a legacy name
	if !isLongID(id) {
		t.Fatal("test id must be long")
	}
	r1 := hb(id, 1, receive.Add(-2*time.Minute), "1.0", 100, 0)
	r2 := hb(id, 2, receive.Add(-time.Minute), "1.0", 101, 2)
	if _, _, err := store.Submit([]Heartbeat{r1, r2}, receive); err != nil {
		t.Fatal(err)
	}
	path := store.nodePath(id)
	before := readFileBytes(t, path)

	calls := failWritesTo(t, path)
	newC, dupC, err := store.Submit([]Heartbeat{r2, r1, r2}, receive)
	if err != nil {
		t.Fatalf("long-id duplicate-only resubmit must succeed without a re-save: %v", err)
	}
	if newC != 0 || dupC != 3 {
		t.Errorf("new=%d duplicate=%d, want new=0 duplicate=3 (the long id is the same node, not a new one)", newC, dupC)
	}
	if *calls != 0 {
		t.Fatalf("long-id duplicate-only node was re-saved (%d attempt)", *calls)
	}
	assertHistory(t, dir, id, []Heartbeat{r1, r2})
	if after := readFileBytes(t, path); string(after) != string(before) {
		t.Errorf("long-id slot bytes changed although nothing was new")
	}

	// Still exactly one slot file and no legacy file: the duplicate was not
	// filed under a second identity.
	slots, err := os.ReadDir(store.slotsDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(slots) != 1 {
		t.Errorf("expected exactly the one slot file, found %d entries", len(slots))
	}
	nodes, err := os.ReadDir(store.nodesDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 0 {
		t.Errorf("long id must never gain a legacy nodes/ file, found %d entries", len(nodes))
	}
}

// TestDuplicateResubmitStillChecksStoredData pins the boundary of the rule:
// skipping the re-save never skips reading and verifying what is already
// stored. Same identity with different content is a conflict; a damaged or
// foreign-owned file is corruption. Every rejection reports new=0 dup=0,
// saves nothing — not even another node's legitimate new record in the same
// batch — and leaves the stored bytes untouched.
func TestDuplicateResubmitStillChecksStoredData(t *testing.T) {
	receive := testBase

	setup := func(t *testing.T) (*Store, string, Heartbeat, Heartbeat) {
		t.Helper()
		store, err := OpenStore(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		saved := hb("n1", 1, receive.Add(-2*time.Minute), "1.0", 100, 0)
		other := hb("other", 1, receive.Add(-2*time.Minute), "1.0", 200, 0)
		if _, _, err := store.Submit([]Heartbeat{saved, other}, receive); err != nil {
			t.Fatal(err)
		}
		return store, store.Dir(), saved, other
	}

	// fresh2 is a legitimate new record for a second node, riding along in
	// each rejected batch to prove the rejection saves nothing at all.
	fresh2 := hb("fresh", 1, receive.Add(-time.Minute), "1.0", 300, 0)

	t.Run("same identity different content is a conflict", func(t *testing.T) {
		store, dir, saved, _ := setup(t)
		conflict := hb("n1", 1, saved.CollectedAt, "2.0", 100, 0) // same node+seq, different version
		newC, dupC, err := store.Submit([]Heartbeat{conflict, fresh2}, receive)
		if err == nil || !strings.Contains(err.Error(), "conflicting record") {
			t.Fatalf("expected a conflict rejection, got %v", err)
		}
		if newC != 0 || dupC != 0 {
			t.Errorf("rejected batch must report new=0 duplicate=0, got %d/%d", newC, dupC)
		}
		assertHistory(t, dir, "n1", []Heartbeat{saved})
		assertHistory(t, dir, "fresh", nil)
	})

	t.Run("damaged file is corruption, not a duplicate", func(t *testing.T) {
		store, dir, saved, _ := setup(t)
		if err := os.WriteFile(store.nodePath("n1"), []byte(`{bad`), 0o644); err != nil {
			t.Fatal(err)
		}
		// The input looks like a plain retransmit of the saved record.
		newC, dupC, err := store.Submit([]Heartbeat{saved, fresh2}, receive)
		if err == nil || !IsCorrupt(err) {
			t.Fatalf("a damaged file must be reported as corruption, got %v", err)
		}
		if newC != 0 || dupC != 0 {
			t.Errorf("rejected batch must report new=0 duplicate=0, got %d/%d", newC, dupC)
		}
		assertHistory(t, dir, "fresh", nil)
	})

	t.Run("foreign-owned file is corruption, not a duplicate", func(t *testing.T) {
		store, dir, saved, other := setup(t)
		// Another node's complete, valid file replaces n1's file.
		copyFileBytes(t, store, "n1", "other")
		planted := readFileBytes(t, store.nodePath("n1"))
		newC, dupC, err := store.Submit([]Heartbeat{saved, fresh2}, receive)
		if err == nil || !IsCorrupt(err) {
			t.Fatalf("a foreign-owned file must be reported as corruption, got %v", err)
		}
		if newC != 0 || dupC != 0 {
			t.Errorf("rejected batch must report new=0 duplicate=0, got %d/%d", newC, dupC)
		}
		if after := readFileBytes(t, store.nodePath("n1")); string(after) != string(planted) {
			t.Errorf("the refused submit must not overwrite the misowned file")
		}
		assertHistory(t, dir, "other", []Heartbeat{other})
		assertHistory(t, dir, "fresh", nil)
	})

	t.Run("long id slot keeps the ownership check", func(t *testing.T) {
		store, err := OpenStore(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		longA := strings.Repeat("alpha-node-", 13) + "A"
		longB := strings.Repeat("beta-node--", 13) + "B"
		if !isLongID(longA) || !isLongID(longB) {
			t.Fatal("test ids must be long")
		}
		savedA := hb(longA, 1, receive.Add(-2*time.Minute), "1.0", 100, 0)
		if _, _, err := store.Submit([]Heartbeat{
			savedA,
			hb(longB, 1, receive.Add(-2*time.Minute), "1.0", 200, 0),
		}, receive); err != nil {
			t.Fatal(err)
		}
		// longB's valid file planted at longA's slot: the slot layout must not
		// skip the ownership check just because the resubmit looks duplicate.
		copyFileBytes(t, store, longA, longB)
		planted := readFileBytes(t, store.nodePath(longA))
		newC, dupC, err := store.Submit([]Heartbeat{savedA, fresh2}, receive)
		if err == nil || !IsCorrupt(err) {
			t.Fatalf("a foreign-owned slot must be reported as corruption, got %v", err)
		}
		if newC != 0 || dupC != 0 {
			t.Errorf("rejected batch must report new=0 duplicate=0, got %d/%d", newC, dupC)
		}
		if after := readFileBytes(t, store.nodePath(longA)); string(after) != string(planted) {
			t.Errorf("the refused submit must not overwrite the misowned slot")
		}
		assertHistory(t, store.Dir(), "fresh", nil)
	})
}
