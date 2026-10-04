package edgefleet

import (
	"os"
	"strings"
	"testing"
	"time"
)

// These tests pin the ownership trust boundary: a node file lives at a path
// derived from the node id, so every record in it must declare that same
// node. A complete, checksum-valid file copied wholesale from another node
// must not be served as the path-owner's telemetry — not partially, not as
// "no telemetry".

// writeForeignFile writes a structurally valid file (correct format marker
// and checksum) at the queried node's path, but its records declare other
// nodes. This models copying node 乙's intact file onto node 甲's path.
func writeForeignFile(t *testing.T, store *Store, pathOwner string, records []Heartbeat) {
	t.Helper()
	if err := writeNodeFile(store.nodePath(pathOwner), records); err != nil {
		t.Fatal(err)
	}
}

func TestForeignFileIsCorruptionNamingFilePositionAndNodes(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase
	path := store.nodePath("alpha")

	// File at alpha's path holds one of beta's heartbeats; format and
	// checksum are both valid for those records.
	writeForeignFile(t, store, "alpha", []Heartbeat{
		hb("beta", 1, receive.Add(-time.Minute), "9.9.9", 4242, 7),
	})

	_, err = loadNodeFile(path, "alpha")
	if err == nil || !IsCorrupt(err) {
		t.Fatalf("foreign telemetry must be corruption, got %v", err)
	}
	msg := err.Error()
	for _, want := range []string{path, "record 1", "alpha", "beta"} {
		if !strings.Contains(msg, want) {
			t.Errorf("error must mention %q, got: %v", want, err)
		}
	}
}

func TestForeignRecordBuriedInHistoryStillCorrupt(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase

	cases := []struct {
		name    string
		records []Heartbeat
		wantPos string
	}{
		{
			name: "foreign record is oldest, latest belongs to owner",
			records: []Heartbeat{
				hb("beta", 1, receive.Add(-3*time.Minute), "9.9.9", 1, 0),
				hb("alpha", 2, receive.Add(-2*time.Minute), "1.0", 2, 0),
				hb("alpha", 3, receive.Add(-time.Minute), "1.0", 3, 0),
			},
			wantPos: "record 1",
		},
		{
			name: "foreign record sits in the middle",
			records: []Heartbeat{
				hb("alpha", 1, receive.Add(-3*time.Minute), "1.0", 1, 0),
				hb("beta", 2, receive.Add(-2*time.Minute), "9.9.9", 2, 5),
				hb("alpha", 3, receive.Add(-time.Minute), "1.0", 3, 0),
			},
			wantPos: "record 2",
		},
		{
			name: "foreign record is newest despite correct older history",
			records: []Heartbeat{
				hb("alpha", 1, receive.Add(-2*time.Minute), "1.0", 1, 0),
				hb("beta", 2, receive.Add(-time.Minute), "9.9.9", 99, 9),
			},
			wantPos: "record 2",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			writeForeignFile(t, store, "alpha", tc.records)

			// Health must not show beta's height/version/missed as alpha's.
			if _, err := store.Health("alpha", receive, "1.0", 0); err == nil || !IsCorrupt(err) {
				t.Fatalf("health must refuse, got %v", err)
			}
			// A baseline anchored at a correct, later record still cannot
			// mask a foreign record before it.
			if _, err := store.HealthSince("alpha", receive, "1.0", 0, 3); err == nil || !IsCorrupt(err) {
				t.Fatalf("baseline health must refuse whole file, got %v", err)
			}
			if _, err := store.HealthSince("alpha", receive, "1.0", 0, 2); err == nil || !IsCorrupt(err) {
				t.Fatalf("baseline health must refuse whole file, got %v", err)
			}
			hist, err := store.History("alpha")
			if err == nil || !IsCorrupt(err) {
				t.Fatalf("history must refuse whole file, got hist=%v err=%v", hist, err)
			}
			if !strings.Contains(err.Error(), tc.wantPos) {
				t.Errorf("error must identify %s, got %v", tc.wantPos, err)
			}
		})
	}
}

func TestHealthDoesNotAdoptForeignTelemetry(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase
	// Beta's own, perfectly valid history copied onto alpha's path.
	writeForeignFile(t, store, "alpha", []Heartbeat{
		hb("beta", 5, receive.Add(-time.Second), "2.0.0-beta", 777, 13),
	})

	if _, err := store.Health("alpha", receive, "2.0.0-beta", 99); err == nil {
		t.Fatal("health must not return beta's telemetry under alpha")
	} else if !IsCorrupt(err) {
		t.Fatalf("want corruption error, got %v", err)
	}
}

func TestSubmitRejectsBatchTouchingForeignOwnedFile(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase

	// A clean node and a fresh node exist; the "alpha" path holds beta's
	// intact file.
	if _, _, err := store.Submit([]Heartbeat{
		hb("good", 1, receive.Add(-time.Minute), "1.0", 100, 0),
	}, receive); err != nil {
		t.Fatal(err)
	}
	writeForeignFile(t, store, "alpha", []Heartbeat{
		hb("beta", 1, receive.Add(-time.Minute), "9.9.9", 1, 0),
	})

	alphaPath := store.nodePath("alpha")
	alphaBefore, err := os.ReadFile(alphaPath)
	if err != nil {
		t.Fatal(err)
	}
	goodPath := store.nodePath("good")
	goodBefore, err := os.ReadFile(goodPath)
	if err != nil {
		t.Fatal(err)
	}

	// Batch touches the misplaced file (a new alpha heartbeat), the good
	// node and a brand new node. Nothing may be saved, no success/count
	// output semantics change (err != nil).
	batch := []Heartbeat{
		hb("alpha", 2, receive.Add(-time.Second), "1.0", 2, 0),
		hb("good", 2, receive.Add(-time.Second), "1.0", 101, 0),
		hb("fresh", 1, receive.Add(-time.Second), "1.0", 1, 0),
	}
	newCount, dupCount, err := store.Submit(batch, receive)
	if err == nil || !IsCorrupt(err) {
		t.Fatalf("submit must refuse whole batch, got new=%d dup=%d err=%v", newCount, dupCount, err)
	}
	if newCount != 0 || dupCount != 0 {
		t.Errorf("rejected batch must report no counts, got new=%d dup=%d", newCount, dupCount)
	}

	alphaAfter, err := os.ReadFile(alphaPath)
	if err != nil {
		t.Fatal(err)
	}
	goodAfter, err := os.ReadFile(goodPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(alphaAfter) != string(alphaBefore) {
		t.Errorf("misplaced file must stay exactly as found")
	}
	if string(goodAfter) != string(goodBefore) {
		t.Errorf("good node file must not be rewritten")
	}
	if _, err := os.Stat(store.nodePath("fresh")); !os.IsNotExist(err) {
		t.Errorf("fresh node file must not be created, stat err=%v", err)
	}
}

func TestForeignFileDoesNotAffectUnrelatedNodes(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase
	if _, _, err := store.Submit([]Heartbeat{
		hb("good", 1, receive.Add(-time.Minute), "1.0", 100, 0),
	}, receive); err != nil {
		t.Fatal(err)
	}
	writeForeignFile(t, store, "alpha", []Heartbeat{
		hb("beta", 1, receive.Add(-time.Minute), "9.9.9", 1, 0),
	})

	if hist, err := store.History("good"); err != nil || len(hist) != 1 {
		t.Fatalf("unrelated history must work: %v %v", hist, err)
	}
	if r, err := store.Health("good", receive, "1.0", 0); err != nil || r.Height != 100 {
		t.Fatalf("unrelated health must work: %+v %v", r, err)
	}
	if _, _, err := store.Submit([]Heartbeat{
		hb("good", 2, receive.Add(-time.Second), "1.0", 101, 0),
	}, receive); err != nil {
		t.Fatalf("unrelated submit must work: %v", err)
	}
	if _, _, err := store.Submit([]Heartbeat{
		hb("beta", 2, receive.Add(-time.Second), "9.9.9", 2, 0),
	}, receive); err != nil {
		t.Fatalf("submit to beta (whose own file is untouched elsewhere) must work: %v", err)
	}
}

func TestOwnershipAcceptsExactStringNodeIDs(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase

	// Distinct ids containing Chinese, spaces and slashes must not be
	// collapsed, trimmed or rewritten by the ownership check.
	ids := []string{"节点/甲", "节点/乙", "node with spaces", " node with spaces"}
	for i, id := range ids {
		h := Heartbeat{
			NodeID:      id,
			Seq:         int64(i + 1),
			CollectedAt: receive.Add(-time.Second),
			Version:     "1.0",
			Height:      int64(i),
		}
		if _, _, err := store.Submit([]Heartbeat{h}, receive); err != nil {
			t.Fatalf("submit for %q failed: %v", id, err)
		}
	}
	for _, id := range ids {
		hist, err := store.History(id)
		if err != nil {
			t.Fatalf("history for %q failed: %v", id, err)
		}
		if len(hist) != 1 || hist[0].NodeID != id {
			t.Fatalf("history for %q returned %+v", id, hist)
		}
	}

	// A file copied from "node with spaces" onto the leading-space node's
	// path differs by exactly that leading space and must still be refused.
	if err := writeNodeFile(store.nodePath(" node with spaces"), []Heartbeat{
		hb("node with spaces", 1, receive.Add(-time.Second), "1.0", 1, 0),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.History(" node with spaces"); err == nil || !IsCorrupt(err) {
		t.Fatalf("ids differing by surrounding whitespace must not merge, got %v", err)
	}
}

func TestEmptyAndMissingFilesKeepNoTelemetrySemantics(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase

	// No file at all.
	if r, err := store.Health("ghost", receive, "1.0", 0); err != nil || r.Status != "notelemetry" {
		t.Fatalf("missing file must be no telemetry, got %+v %v", r, err)
	}
	if hist, err := store.History("ghost"); err != nil || len(hist) != 0 {
		t.Fatalf("missing file history must be empty, got %v %v", hist, err)
	}

	// Existing file with an empty record set.
	if err := writeNodeFile(store.nodePath("empty"), []Heartbeat{}); err != nil {
		t.Fatal(err)
	}
	if r, err := store.Health("empty", receive, "1.0", 0); err != nil || r.Status != "notelemetry" {
		t.Fatalf("empty records must be no telemetry, got %+v %v", r, err)
	}
	if hist, err := store.History("empty"); err != nil || len(hist) != 0 {
		t.Fatalf("empty history must be empty slice, got %v %v", hist, err)
	}
}
