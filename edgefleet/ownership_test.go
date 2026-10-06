package edgefleet

import (
	"os"
	"strings"
	"testing"
	"time"
)

// These tests protect node ownership of stored heartbeat files. Each node's
// file must only carry that node's own records: a file copied over from
// another node keeps a valid format and checksum, yet its records are not
// this node's telemetry and must be refused as corruption — on every query
// and on submit — rather than read as the wrong node's height, version and
// missed count.

// copyFileBytes copies src node's stored file over dst node's file path,
// simulating an operator restoring the wrong node's backup.
func copyFileBytes(t *testing.T, store *Store, dst, src string) {
	t.Helper()
	data, err := os.ReadFile(store.nodePath(src))
	if err != nil {
		t.Fatalf("test setup: read source file: %v", err)
	}
	if err := os.WriteFile(store.nodePath(dst), data, 0o644); err != nil {
		t.Fatalf("test setup: plant copied file: %v", err)
	}
}

func TestCopiedNodeFileIsRefusedAsCorrupt(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase
	if _, _, err := store.Submit([]Heartbeat{
		hb("乙", 1, receive.Add(-time.Minute), "1.0", 100, 0),
		hb("乙", 2, receive.Add(-time.Second), "1.0", 101, 3),
	}, receive); err != nil {
		t.Fatal(err)
	}
	copyFileBytes(t, store, "甲", "乙")

	path := store.nodePath("甲")
	check := func(name string, err error) {
		t.Helper()
		if err == nil || !IsCorrupt(err) {
			t.Fatalf("%s must refuse the copied file as corruption, got %v", name, err)
		}
		msg := err.Error()
		for _, want := range []string{path, "record 1", `"甲"`, `"乙"`} {
			if !strings.Contains(msg, want) {
				t.Errorf("%s error must contain %s: %v", name, want, err)
			}
		}
	}

	_, err = store.Health("甲", receive, "1.0", 5)
	check("health", err)
	_, err = store.HealthSince("甲", receive, "1.0", 5, 1)
	check("health with baseline", err)
	_, err = store.History("甲")
	check("history", err)

	// The real owner's data is untouched and still queryable.
	r, err := store.Health("乙", receive, "1.0", 5)
	if err != nil {
		t.Fatalf("the copied-from node must stay queryable: %v", err)
	}
	if r.Seq != 2 || r.Height != 101 || r.Missed != 3 {
		t.Errorf("source node telemetry misread: %+v", r)
	}
	hist, err := store.History("乙")
	if err != nil || len(hist) != 2 {
		t.Errorf("source node history broken: len=%d err=%v", len(hist), err)
	}
}

func TestForeignRecordAnywhereInFileIsCorrupt(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	path := store.nodePath("n1")
	at := func(sec int) time.Time { return testBase.Add(time.Duration(sec) * time.Second) }

	cases := []struct {
		name    string
		records []Heartbeat
		wantRec string
	}{
		// The foreign record is the oldest history; the latest record names
		// the right node. Still corrupt.
		{"foreign oldest record", []Heartbeat{
			hb("n2", 1, at(1), "1.0", 10, 0),
			hb("n1", 2, at(2), "1.0", 11, 0),
			hb("n1", 3, at(3), "1.0", 12, 1),
		}, "record 1"},
		// The foreign record sits in the middle, before any baseline a
		// missed-since query might use.
		{"foreign middle record", []Heartbeat{
			hb("n1", 1, at(1), "1.0", 10, 0),
			hb("n2", 2, at(2), "1.0", 11, 0),
			hb("n1", 3, at(3), "1.0", 12, 1),
		}, "record 2"},
		// Only the latest record is foreign.
		{"foreign latest record", []Heartbeat{
			hb("n1", 1, at(1), "1.0", 10, 0),
			hb("n2", 2, at(2), "1.0", 11, 0),
		}, "record 2"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := writeNodeFile(path, tc.records); err != nil {
				t.Fatal(err)
			}
			_, err := loadNodeFileFor(store, "n1")
			if err == nil || !IsCorrupt(err) {
				t.Fatalf("read must report corruption, got %v", err)
			}
			msg := err.Error()
			for _, want := range []string{path, tc.wantRec, `"n1"`, `"n2"`} {
				if !strings.Contains(msg, want) {
					t.Errorf("error must contain %s: %v", want, err)
				}
			}

			// Every query form refuses: plain health, baseline health (even
			// anchored after the foreign record) and history.
			if _, err := store.Health("n1", testBase.Add(time.Hour), "1.0", 5); err == nil || !IsCorrupt(err) {
				t.Errorf("health must refuse, got %v", err)
			}
			if _, err := store.HealthSince("n1", testBase.Add(time.Hour), "1.0", 5, 3); err == nil || !IsCorrupt(err) {
				t.Errorf("health with baseline after the foreign record must refuse, got %v", err)
			}
			if _, err := store.History("n1"); err == nil || !IsCorrupt(err) {
				t.Errorf("history must refuse, got %v", err)
			}
		})
	}
}

func TestSubmitRejectsBatchTouchingMisownedFile(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase
	if _, _, err := store.Submit([]Heartbeat{
		hb("good", 1, receive.Add(-time.Minute), "1.0", 100, 0),
		hb("src", 1, receive.Add(-time.Minute), "1.0", 200, 0),
	}, receive); err != nil {
		t.Fatal(err)
	}
	// Plant src's file under bad's name: valid format and checksum, wrong owner.
	copyFileBytes(t, store, "bad", "src")

	badBefore, err := os.ReadFile(store.nodePath("bad"))
	if err != nil {
		t.Fatal(err)
	}
	goodBefore, err := os.ReadFile(store.nodePath("good"))
	if err != nil {
		t.Fatal(err)
	}

	// The batch spans the misowned node, a healthy existing node and a
	// first-time node: everything must be refused before any write.
	batch := []Heartbeat{
		hb("bad", 9, receive.Add(-time.Second), "1.0", 999, 0),
		hb("good", 2, receive.Add(-time.Second), "1.0", 101, 0),
		hb("fresh", 1, receive.Add(-time.Second), "1.0", 1, 0),
	}
	newCount, dupCount, err := store.Submit(batch, receive)
	if err == nil || !IsCorrupt(err) {
		t.Fatalf("submit must refuse the batch with a corruption error, got %v", err)
	}
	if newCount != 0 || dupCount != 0 {
		t.Errorf("rejected batch must report no counts, got new=%d dup=%d", newCount, dupCount)
	}

	badAfter, _ := os.ReadFile(store.nodePath("bad"))
	goodAfter, _ := os.ReadFile(store.nodePath("good"))
	if string(badAfter) != string(badBefore) {
		t.Errorf("misowned file must stay exactly as found: no append, delete, rename or migration")
	}
	if string(goodAfter) != string(goodBefore) {
		t.Errorf("healthy node file was rewritten during the rejected batch")
	}
	if _, err := os.Stat(store.nodePath("fresh")); !os.IsNotExist(err) {
		t.Errorf("fresh node file must not be created, stat err=%v", err)
	}

	// Unrelated nodes keep accepting submits and queries.
	if _, _, err := store.Submit([]Heartbeat{
		hb("good", 2, receive.Add(-time.Second), "1.0", 101, 0),
	}, receive); err != nil {
		t.Errorf("submit to a healthy node must work: %v", err)
	}
	if _, err := store.Health("src", receive, "1.0", 0); err != nil {
		t.Errorf("source node must stay queryable: %v", err)
	}
}

func TestUnusualNodeIDsKeepWorkingWithOwnershipCheck(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase
	ids := []string{"节点/甲", "edge node 1", "a/b/c", "空格 和/斜杠"}
	var batch []Heartbeat
	for i, id := range ids {
		batch = append(batch, hb(id, 1, receive.Add(-time.Second), "1.0", int64(i), 0))
	}
	if _, _, err := store.Submit(batch, receive); err != nil {
		t.Fatalf("legal node ids must submit: %v", err)
	}
	for i, id := range ids {
		r, err := store.Health(id, receive, "1.0", 0)
		if err != nil {
			t.Fatalf("health for %q must work: %v", id, err)
		}
		if r.NodeID != id || r.Height != int64(i) {
			t.Errorf("node %q misidentified: %+v", id, r)
		}
		hist, err := store.History(id)
		if err != nil || len(hist) != 1 || hist[0].NodeID != id {
			t.Errorf("history for %q broken: %+v err=%v", id, hist, err)
		}
	}
}
