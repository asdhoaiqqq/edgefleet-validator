package edgefleet

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// These tests protect storage and querying of long node ids. A node id is
// arbitrary legal Unicode text of any length: the old per-node hex file name
// doubles the id's byte length, so 126 ASCII characters (or 42 Han characters)
// already exceed a 255-byte local file-name limit and made submit fail and
// queries for the unsaved node look like read failures. Long ids now use a
// fixed-length, content-addressed slot file; node identity nevertheless stays
// the user's full text, with the same ownership, dedup and conflict rules.

// slotPath mirrors the store's long-id slot naming.
func slotPath(store *Store, nodeID string) string {
	sum := sha256.Sum256([]byte(nodeID))
	return filepath.Join(store.slotsDir, hex.EncodeToString(sum[:])+".json")
}

// isLongID reports whether the id uses a slot rather than a legacy hex name.
func isLongID(nodeID string) bool {
	return len(hex.EncodeToString([]byte(nodeID))+".json") > maxNodeFileNameBytes
}

func TestLongNodeIDStorageLayout(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	short := "n1"
	shortLoc := store.nodeLocationFor(short)
	if filepath.Dir(shortLoc.path) != store.nodesDir {
		t.Fatalf("short id must stay under nodes/, got %s", shortLoc.path)
	}
	if filepath.Base(shortLoc.path) != hex.EncodeToString([]byte(short))+".json" {
		t.Fatalf("short id file name changed: %s", shortLoc.path)
	}

	// Boundary ids: 125 id bytes -> a 255-byte hex name (still legacy, exactly
	// the largest name the old code could ever have written); 126 id bytes ->
	// 257 bytes (would fail) -> a fixed-length slot.
	boundaryLegacy := strings.Repeat("a", 125)
	boundarySlot := strings.Repeat("a", 126)
	if isLongID(boundaryLegacy) {
		t.Fatal("125-byte id has a 255-byte name and must keep its legacy path")
	}
	if !isLongID(boundarySlot) {
		t.Fatal("126-byte id must use a slot")
	}
	legacyLoc := store.nodeLocationFor(boundaryLegacy)
	if filepath.Dir(legacyLoc.path) != store.nodesDir ||
		filepath.Base(legacyLoc.path) != hex.EncodeToString([]byte(boundaryLegacy))+".json" {
		t.Fatalf("boundary legacy id moved: %s", legacyLoc.path)
	}

	for _, id := range []string{boundarySlot, strings.Repeat("汉", 42), strings.Repeat("x", 4096)} {
		loc := store.nodeLocationFor(id)
		if filepath.Dir(loc.path) != store.slotsDir {
			t.Fatalf("long id (bytes=%d) must live under slots/, got %s", len(id), loc.path)
		}
		base := filepath.Base(loc.path)
		if len(base) != 64+len(".json") {
			t.Errorf("slot name %q is not the fixed 64-hex-char length", base)
		}
		if base != filepath.Base(slotPath(store, id)) {
			t.Errorf("slot name mismatch for id bytes=%d", len(id))
		}
		if loc.path == store.nodeLocationFor(id+"x").path {
			t.Errorf("id and id+%q share a slot: storage merges distinct nodes", "x")
		}
	}
	for _, id := range []string{boundarySlot, strings.Repeat("汉", 42)} {
		// The slot name is opaque hex and cannot embed the id text; a long run
		// of the id's characters must not appear in a 64-hex-char hash name
		// (hanzi cannot occur in hex at all; ten repeated hex chars by chance
		// is negligible).
		base := filepath.Base(store.nodePath(id))
		r := []rune(id)[0]
		var marker string
		if r < 128 {
			marker = strings.Repeat(string(r), 10)
		} else {
			marker = string(r)
		}
		if strings.Contains(base, marker) {
			t.Errorf("slot name %q appears to embed id text", base)
		}
	}
}

func TestLongNodeIDsSaveAndQuery(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase

	cases := []struct {
		name string
		id   string
	}{
		{"126 ascii", strings.Repeat("a", 126)},
		{"127 ascii", strings.Repeat("b", 127)},
		{"42 hanzi", strings.Repeat("汉", 42)},
		{"hanzi and emoji", strings.Repeat("节", 41) + "😀"},
		{"spaces hanzi emoji", strings.Repeat(" ", 61) + strings.Repeat("汉", 22) + "😀"},
		{"very long ascii", strings.Repeat("edge-", 200)},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if !isLongID(tc.id) {
				t.Fatalf("test id (%d bytes) is not a long id", len(tc.id))
			}
			r := hb(tc.id, 1, receive.Add(-time.Second), "v"+tc.name, int64(100+i), int64(i))
			newC, dupC, err := store.Submit([]Heartbeat{r}, receive)
			if err != nil {
				t.Fatalf("long id submit failed: %v", err)
			}
			if newC != 1 || dupC != 0 {
				t.Fatalf("new=%d dup=%d, want 1/0", newC, dupC)
			}
			// The record is in a slot whose name is short enough for any local
			// file system, and no over-long legacy name was created.
			info, err := os.Stat(store.nodePath(tc.id))
			if err != nil {
				t.Fatalf("slot not written: %v", err)
			}
			if len(info.Name()) > 255 {
				t.Errorf("slot name length %d still exceeds 255", len(info.Name()))
			}
			legacyEntries, err := os.ReadDir(store.nodesDir)
			if err != nil {
				t.Fatal(err)
			}
			for _, e := range legacyEntries {
				if len(e.Name()) > maxNodeFileNameBytes {
					t.Errorf("long id must not create an over-long nodes/ name: %d-byte name", len(e.Name()))
				}
			}

			// Health reads this node's own telemetry, selected by greatest seq.
			got, err := store.Health(tc.id, receive, "v"+tc.name, 99)
			if err != nil {
				t.Fatalf("health failed: %v", err)
			}
			if got.NodeID != tc.id || got.Status != "online" || got.Seq != 1 ||
				got.Version != "v"+tc.name || got.Height != int64(100+i) || got.Missed != int64(i) {
				t.Errorf("health misread the node or its telemetry: %+v", got)
			}

			// History round-trips the full id and record.
			hist, err := store.History(tc.id)
			if err != nil || len(hist) != 1 || !hist[0].Equal(r) {
				t.Fatalf("history broken: %+v err=%v", hist, err)
			}
		})
	}

	// A fresh store instance on the same directory sees the same data.
	reopened, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	got, err := reopened.Health(cases[1].id, receive, "v127 ascii", 99)
	if err != nil || got.Height != 101 {
		t.Fatalf("long id data not readable after restart: %+v err=%v", got, err)
	}
}

func TestLongNodeIDUnsavedQueriesAreEmptyNotFailures(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{strings.Repeat("a", 126), strings.Repeat("汉", 42)} {
		r, err := store.Health(id, testBase, "1.0", 0)
		if err != nil {
			t.Fatalf("health on a never-saved long id must not be a read failure: %v", err)
		}
		if r.Status != "notelemetry" || r.NodeID != id || len(r.Findings) != 1 || r.Findings[0] != "无遥测" {
			t.Errorf("want notelemetry for %q, got %+v", id, r)
		}
		hist, err := store.History(id)
		if err != nil || len(hist) != 0 {
			t.Fatalf("history on unsaved long id must be empty: %+v err=%v", hist, err)
		}
		// A baseline query still has no anchor and must say so.
		if _, err := store.HealthSince(id, testBase, "1.0", 0, 1); err == nil ||
			!strings.Contains(err.Error(), "no heartbeats") {
			t.Fatalf("baseline query on unsaved long id must report no heartbeats, got %v", err)
		}
		// Querying must not have created the slot.
		if _, err := os.Stat(slotPath(store, id)); !os.IsNotExist(err) {
			t.Errorf("querying an unsaved long id created its slot: %v", err)
		}
	}
}

func TestLongNodeIDsSharingLongPrefixStayDistinct(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase
	prefix := strings.Repeat("shared-long-prefix-", 8) // 152 bytes
	idA := prefix + "A"
	idB := prefix + "B"
	if !isLongID(idA) || !isLongID(idB) {
		t.Fatalf("test ids must be long: %d bytes", len(prefix)+1)
	}
	if store.nodePath(idA) == store.nodePath(idB) {
		t.Fatal("ids differing only at the end must not share a slot")
	}

	// Same seq, different content: two different nodes, both new.
	batch := []Heartbeat{
		hb(idA, 1, receive.Add(-time.Second), "1.0", 111, 1),
		hb(idB, 1, receive.Add(-time.Second), "1.0", 222, 2),
	}
	newC, dupC, err := store.Submit(batch, receive)
	if err != nil || newC != 2 || dupC != 0 {
		t.Fatalf("new=%d dup=%d err=%v, want 2/0/nil", newC, dupC, err)
	}

	for _, tc := range []struct {
		id     string
		height int64
		missed int64
	}{{idA, 111, 1}, {idB, 222, 2}} {
		r, err := store.Health(tc.id, receive, "1.0", 99)
		if err != nil {
			t.Fatalf("health %q failed: %v", tc.id, err)
		}
		if r.NodeID != tc.id || r.Height != tc.height || r.Missed != tc.missed || r.Seq != 1 {
			t.Errorf("node %q read the other node's data: %+v", tc.id, r)
		}
		hist, err := store.History(tc.id)
		if err != nil || len(hist) != 1 || hist[0].NodeID != tc.id {
			t.Errorf("history for %q leaked another node: %+v err=%v", tc.id, hist, err)
		}
	}

	// The same seq submitted again to each node is a per-node duplicate; a
	// different version is a per-node conflict — nodes never merge.
	newC, dupC, err = store.Submit([]Heartbeat{
		hb(idA, 1, receive.Add(-time.Second), "1.0", 111, 1),
		hb(idB, 1, receive.Add(-time.Second), "1.0", 222, 2),
	}, receive)
	if err != nil || newC != 0 || dupC != 2 {
		t.Fatalf("resubmit: new=%d dup=%d err=%v, want 0/2/nil", newC, dupC, err)
	}
	_, _, err = store.Submit([]Heartbeat{hb(idA, 1, receive.Add(-time.Second), "2.0", 111, 1)}, receive)
	if err == nil || !strings.Contains(err.Error(), "conflicting record") {
		t.Fatalf("cross-content resubmit to idA must conflict, got %v", err)
	}
	// idB is untouched by idA's conflict.
	if r, err := store.Health(idB, receive, "1.0", 99); err != nil || r.Height != 222 {
		t.Fatalf("idB affected by idA conflict: %+v err=%v", r, err)
	}
}

func TestLongNodeIDLiteralAndJSONEscapesAreSameNode(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	// 122 ASCII bytes + a 4-byte emoji = 126 bytes: long, and the emoji is
	// written below as a JSON surrogate-pair escape.
	id := strings.Repeat("a", 122) + "😀"
	if !isLongID(id) {
		t.Fatal("test id must be long")
	}

	literalJSON, err := json.Marshal([]Heartbeat{
		hb(id, 1, testBase.Add(-time.Second), "1.0", 1, 0),
	})
	if err != nil {
		t.Fatal(err)
	}
	records, err := ParseHeartbeats(literalJSON, testBase)
	if err != nil {
		t.Fatalf("literal form rejected: %v", err)
	}
	if newC, dupC, err := store.Submit(records, testBase); err != nil || newC != 1 || dupC != 0 {
		t.Fatalf("literal submit: new=%d dup=%d err=%v, want 1/0/nil", newC, dupC, err)
	}

	// The emoji written with a JSON surrogate-pair escape denotes the same id.
	bs := "\\"
	escaped := `[{"node":"` + strings.Repeat("a", 122) + bs + `uD83D` + bs + `uDE00",` +
		`"seq":1,"collected_at":"2026-10-01T11:59:59Z","version":"1.0","height":1,"missed":0}]`
	records, err = ParseHeartbeats([]byte(escaped), testBase)
	if err != nil {
		t.Fatalf("escaped form rejected: %v", err)
	}
	if records[0].NodeID != id {
		t.Fatalf("escaped id decoded to %q, want %q", records[0].NodeID, id)
	}
	if newC, dupC, err := store.Submit(records, testBase); err != nil || newC != 0 || dupC != 1 {
		t.Fatalf("escaped submit must hit the same node as a duplicate: new=%d dup=%d err=%v", newC, dupC, err)
	}
	hist, err := store.History(id)
	if err != nil || len(hist) != 1 {
		t.Fatalf("the two spellings must not split the node: %+v err=%v", hist, err)
	}
}

func TestLongNodeIDHistoryOrderHealthSelectionAndBaseline(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase
	id := strings.Repeat("长节点-", 30) // 120 bytes... ensure long
	if !isLongID(id) {
		id = strings.Repeat("长节点-", 32) // 128 bytes
	}
	batch := []Heartbeat{
		hb(id, 3, receive.Add(-3*time.Second), "1.0", 300, 5),
		hb(id, 1, receive.Add(-30*time.Second), "1.0", 100, 1),
		hb(id, 5, receive.Add(-time.Second), "2.0", 500, 9),
	}
	if _, _, err := store.Submit(batch, receive); err != nil {
		t.Fatal(err)
	}

	hist, err := store.History(id)
	if err != nil {
		t.Fatal(err)
	}
	if len(hist) != 3 {
		t.Fatalf("history len=%d, want 3", len(hist))
	}
	for i, wantSeq := range []int64{1, 3, 5} {
		if hist[i].Seq != wantSeq || hist[i].NodeID != id {
			t.Errorf("position %d: %+v, want seq %d for the long node", i, hist[i], wantSeq)
		}
	}

	// Health is driven by the greatest-seq record only.
	r, err := store.Health(id, receive, "1.0", 3)
	if err != nil {
		t.Fatal(err)
	}
	if r.Seq != 5 || r.Version != "2.0" || r.Height != 500 || r.Missed != 9 {
		t.Errorf("health did not take all fields from seq 5: %+v", r)
	}
	joined := strings.Join(r.Findings, ";")
	if !strings.Contains(joined, "version skew") || !strings.Contains(joined, "missed duties above tolerance") {
		t.Errorf("findings wrong: %v", r.Findings)
	}

	// Baseline query: new missed between seq 3 (5) and seq 5 (9) is 4; a
	// tolerance of 4 must not alarm.
	since, err := store.HealthSince(id, receive, "1.0", 4, 3)
	if err != nil {
		t.Fatal(err)
	}
	if since.Seq != 5 || since.BaselineSeq != 3 || since.BaselineMissed != 5 ||
		!since.NewMissedKnown || since.NewMissed != 4 {
		t.Errorf("baseline result wrong: %+v", since)
	}
	for _, f := range since.Findings {
		if f == missedAboveToleranceFinding {
			t.Errorf("new missed equal to tolerance must not alarm: %v", since.Findings)
		}
	}
}

func TestLongNodeIDForeignFileAtSlotIsRefused(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase
	idA := strings.Repeat("alpha-node-", 13) + "A" // 144 bytes
	idB := strings.Repeat("beta-node--", 13) + "B" // 144 bytes
	shortC := "short-c"
	shortD := "short-d"
	if !isLongID(idA) || !isLongID(idB) {
		t.Fatal("long test ids are not long")
	}
	if _, _, err := store.Submit([]Heartbeat{
		hb(idA, 1, receive.Add(-time.Minute), "1.0", 100, 0),
		hb(idA, 2, receive.Add(-time.Second), "1.0", 101, 3),
		hb(idB, 1, receive.Add(-time.Second), "1.0", 200, 7),
		hb(shortC, 1, receive.Add(-time.Second), "1.0", 300, 0),
		hb(shortD, 1, receive.Add(-time.Second), "1.0", 400, 0),
	}, receive); err != nil {
		t.Fatal(err)
	}

	// Case 1: another long node's complete, valid file is planted at idA's
	// slot. Format and checksum are intact; the records name idB.
	slotA := store.nodePath(idA)
	copyFileBytes(t, store, idA, idB)

	check := func(name string, err error) {
		t.Helper()
		if err == nil || !IsCorrupt(err) {
			t.Fatalf("%s must refuse the foreign slot file as corruption, got %v", name, err)
		}
		msg := err.Error()
		for _, want := range []string{slotA, "record 1", fmt.Sprintf("%q", idA), fmt.Sprintf("%q", idB)} {
			if !strings.Contains(msg, want) {
				t.Errorf("%s error must contain %s: %v", name, want, err)
			}
		}
	}
	_, err = store.Health(idA, receive, "1.0", 5)
	check("health", err)
	_, err = store.HealthSince(idA, receive, "1.0", 5, 1)
	check("health with baseline", err)
	_, err = store.History(idA)
	check("history", err)

	// It must not be mistaken for "no telemetry".
	if r, _ := store.Health(idA, receive.Add(time.Second), "1.0", 5); r.Status == "notelemetry" {
		t.Errorf("foreign file must not read as no telemetry")
	}

	// A continued submit touching idA fails the whole batch atomically and
	// must not overwrite, delete or replace the planted file; idB and the
	// short nodes are unaffected.
	planted, err := os.ReadFile(slotA)
	if err != nil {
		t.Fatal(err)
	}
	newC, dupC, err := store.Submit([]Heartbeat{
		hb(idA, 9, receive.Add(-time.Second), "1.0", 999, 0),
		hb(shortD, 2, receive.Add(-time.Second), "1.0", 401, 0),
		hb("fresh-node", 1, receive.Add(-time.Second), "1.0", 1, 0),
	}, receive)
	if err == nil || !IsCorrupt(err) {
		t.Fatalf("submit must refuse a batch touching the misowned slot, got %v", err)
	}
	if newC != 0 || dupC != 0 {
		t.Errorf("rejected batch must report no counts, got new=%d dup=%d", newC, dupC)
	}
	after, err := os.ReadFile(slotA)
	if err != nil || string(after) != string(planted) {
		t.Errorf("the refused submit must not modify the misowned slot")
	}
	if r, err := store.Health(idB, receive, "1.0", 99); err != nil || r.Height != 200 || r.Missed != 7 {
		t.Errorf("the copied-from long node must stay queryable: %+v err=%v", r, err)
	}
	if r, err := store.Health(shortD, receive, "1.0", 0); err != nil || r.Seq != 1 {
		t.Errorf("short node in the rejected batch must be unchanged: %+v err=%v", r, err)
	}

	// Case 2: a short node's valid file planted in a long node's slot is
	// refused by the record-vs-queried-id ownership check.
	copyFileBytes(t, store, idA, shortC)
	if _, err := loadNodeFileFor(store, idA); err == nil || !IsCorrupt(err) {
		t.Fatalf("short node file in long slot must be corrupt, got %v", err)
	}

	// Case 3: a long node's valid file planted on a short node's legacy name
	// is refused; the legacy name decode and the records disagree.
	copyFileBytes(t, store, shortD, idB)
	if _, err := loadNodeFileFor(store, shortD); err == nil || !IsCorrupt(err) {
		t.Fatalf("long node file under a short node name must be corrupt, got %v", err)
	}
	if _, err := store.History(shortD); err == nil || !IsCorrupt(err) {
		t.Errorf("history on the misowned legacy name must refuse, got %v", err)
	}
}

func TestLongNodeIDGarbageSlotIsCorruptNotNoTelemetry(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase
	id := strings.Repeat("g", 200)
	if err := os.WriteFile(store.nodePath(id), []byte("not json at all"), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, fn := range []func() error{
		func() error { _, e := store.Health(id, receive, "1.0", 0); return e },
		func() error { _, e := store.HealthSince(id, receive, "1.0", 0, 1); return e },
		func() error { _, e := store.History(id); return e },
		func() error {
			_, _, e := store.Submit([]Heartbeat{hb(id, 1, receive.Add(-time.Second), "1.0", 1, 0)}, receive)
			return e
		},
	} {
		if err := fn(); err == nil || !IsCorrupt(err) {
			t.Fatalf("garbage slot must be reported as corruption, got %v", err)
		}
	}
}

func TestLegacyShortIDsRemainAtOriginalLocation(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase

	// The largest id the old layout could ever have written: a 255-byte hex
	// name (125 id bytes), plus 41 Han characters (123 bytes -> 251-byte name).
	ids := []string{
		"n1",
		"节点/甲",
		strings.Repeat("a", 125),
		strings.Repeat("汉", 41),
	}
	var batch []Heartbeat
	for i, id := range ids {
		batch = append(batch, hb(id, 1, receive.Add(-time.Second), "1.0", int64(i), 0))
	}
	if _, _, err := store.Submit(batch, receive); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(store.slotsDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("short ids must create no slot files, found %d", len(entries))
	}
	for i, id := range ids {
		want := filepath.Join(store.nodesDir, hex.EncodeToString([]byte(id))+".json")
		if store.nodePath(id) != want {
			t.Errorf("short id %q moved away from %s", id, want)
		}
		if _, err := os.Stat(want); err != nil {
			t.Errorf("short id data missing at original location: %v", err)
		}
		r, err := store.Health(id, receive, "1.0", 0)
		if err != nil || r.Height != int64(i) {
			t.Errorf("short id %q no longer reads in place: %+v err=%v", id, r, err)
		}
	}

	// Simulate an upgrade on a data directory written by the old code: the
	// file already sits at nodes/hex(id).json and a freshly opened store must
	// read and append to it in place, without moving anything.
	reopened, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := reopened.Submit([]Heartbeat{hb("n1", 2, receive.Add(-time.Second), "1.0", 99, 0)}, receive); err != nil {
		t.Fatal(err)
	}
	hist, err := reopened.History("n1")
	if err != nil || len(hist) != 2 || hist[1].Seq != 2 {
		t.Fatalf("append to legacy file in place failed: %+v err=%v", hist, err)
	}
	if _, err := os.Stat(slotPath(reopened, "n1")); !os.IsNotExist(err) {
		t.Errorf("short id must never gain a slot file")
	}
}
