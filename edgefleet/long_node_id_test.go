package edgefleet

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// These tests cover node ids whose hex(file-name) encoding no longer fits the
// local 255-byte file name limit (126 ASCII characters, or 42 Han characters,
// already do). Such ids must submit, persist and query exactly like short ids:
// the complete original text stays the node identity, history stays ascending
// by seq, health uses the greatest-seq record, duplicate/conflict rules are
// unchanged, and a node with no records returns notelemetry / an empty history
// instead of a file-name or corruption error.

// longASCII returns an ASCII node id of exactly n characters.
func longASCII(n int) string { return strings.Repeat("a", n) }

// repeatRune returns a node id of n copies of r.
func repeatRune(r rune, n int) string { return strings.Repeat(string(r), n) }

// everyPathComponentShort reports that no component of p exceeds the local
// 255-byte file name limit.
func everyPathComponentShort(t *testing.T, p string) bool {
	t.Helper()
	for _, c := range strings.Split(filepath.Clean(p), string(filepath.Separator)) {
		if len(c) > 255 {
			t.Errorf("path component %q is %d bytes, over the 255-byte file name limit", c, len(c))
			return false
		}
	}
	return true
}

func TestLongNodeIDsSubmitAndQuery(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase

	// 126 ASCII -> 252 hex chars -> 257-byte file name: the first length that
	// was previously unsavable. 125 ASCII (255-byte name exactly) stays on the
	// legacy flat path. The Han and mixed ids cross the limit as well, with the
	// chunk boundary splitting multi-byte UTF-8 sequences. The 300-ASCII id
	// spans several nested chunks.
	ids := []string{
		longASCII(125),
		longASCII(126),
		repeatRune('汉', 42),
		repeatRune('汉', 43),
		"😀 节 点 " + strings.Repeat("x", 120) + " 尾 部",
		longASCII(300),
	}
	for i, id := range ids {
		path := store.nodePath(id)
		if !strings.HasSuffix(path, ".json") {
			t.Fatalf("path for long id %d must end in .json: %s", i, path)
		}
		everyPathComponentShort(t, path)
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("no file should exist before first save for id %d, stat err=%v", i, err)
		}
		if _, _, err := store.Submit([]Heartbeat{
			hb(id, 1, receive.Add(-2*time.Minute), "1.0", int64(100+i), 0),
			hb(id, 3, receive.Add(-time.Second), "1.2", int64(300+i), 2),
		}, receive); err != nil {
			t.Fatalf("long id %d submit: %v", i, err)
		}
	}

	// Reopening the store represents a platform restart: data is on disk and
	// must be found again by the same complete ids.
	reopened, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	for i, id := range ids {
		hist, err := reopened.History(id)
		if err != nil {
			t.Fatalf("history for long id %d: %v", i, err)
		}
		if len(hist) != 2 || hist[0].Seq != 1 || hist[1].Seq != 3 {
			t.Fatalf("long id %d history wrong: %+v", i, hist)
		}
		for _, r := range hist {
			if r.NodeID != id {
				t.Errorf("long id %d record misowned: %q", i, r.NodeID)
			}
		}

		// Health takes every field from the greatest-seq record, never from a
		// different node's file that shares the shard directory.
		r, err := reopened.Health(id, receive, "1.2", 2)
		if err != nil {
			t.Fatalf("health for long id %d: %v", i, err)
		}
		if r.Status != "online" || r.Seq != 3 || r.Version != "1.2" ||
			r.Height != int64(300+i) || r.Missed != 2 || r.NodeID != id {
			t.Errorf("long id %d health fields wrong: %+v", i, r)
		}
		if !r.CollectedAt.Equal(receive.Add(-time.Second)) {
			t.Errorf("long id %d collected_at wrong: %v", i, r.CollectedAt)
		}

		// The baseline query path reads the same file and same records.
		rb, err := reopened.HealthSince(id, receive, "1.2", 2, 1)
		if err != nil {
			t.Fatalf("health baseline for long id %d: %v", i, err)
		}
		if rb.Seq != 3 || rb.BaselineSeq != 1 || rb.NewMissed != 2 {
			t.Errorf("long id %d baseline result wrong: %+v", i, rb)
		}
	}

	// The exact 255-byte boundary id keeps its legacy flat location; every id
	// past it lives below at least one shard directory.
	flat := store.nodePath(longASCII(125))
	if filepath.Dir(flat) != store.nodesDir {
		t.Errorf("125-ASCII id must stay on the legacy flat path, got %s", flat)
	}
	if _, err := os.Stat(flat); err != nil {
		t.Errorf("legacy flat file missing: %v", err)
	}
	for _, id := range ids[1:] {
		if filepath.Dir(store.nodePath(id)) == store.nodesDir {
			t.Errorf("long id %q must use a sharded path", id)
		}
	}
}

func TestLongNodeIDDuplicateAndConflictRulesUnchanged(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase
	id := repeatRune('汉', 50)
	first := hb(id, 7, receive.Add(-time.Minute), "1.0", 100, 1)
	if _, _, err := store.Submit([]Heartbeat{first}, receive); err != nil {
		t.Fatal(err)
	}

	// Identical content at the same seq is a duplicate, including when the
	// long node text arrives entirely through JSON \uXXXX escapes (汉 is
	// U+6C49): the escaped form denotes exactly the same node identity.
	raw := `[{"node":"` + strings.Repeat("\\u6c49", 50) +
		`","seq":7,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":100,"missed":1}]`
	if strings.ContainsRune(raw, '汉') {
		t.Fatal("test setup: escaped payload must not contain the literal Han character")
	}
	recs, err := ParseHeartbeats([]byte(raw), receive)
	if err != nil {
		t.Fatalf("escaped long id rejected: %v", err)
	}
	if recs[0].NodeID != id {
		t.Fatalf("escaped form decoded to a different node: %q", recs[0].NodeID)
	}
	newC, dupC, err := store.Submit(recs, receive)
	if err != nil || newC != 0 || dupC != 1 {
		t.Fatalf("escaped resubmit: new=%d dup=%d err=%v, want 0/1/nil", newC, dupC, err)
	}

	// Different content at the same seq is a conflict and changes nothing.
	conflict := hb(id, 7, receive.Add(-time.Minute), "1.0", 101, 1)
	if _, _, err := store.Submit([]Heartbeat{conflict}, receive); err == nil {
		t.Fatal("conflicting long-id record must be rejected")
	}
	hist, err := store.History(id)
	if err != nil || len(hist) != 1 || hist[0].Height != 100 {
		t.Fatalf("conflict must not modify history: %+v err=%v", hist, err)
	}
}

func TestLongNodeIDWithoutRecordsIsNotAReadFailure(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase
	for _, id := range []string{longASCII(126), repeatRune('汉', 43), longASCII(400)} {
		r, err := store.Health(id, receive, "1.0", 0)
		if err != nil {
			t.Errorf("plain health for unsaved long id must not fail: %v", err)
		}
		if r.Status != "notelemetry" || r.NodeID != id {
			t.Errorf("unsaved long id result wrong: %+v", r)
		}
		hist, err := store.History(id)
		if err != nil || len(hist) != 0 {
			t.Errorf("unsaved long id history must be empty: %+v err=%v", hist, err)
		}
		// A baseline anchored at nothing is still an error, not telemetry.
		if _, err := store.HealthSince(id, receive, "1.0", 0, 1); err == nil {
			t.Errorf("baseline query for unsaved long id must fail")
		}
		// A mere failed/empty lookup never creates files or shard directories.
		if _, err := os.Stat(store.nodePath(id)); !os.IsNotExist(err) {
			t.Errorf("querying an unsaved long id created its file, stat err=%v", err)
		}
	}
}

func TestLongIDsSharingPrefixDifferingAtSuffixStayDistinct(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase
	prefix := strings.Repeat("shared-long-prefix-", 8) // 160 chars
	a := prefix + "A"
	b := prefix + "B"
	// Identical seq numbers; content differs. The shared shard directories
	// must not merge the nodes, and the differing tail character is what keeps
	// the file names distinct.
	if _, _, err := store.Submit([]Heartbeat{hb(a, 1, receive.Add(-time.Second), "1.0", 111, 0)}, receive); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Submit([]Heartbeat{hb(b, 1, receive.Add(-time.Second), "1.0", 222, 0)}, receive); err != nil {
		t.Fatal(err)
	}
	if store.nodePath(a) == store.nodePath(b) {
		t.Fatal("suffix-distinct long ids mapped to the same file")
	}
	for _, tc := range []struct {
		id     string
		height int64
	}{{a, 111}, {b, 222}} {
		hist, err := store.History(tc.id)
		if err != nil || len(hist) != 1 || hist[0].NodeID != tc.id || hist[0].Height != tc.height {
			t.Errorf("node %q history wrong: %+v err=%v", tc.id, hist, err)
		}
		r, err := store.Health(tc.id, receive, "1.0", 0)
		if err != nil || r.Height != tc.height || r.NodeID != tc.id {
			t.Errorf("node %q health wrong: %+v err=%v", tc.id, r, err)
		}
	}
}

// plantFileAtLongPath copies src node's stored file to dst node's (possibly
// sharded) path, creating the shard directories first.
func plantFileAtLongPath(t *testing.T, store *Store, dst, src string) {
	t.Helper()
	data, err := os.ReadFile(store.nodePath(src))
	if err != nil {
		t.Fatalf("test setup: read source file: %v", err)
	}
	dstPath := store.nodePath(dst)
	if err := os.MkdirAll(filepath.Dir(dstPath), 0o755); err != nil {
		t.Fatalf("test setup: create shard dirs: %v", err)
	}
	if err := os.WriteFile(dstPath, data, 0o644); err != nil {
		t.Fatalf("test setup: plant copied file: %v", err)
	}
}

func TestForeignFilePlantedAtLongNodePathIsCorrupt(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase
	a := longASCII(126)
	b := longASCII(127)
	c := strings.Repeat("c", 126)
	if _, _, err := store.Submit([]Heartbeat{
		hb(a, 1, receive.Add(-time.Minute), "1.0", 100, 0),
		hb(a, 2, receive.Add(-time.Second), "1.0", 101, 1),
		hb(c, 1, receive.Add(-time.Second), "1.0", 300, 0),
	}, receive); err != nil {
		t.Fatal(err)
	}
	// A's well-formed, checksum-valid file is copied into b's sharded path.
	plantFileAtLongPath(t, store, b, a)
	planted := store.nodePath(b)

	check := func(name string, err error) {
		t.Helper()
		if err == nil || !IsCorrupt(err) {
			t.Fatalf("%s must refuse the planted file as corruption, got %v", name, err)
		}
		msg := err.Error()
		for _, want := range []string{planted, "record 1", a, b} {
			if !strings.Contains(msg, want) {
				t.Errorf("%s error must contain %q: %v", name, want, err)
			}
		}
	}
	_, err = store.Health(b, receive, "1.0", 5)
	check("health", err)
	_, err = store.HealthSince(b, receive, "1.0", 5, 1)
	check("health baseline", err)
	_, err = store.History(b)
	check("history", err)

	// Submitting to b must refuse and leave the planted bytes exactly as found.
	before, err := os.ReadFile(planted)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Submit([]Heartbeat{
		hb(b, 3, receive.Add(-time.Second), "1.0", 200, 0),
	}, receive); err == nil || !IsCorrupt(err) {
		t.Fatalf("submit to the misowned long path must be refused, got %v", err)
	}
	after, err := os.ReadFile(planted)
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(before) {
		t.Errorf("misowned long-id file must not be overwritten or migrated")
	}

	// The real owners keep working, with only their own records.
	r, err := store.Health(a, receive, "1.0", 5)
	if err != nil || r.Seq != 2 || r.Height != 101 || r.NodeID != a {
		t.Errorf("source long node misread: %+v err=%v", r, err)
	}
	r, err = store.Health(c, receive, "1.0", 0)
	if err != nil || r.Height != 300 || r.NodeID != c {
		t.Errorf("other shard node misread: %+v err=%v", r, err)
	}
}

func TestNodeIDBeyondWholePathLimitIsNotCorruption(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase
	// A 3000-ASCII id shards into legal 128-hex components, but its whole
	// storage path runs past the operating system's total path length limit
	// (PATH_MAX on Linux). No per-file layout could make such a file exist, so
	// queries must treat it as absent — never as data corruption.
	id := longASCII(3000)
	if len(store.nodePath(id)) <= 4096 {
		t.Skip("test platform has no effective 4096-byte whole-path limit")
	}
	for _, p := range strings.Split(store.nodePath(id), string(filepath.Separator)) {
		if len(p) > 255 {
			t.Fatalf("individual components stay short, got %d-byte component", len(p))
		}
	}

	r, err := store.Health(id, receive, "1.0", 0)
	if err != nil {
		t.Fatalf("health must not report corruption for an unaddressable id: %v", err)
	}
	if r.Status != "notelemetry" {
		t.Errorf("status=%q, want notelemetry", r.Status)
	}
	hist, err := store.History(id)
	if err != nil || len(hist) != 0 {
		t.Errorf("history must be empty without error, got %+v err=%v", hist, err)
	}

	// Saving such an id still fails, and the failure is a persistence error —
	// not a corruption error that would quarantine the directory.
	_, _, err = store.Submit([]Heartbeat{
		hb(id, 1, receive.Add(-time.Second), "1.0", 1, 0),
	}, receive)
	if err == nil {
		t.Skip("platform accepted the overlong path; nothing more to assert")
	}
	if IsCorrupt(err) {
		t.Errorf("overlong-path save must not be classified as corruption: %v", err)
	}
}

func TestNodePathRoundTripAndCanonicalLocations(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	ids := []string{
		"", // empty must be rejected by ownership recovery even if hex-decodable
		"n",
		"val-eu-1",
		"节点/甲",
		"a b 😀",
		longASCII(124),
		longASCII(125), // 255-byte file name: last flat one
		longASCII(126), // 257-byte file name: first sharded one
		longASCII(127),
		longASCII(256), // 512 hex: two full chunks plus tail
		repeatRune('汉', 42),
		repeatRune('汉', 43),
		strings.Repeat("😀", 50),
	}
	for _, id := range ids {
		path := store.nodePath(id)
		everyPathComponentShort(t, path)
		owner, err := nodeIDFromPath(path)
		if id == "" {
			if err == nil {
				t.Errorf("empty-id path %q must not recover an owner", path)
			}
			continue
		}
		if err != nil {
			t.Fatalf("nodeIDFromPath(%q): %v", path, err)
		}
		if owner != id {
			t.Errorf("round trip mismatch: got %q want %q", owner, id)
		}
	}

	// A well-formed shard path that is not the owner's canonical location —
	// tail renamed to another hex string — must not recover any owner:
	// nodePathFor over the shard directories rebuilds a different path.
	long := longASCII(126)
	good := store.nodePath(long)
	bad := filepath.Join(filepath.Dir(good), "deadbeef.json")
	if _, err := nodeIDFromPath(bad); err == nil {
		t.Errorf("non-canonical shard file %q must not recover an owner", bad)
	}
	// A shard directory whose tail reconstructs an id short enough that its
	// canonical file belongs in the flat area is a non-canonical location and
	// must not be accepted as that node's store.
	chunk := strings.Repeat("a", nodeNameChunk) // one full shard component
	shortTail := "6161616161"                   // whole id stays short enough for a flat name
	nonCanonical := filepath.Join(store.nodesDir, chunk, shortTail+".json")
	if _, err := nodeIDFromPath(nonCanonical); err == nil {
		t.Errorf("shard-shaped path with a flat-length owner must be rejected: %s", nonCanonical)
	}
	// A plain non-hex file name is rejected outright.
	if _, err := nodeIDFromPath(filepath.Join(store.nodesDir, "not-hex.json")); err == nil {
		t.Error("non-hex flat file name must be rejected")
	}
}
