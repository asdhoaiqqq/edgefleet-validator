package edgefleet

// Regression coverage for the "duplicate-only node file is never re-saved"
// guarantee during post-reconnect backfill.
//
// A batch made entirely of records a node already saved must not rewrite that
// node's file: the lock is taken and the saved history is read and verified
// exactly as usual, but when every incoming record for a node is a duplicate
// the persist stage skips the node entirely. Re-writing identical bytes would
// produce the same history and the duplicate counts, so before/after content
// comparison alone cannot protect the rule — the avoidable failure this guards
// against is a redundant rewrite (temp file, fsync, rename) hitting an I/O
// error at exactly the wrong moment and turning a harmless retransmit into a
// failed submit. These tests therefore watch the persistence boundary itself:
// the package-level writeNodeFile seam records every attempted node-file write
// and can deterministically make one node's rewrite fail, with no disk filling,
// permission tricks or file-time waiting.
//
// The guarantee is narrower than "skip the node": identity/content checks are
// NOT skipped. Same node+seq with different content is still a conflict, and a
// corrupt or foreign-owned saved file is still a corruption error; both reject
// the whole batch with new=0 duplicate=0 before anything is persisted.

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// errInjectedRewrite stands in for "this node file cannot be re-saved right
// now" (disk full, I/O error, ...).
var errInjectedRewrite = errors.New("injected node-file rewrite failure")

// persistWriteSpy temporarily replaces writeNodeFile. It counts every
// persistence attempt and records the target paths; when failPath is non-empty,
// a write targeting exactly that path returns errInjectedRewrite instead of
// running the real atomic write, while every other node writes for real. The
// returned counter survives across the closure and is read after Submit.
func persistWriteSpy(t *testing.T, failPath string) (*int, *[]string) {
	t.Helper()
	calls := 0
	paths := []string{}
	orig := writeNodeFile
	writeNodeFile = func(path string, records []Heartbeat) error {
		calls++
		paths = append(paths, path)
		if failPath != "" && path == failPath {
			return errInjectedRewrite
		}
		return orig(path, records)
	}
	t.Cleanup(func() { writeNodeFile = orig })
	return &calls, &paths
}

// writesTo reports how many recorded writes targeted path.
func writesTo(paths *[]string, path string) int {
	n := 0
	for _, p := range *paths {
		if p == path {
			n++
		}
	}
	return n
}

// fileSnapshot reads a file's exact bytes, failing the test if it is gone.
func fileSnapshot(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("snapshot %s: %v", path, err)
	}
	return string(b)
}

// reopenStore closes nothing (stores hold no persistent handles) and returns a
// fresh instance on dir, so the exercise reads saved history from disk rather
// than from any in-memory state the setup used.
func reopenStore(t *testing.T, dir string) *Store {
	t.Helper()
	s, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// TestSubmitAllDuplicateBatchSkipsNodeFileRewrite is the primary regression:
// every input record already exists for the node, so the submit must succeed
// with new=0, duplicate counted per input occurrence, no extra history row,
// and — the point the history assertions alone cannot make — zero calls to the
// node-file writer. Byte-identical content after the submit is only a
// secondary witness; the write counter is what proves no rewrite happened.
func TestSubmitAllDuplicateBatchSkipsNodeFileRewrite(t *testing.T) {
	dir := t.TempDir()
	receive := testBase
	saved := []Heartbeat{
		hb("dup-node", 1, receive.Add(-2*time.Minute), "1.0", 100, 0),
		hb("dup-node", 2, receive.Add(-time.Minute), "1.0", 101, 1),
	}
	if _, _, err := reopenStore(t, dir).Submit(saved, receive); err != nil {
		t.Fatal(err)
	}

	// A fresh store: duplicate detection must work from durably saved history.
	store := reopenStore(t, dir)
	path := store.nodePath("dup-node")
	before := fileSnapshot(t, path)

	// seq 1 shows up twice, seq 2 once: three input occurrences, all saved.
	batch := []Heartbeat{saved[0], saved[0], saved[1]}
	writes, paths := persistWriteSpy(t, "")
	newC, dupC, err := store.Submit(batch, receive)
	if err != nil {
		t.Fatalf("an all-duplicate batch must succeed even with no rewrite: %v", err)
	}
	if newC != 0 || dupC != 3 {
		t.Errorf("new=%d duplicate=%d, want new=0 duplicate=3 (occurrences in input, not distinct records)", newC, dupC)
	}
	if *writes != 0 {
		t.Fatalf("an all-duplicate node must not be re-saved, but writeNodeFile was called %d time(s): %v", *writes, *paths)
	}

	// The saved file is byte-for-byte the same and gains no history row.
	if got := fileSnapshot(t, path); got != before {
		t.Errorf("duplicate-only node file was rewritten:\nbefore=%q\nafter =%q", before, got)
	}
	assertHistory(t, dir, "dup-node", saved)
}

// TestSubmitAllDuplicateBatchSucceedsWhenRewriteWouldFail reproduces the
// outage deterministically: the platform takes the directory lock and reads
// the saved history, but re-saving this node's file is temporarily failing.
// Because nothing new has to be persisted, the submit must still succeed —
// instead of dying from an avoidable write — and the file must be untouched.
func TestSubmitAllDuplicateBatchSucceedsWhenRewriteWouldFail(t *testing.T) {
	dir := t.TempDir()
	receive := testBase
	rec := hb("dup-node", 1, receive.Add(-time.Minute), "1.0", 100, 0)
	if _, _, err := reopenStore(t, dir).Submit([]Heartbeat{rec}, receive); err != nil {
		t.Fatal(err)
	}

	store := reopenStore(t, dir)
	loc := store.nodeLocationFor("dup-node")
	before := fileSnapshot(t, loc.path)
	writes, paths := persistWriteSpy(t, loc.path)

	newC, dupC, err := store.Submit([]Heartbeat{rec, rec}, receive)
	if err != nil {
		t.Fatalf("duplicates need no re-save, so a failing rewrite must not fail the submit: %v", err)
	}
	if newC != 0 || dupC != 2 {
		t.Errorf("new=%d duplicate=%d, want new=0 duplicate=2", newC, dupC)
	}
	if *writes != 0 {
		t.Fatalf("the duplicate-only node's writer must never be called, got %d call(s): %v", *writes, *paths)
	}
	if got := fileSnapshot(t, loc.path); got != before {
		t.Errorf("the node file changed despite an all-duplicate batch:\nbefore=%q\nafter =%q", before, got)
	}
	assertHistory(t, dir, "dup-node", []Heartbeat{rec})

	// Sanity that the fault really is wired (otherwise the test would pass
	// vacuously): a direct rewrite attempt for this node fails as advertised.
	// It serialises the same single record, so the file bytes stay identical.
	if err := writeNodeFile(loc.path, []Heartbeat{rec}); !errors.Is(err, errInjectedRewrite) {
		t.Fatalf("test setup: rewrite fault not in effect, got err=%v", err)
	}
	if got := fileSnapshot(t, loc.path); got != before {
		t.Errorf("file changed while proving the rewrite fault: %q", got)
	}
}

// TestSubmitDuplicateOnlyNodeNeverBlocksOtherNode pins the mixed reconnect
// batch: one node only re-sends records it already has (and its file currently
// cannot be re-saved), while another node in the same batch reports a genuine
// new record. The duplicate-only node must not be written at all, so it cannot
// drag the other node's legitimate submit down. Both storage layouts — the
// legacy nodes/hex name and the fixed-length slots/ hash name — must follow
// exactly this rule; a long id must neither be treated as a new node nor skip
// the ordinary per-node load.
func TestSubmitDuplicateOnlyNodeNeverBlocksOtherNode(t *testing.T) {
	receive := testBase
	longA := strings.Repeat("a", 126)  // slots/ layout
	longHan := strings.Repeat("汉", 42) // slots/ layout (multi-byte)
	longC := strings.Repeat("c", 126)

	cases := []struct {
		name          string
		dupNode       string
		dupUsesSlot   bool
		other         string
		otherUsesSlot bool
	}{
		{"legacy name duplicate node, legacy name new node", "dup-node", false, "other-node", false},
		{"slot duplicate node, legacy name new node", longA, true, "other-node", false},
		{"legacy name duplicate node, slot new node", "dup-node", false, longC, true},
		{"slot duplicate node (hanzi), slot new node", longHan, true, strings.Repeat("d", 126), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			dupRec := hb(tc.dupNode, 1, receive.Add(-time.Minute), "1.0", 100, 0)
			if _, _, err := reopenStore(t, dir).Submit([]Heartbeat{dupRec}, receive); err != nil {
				t.Fatal(err)
			}

			store := reopenStore(t, dir)
			dupLoc := store.nodeLocationFor(tc.dupNode)
			otherLoc := store.nodeLocationFor(tc.other)
			if tc.dupUsesSlot && filepath.Dir(dupLoc.path) != store.slotsDir {
				t.Fatalf("test setup: dup node %q must use a slot, got %s", tc.dupNode, dupLoc.path)
			}
			if !tc.dupUsesSlot && filepath.Dir(dupLoc.path) != store.nodesDir {
				t.Fatalf("test setup: dup node %q must use a legacy name, got %s", tc.dupNode, dupLoc.path)
			}
			if tc.otherUsesSlot && filepath.Dir(otherLoc.path) != store.slotsDir {
				t.Fatalf("test setup: other node %q must use a slot, got %s", tc.other, otherLoc.path)
			}
			dupBefore := fileSnapshot(t, dupLoc.path)

			// The duplicate node's rewrite is "temporarily failing"; the same
			// batch carries the other node's first, legitimate record. The
			// duplicate record is sent twice on purpose to pin occurrence
			// counting under the mixed condition.
			writes, paths := persistWriteSpy(t, dupLoc.path)
			otherRec := hb(tc.other, 1, receive.Add(-30*time.Second), "1.0", 200, 2)
			newC, dupC, err := store.Submit([]Heartbeat{dupRec, dupRec, otherRec}, receive)
			if err != nil {
				t.Fatalf("the duplicate-only node must not block the other node's legal submit: %v", err)
			}
			if newC != 1 || dupC != 2 {
				t.Errorf("new=%d duplicate=%d, want new=1 duplicate=2", newC, dupC)
			}
			if *writes != 1 || writesTo(paths, dupLoc.path) != 0 {
				t.Fatalf("exactly one write (the new node's file) must be attempted, got %d: %v", *writes, *paths)
			}
			if (*paths)[0] != otherLoc.path {
				t.Fatalf("the sole write must target the new node %s, got %s", otherLoc.path, (*paths)[0])
			}

			// The other node's new heartbeat is really on disk and queryable;
			// the duplicate node's whole history is exactly as it was.
			assertHistory(t, dir, tc.other, []Heartbeat{otherRec})
			assertHistory(t, dir, tc.dupNode, []Heartbeat{dupRec})
			if got := fileSnapshot(t, dupLoc.path); got != dupBefore {
				t.Errorf("duplicate-only node file was rewritten:\nbefore=%q\nafter =%q", dupBefore, got)
			}
		})
	}
}

// TestSubmitEquivalentDuplicateSpellingsSkipRewrite covers the duplicate
// identity/content rule end to end at the persistence boundary: same true
// instant in another timezone, and node/version written with equivalent JSON
// \uXXXX escapes, all denote records already saved. Each occurrence counts as
// a duplicate, no write is attempted, and the original stored text and
// collection instant are never rewritten in the alternative spelling.
func TestSubmitEquivalentDuplicateSpellingsSkipRewrite(t *testing.T) {
	dir := t.TempDir()
	receive := testBase

	// The saved record carries a sub-second instant so timezone equivalence is
	// checked at full nanosecond precision, not just to the printed second.
	canonical := `[{"node":"peer-a","seq":7,"collected_at":"2026-10-01T11:59:00.25Z","version":"1.0","height":12345,"missed":0}]`
	saved, err := ParseHeartbeats([]byte(canonical), receive)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := reopenStore(t, dir).Submit(saved, receive); err != nil {
		t.Fatal(err)
	}
	original := saved[0]

	store := reopenStore(t, dir)
	path := store.nodePath("peer-a")
	before := fileSnapshot(t, path)

	bs := "\\"
	equivalents := `[
	  {"node":"peer-a","seq":7,"collected_at":"2026-10-01T19:59:00.25+08:00","version":"1.0","height":12345,"missed":0},
	  {"node":"` + bs + `u0070eer-` + bs + `u0061","seq":7,"collected_at":"2026-10-01T11:59:00.25Z","version":"1.0","height":12345,"missed":0},
	  {"node":"peer-a","seq":7,"collected_at":"2026-10-01T11:59:00.25Z","version":"` + bs + `u0031.` + bs + `u0030","height":12345,"missed":0},
	  {"node":"` + bs + `u0070eer-` + bs + `u0061","seq":7,"collected_at":"2026-10-01T19:59:00.25+08:00","version":"` + bs + `u0031.` + bs + `u0030","height":12345,"missed":0}
	]`
	records, err := ParseHeartbeats([]byte(equivalents), receive)
	if err != nil {
		t.Fatal(err)
	}
	writes, paths := persistWriteSpy(t, path)
	newC, dupC, err := store.Submit(records, receive)
	if err != nil {
		t.Fatalf("equivalent spellings must be accepted duplicates: %v", err)
	}
	if newC != 0 || dupC != 4 {
		t.Errorf("new=%d duplicate=%d, want new=0 duplicate=4", newC, dupC)
	}
	if *writes != 0 {
		t.Fatalf("equivalent duplicates must not rewrite the file, got write(s): %v", *paths)
	}

	// History keeps one row, and the stored original text/instant survives:
	// no offset spelling, escape sequence or rounded fraction is written back.
	assertHistory(t, dir, "peer-a", []Heartbeat{original})
	hist, err := reopenStore(t, dir).History("peer-a")
	if err != nil {
		t.Fatal(err)
	}
	if len(hist) != 1 {
		t.Fatalf("history must keep one row, got %d: %+v", len(hist), hist)
	}
	got := hist[0]
	if got.NodeID != "peer-a" {
		t.Errorf("stored node text was rewritten to %q", got.NodeID)
	}
	if got.Version != "1.0" {
		t.Errorf("stored version text was rewritten to %q", got.Version)
	}
	if !got.CollectedAt.Equal(original.CollectedAt) || got.CollectedAt.Nanosecond() != 250_000_000 {
		t.Errorf("stored collection instant was rewritten: %v (nanos=%d), want %v",
			got.CollectedAt, got.CollectedAt.Nanosecond(), original.CollectedAt)
	}
	if got := fileSnapshot(t, path); got != before {
		t.Errorf("node file bytes changed despite an all-equivalent-duplicate batch:\nbefore=%q\nafter =%q", before, got)
	}
}

// TestSubmitContentConflictRejectedBeforeAnyWrite proves "not re-saved" never
// means "not checked": a same-node/same-seq record with different content is a
// conflict even though the node would otherwise be a no-new-record node. The
// batch is rejected with new=0 duplicate=0 before a single file write, the
// original history is not overwritten, and a legal new record for another
// node does not land either.
func TestSubmitContentConflictRejectedBeforeAnyWrite(t *testing.T) {
	receive := testBase
	saved := `[{"node":"dup-node","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.26.0","height":12345,"missed":0}]`

	cases := []struct {
		name     string
		incoming string
	}{
		{"version differs", `{"node":"dup-node","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.27.0","height":12345,"missed":0}`},
		{"height differs", `{"node":"dup-node","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.26.0","height":12346,"missed":0}`},
		{"missed differs", `{"node":"dup-node","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.26.0","height":12345,"missed":1}`},
		{"collection instant differs", `{"node":"dup-node","seq":1,"collected_at":"2026-10-01T11:58:00Z","version":"1.26.0","height":12345,"missed":0}`},
		{"same wall-clock text, another offset is another instant", `{"node":"dup-node","seq":1,"collected_at":"2026-10-01T11:59:00+08:00","version":"1.26.0","height":12345,"missed":0}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			records, err := ParseHeartbeats([]byte(saved), receive)
			if err != nil {
				t.Fatal(err)
			}
			original := records[0]
			if _, _, err := reopenStore(t, dir).Submit(records, receive); err != nil {
				t.Fatal(err)
			}

			store := reopenStore(t, dir)
			path := store.nodePath("dup-node")
			before := fileSnapshot(t, path)
			conflicting, err := ParseHeartbeats([]byte("["+tc.incoming+"]"), receive)
			if err != nil {
				t.Fatal(err)
			}
			// The same batch carries another node's legal first record: it
			// must not survive the rejection.
			legalNew := hb("fresh-node", 1, receive.Add(-time.Second), "1.0", 1, 0)
			batch := []Heartbeat{conflicting[0], legalNew}

			// Even if the duplicate node's rewrite "worked", the conflict must
			// be found in the merge phase before the persist phase.
			writes, paths := persistWriteSpy(t, path)
			newC, dupC, err := store.Submit(batch, receive)
			if err == nil || !strings.Contains(err.Error(), "conflicting record") {
				t.Fatalf("same identity with different content must conflict, got %v", err)
			}
			if newC != 0 || dupC != 0 {
				t.Errorf("rejected conflict batch must report new=0 duplicate=0, got new=%d duplicate=%d", newC, dupC)
			}
			if *writes != 0 {
				t.Fatalf("nothing may be persisted on conflict, got write attempt(s): %v", *paths)
			}

			// The original record is intact and the other node got no file.
			assertHistory(t, dir, "dup-node", []Heartbeat{original})
			if got := fileSnapshot(t, path); got != before {
				t.Errorf("history was overwritten despite the conflict:\nbefore=%q\nafter =%q", before, got)
			}
			if _, err := os.Stat(store.nodePath("fresh-node")); !os.IsNotExist(err) {
				t.Errorf("the batch's legal new record must not be saved, stat err=%v", err)
			}
		})
	}

	// An in-batch conflict (two brand-new records, same node+seq, different
	// content) is rejected identically: zero counts, no writes, and the legal
	// record of yet another node in the batch is not saved either.
	t.Run("conflict within the batch", func(t *testing.T) {
		dir := t.TempDir()
		store := reopenStore(t, dir)
		writes, paths := persistWriteSpy(t, "")
		batch := []Heartbeat{
			hb("new-node", 1, receive.Add(-2*time.Second), "1.0", 100, 0),
			hb("new-node", 1, receive.Add(-2*time.Second), "2.0", 100, 0),
			hb("other-new", 1, receive.Add(-time.Second), "1.0", 1, 0),
		}
		newC, dupC, err := store.Submit(batch, receive)
		if err == nil || !strings.Contains(err.Error(), "conflicting record") {
			t.Fatalf("in-batch conflict must be rejected, got %v", err)
		}
		if newC != 0 || dupC != 0 {
			t.Errorf("rejected batch must report new=0 duplicate=0, got new=%d duplicate=%d", newC, dupC)
		}
		if *writes != 0 {
			t.Fatalf("nothing may be persisted on an in-batch conflict, got %v", *paths)
		}
		for _, node := range []string{"new-node", "other-new"} {
			if _, err := os.Stat(store.nodePath(node)); !os.IsNotExist(err) {
				t.Errorf("node %q must have no file after the rejected batch, stat err=%v", node, err)
			}
		}
	})
}

// TestSubmitSeemingDuplicateOnCorruptFileIsRejected exercises the other
// "looks like a duplicate" condition: the node resends records it believes
// were already saved, but its saved file is corrupt. Loading and verifying
// history still happens for a no-new node, so the batch — including a legal
// append to a healthy node and a first-time node — is rejected wholesale with
// new=0 duplicate=0 and no write; the corrupt bytes and healthy history are
// left exactly as found.
func TestSubmitSeemingDuplicateOnCorruptFileIsRejected(t *testing.T) {
	dir := t.TempDir()
	receive := testBase
	good1 := hb("good-node", 1, receive.Add(-time.Minute), "1.0", 100, 0)
	if _, _, err := reopenStore(t, dir).Submit([]Heartbeat{good1}, receive); err != nil {
		t.Fatal(err)
	}

	store := reopenStore(t, dir)
	badPath := store.nodePath("bad-node")
	if err := os.WriteFile(badPath, []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	goodPath := store.nodePath("good-node")
	badBefore := fileSnapshot(t, badPath)
	goodBefore := fileSnapshot(t, goodPath)

	// "bad-node" resends its presumed seq 1; the same batch legally extends
	// the healthy node and creates a first-time node.
	seemingDup := hb("bad-node", 1, receive.Add(-time.Minute), "1.0", 100, 0)
	good2 := hb("good-node", 2, receive.Add(-time.Second), "1.0", 101, 0)
	fresh := hb("fresh-node", 1, receive.Add(-time.Second), "1.0", 1, 0)

	writes, paths := persistWriteSpy(t, "")
	newC, dupC, err := store.Submit([]Heartbeat{seemingDup, good2, fresh}, receive)
	if err == nil || !IsCorrupt(err) {
		t.Fatalf("a corrupt saved file must reject the apparent duplicate batch, got %v", err)
	}
	if newC != 0 || dupC != 0 {
		t.Errorf("corruption rejection must report new=0 duplicate=0, got new=%d duplicate=%d", newC, dupC)
	}
	if *writes != 0 {
		t.Fatalf("corruption is found while loading, before any persist; got write attempt(s): %v", *paths)
	}
	if got := fileSnapshot(t, badPath); got != badBefore {
		t.Errorf("corrupt file must not be repaired, replaced or deleted:\nbefore=%q\nafter =%q", badBefore, got)
	}
	if got := fileSnapshot(t, goodPath); got != goodBefore {
		t.Errorf("the healthy node was rewritten during the rejected batch:\nbefore=%q\nafter =%q", goodBefore, got)
	}
	assertHistory(t, dir, "good-node", []Heartbeat{good1})
	if _, err := os.Stat(store.nodePath("fresh-node")); !os.IsNotExist(err) {
		t.Errorf("first-time node file must not be created, stat err=%v", err)
	}

	// An unrelated, healthy submit afterwards still works.
	if _, _, err := reopenStore(t, dir).Submit([]Heartbeat{good2}, receive); err != nil {
		t.Fatalf("healthy nodes must keep accepting submits: %v", err)
	}
	assertHistory(t, dir, "good-node", []Heartbeat{good1, good2})
}

// TestSubmitSeemingDuplicateOnMisownedFileIsRejected plants another node's
// complete, checksum-valid file at the resending node's location and resends
// the same seq — the strongest "looks like a duplicate" input. The ownership
// check must still fire for BOTH storage layouts (legacy hex name and hash
// slot, where the long id must never be mistaken for a new node) and reject
// the whole batch with new=0 duplicate=0, no writes, the planted file
// untouched and the batch's legal new record unsaved.
func TestSubmitSeemingDuplicateOnMisownedFileIsRejected(t *testing.T) {
	receive := testBase

	cases := []struct {
		name   string
		src    string
		victim string
	}{
		{"foreign file on a legacy hex name", "src-node", "victim-node"},
		{"foreign file in a long-id hash slot",
			strings.Repeat("alpha-node-", 13) + "S",
			strings.Repeat("beta-node--", 13) + "V"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			store := reopenStore(t, dir)
			srcRec := hb(tc.src, 1, receive.Add(-time.Minute), "1.0", 100, 0)
			if _, _, err := store.Submit([]Heartbeat{srcRec}, receive); err != nil {
				t.Fatal(err)
			}

			// Copy the source node's intact file onto the victim's location.
			copyFileBytes(t, store, tc.victim, tc.src)
			victimPath := store.nodePath(tc.victim)
			planted := fileSnapshot(t, victimPath)

			// The victim resends seq 1 (identity victim+1 — exactly what a
			// reconnect retransmit looks like), alongside a first-time node.
			seemingDup := hb(tc.victim, 1, receive.Add(-time.Minute), "1.0", 100, 0)
			fresh := hb("fresh-node", 1, receive.Add(-time.Second), "1.0", 1, 0)

			writes, paths := persistWriteSpy(t, "")
			newC, dupC, err := store.Submit([]Heartbeat{seemingDup, fresh}, receive)
			if err == nil || !IsCorrupt(err) {
				t.Fatalf("a foreign-owned file must reject the apparent duplicate, got %v", err)
			}
			msg := err.Error()
			for _, want := range []string{"record 1", fmtNode(tc.victim), fmtNode(tc.src)} {
				if !strings.Contains(msg, want) {
					t.Errorf("corruption error must name %q; got: %v", want, err)
				}
			}
			if newC != 0 || dupC != 0 {
				t.Errorf("ownership rejection must report new=0 duplicate=0, got new=%d duplicate=%d", newC, dupC)
			}
			if *writes != 0 {
				t.Fatalf("ownership is verified while loading, before any persist; got write attempt(s): %v", *paths)
			}
			if got := fileSnapshot(t, victimPath); got != planted {
				t.Errorf("the misowned file must stay exactly as planted:\nbefore=%q\nafter =%q", planted, got)
			}
			if _, err := os.Stat(store.nodePath("fresh-node")); !os.IsNotExist(err) {
				t.Errorf("the batch's legal new record must not be saved, stat err=%v", err)
			}
			// The real owner is unaffected.
			assertHistory(t, dir, tc.src, []Heartbeat{srcRec})

			// The long id must not have silently become "no telemetry": a
			// history read keeps refusing it as corruption, too.
			if _, err := reopenStore(t, dir).History(tc.victim); err == nil || !IsCorrupt(err) {
				t.Fatalf("history on the misowned location must keep refusing, got %v", err)
			}
		})
	}
}

// fmtNode renders a node id the way store error messages quote it.
func fmtNode(node string) string {
	return `"` + node + `"`
}
