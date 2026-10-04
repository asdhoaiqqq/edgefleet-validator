package edgefleet

import (
	"os"
	"strings"
	"testing"
	"time"
)

// These tests protect node identity at the two submit trust boundaries:
//
//   - JSON input (ParseHeartbeats): the node value must be losslessly
//     representable Unicode text. encoding/json itself silently replaces
//     invalid UTF-8 bytes and unpaired \uD800-\uDFFF surrogate escapes with
//     U+FFFD, which would make different inputs land on one node; both must
//     be rejected before that rewrite can happen.
//   - Go callers (Store.Submit / ValidateHeartbeat): a NodeID holding invalid
//     UTF-8 bytes can never be saved losslessly (json.Marshal rewrites it),
//     so the submit fails with new=dup=0 instead of succeeding and later
//     disagreeing about ownership.
//
// A deliberately entered U+FFFD, astral characters written as a correct
// surrogate pair, and ordinary text with Chinese characters, spaces and
// slashes are all legal and keep working unchanged.

// recordJSON wraps a raw JSON node value (including its quotes) in one valid
// heartbeat object.
func recordJSON(nodeJSON string) string {
	return `{"node":` + nodeJSON + `,"seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":1,"missed":0}`
}

func TestParseHeartbeatsRejectsLossyNodeIDs(t *testing.T) {
	cases := []struct {
		name     string
		nodeJSON string
	}{
		// Bare invalid bytes embedded in the JSON string; Go interpreted
		// strings produce the real bytes rather than the text "\xff".
		{"invalid utf8 byte", `"n` + "\xff" + `"`},
		{"truncated multibyte sequence", `"n` + "\xe4\xb8" + `"`},
		{"lone continuation byte", `"n` + "\x80" + `"`},
		{"invalid byte between ascii", `"ab` + "\xff" + `cd"`},
		// Unpaired surrogates expressed via JSON Unicode escapes (literal
		// escape text in the input).
		{"unpaired high surrogate", `"\ud800"`},
		{"unpaired low surrogate", `"\udc00"`},
		{"high surrogate at end of text", `"n\ud83d"`},
		{"low surrogate at end of text", `"n\ude00"`},
		{"two high surrogates", `"\ud83d\ud83d"`},
		{"high surrogate followed by ascii", `"\ud83dx"`},
		{"ascii followed by low surrogate", `"x\ude00"`},
		{"paired then trailing high", `"\ud83d\ude00\ud83d"`},
		// A pair only counts when the two escapes are adjacent; anything in
		// between leaves both surrogates unpaired.
		{"high and low separated by escape", `"\ud83d\n\ude00"`},
		{"high and low separated by ascii", `"\ud83dA\ude00"`},
		{"high then another high-low pair", `"\ud83d\ud83d\ude00"`},
		{"low then a complete pair", `"\ude00\ud83d\ude00"`},
		{"pair then trailing low", `"\ud83d\ude00\ude00"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			input := "[" + recordJSON(tc.nodeJSON) + "]"
			recs, err := ParseHeartbeats([]byte(input), testBase)
			if err == nil {
				t.Fatalf("lossy node id must be rejected, parsed as %+v", recs)
			}
			msg := err.Error()
			if !strings.Contains(msg, "record 1") {
				t.Errorf("error must locate record 1, got: %v", err)
			}
			if !strings.Contains(msg, "node") {
				t.Errorf("error must name the node field, got: %v", err)
			}
			if recs != nil {
				t.Errorf("failed parse must return no records, got %+v", recs)
			}
		})
	}
}

func TestParseHeartbeatsRejectsLossyNodeIDRecordPosition(t *testing.T) {
	good := recordJSON(`"good"`)
	bad := recordJSON(`"bad` + "\xff" + `"`)
	for _, tc := range []struct {
		name  string
		input string
		pos   string
	}{
		{"bad first", "[" + bad + "," + good + "]", "record 1"},
		{"bad second", "[" + good + "," + bad + "]", "record 2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseHeartbeats([]byte(tc.input), testBase)
			if err == nil {
				t.Fatalf("expected rejection")
			}
			if !strings.Contains(err.Error(), tc.pos) {
				t.Errorf("error must identify %s, got: %v", tc.pos, err)
			}
		})
	}
}

func TestParseHeartbeatsAcceptsLosslessNodeIDs(t *testing.T) {
	cases := []struct {
		name     string
		nodeJSON string
		want     string
	}{
		{"chinese", `"节点/甲"`, "节点/甲"},
		{"spaces and slash", `"edge node 1/a"`, "edge node 1/a"},
		{"astral literal", `"😀"`, "😀"},
		// Astral character written as one correctly paired surrogate.
		{"astral via surrogate pair", `"\ud83d\ude00"`, "😀"},
		{"pair embedded in text", `"node-😀-end"`, "node-😀-end"},
		{"two adjacent pairs", `"\ud83d\ude00\ud83d\ude01"`, "😀😁"},
		{"uppercase hex pair", `"\uD83D\uDE00"`, "😀"},
		{"pair then simple escape and text", `"\ud83d\ude00\nx"`, "😀\nx"},
		{"simple escape and text then pair", `"x\n\ud83d\ude00"`, "x\n😀"},
		// U+FFFD that the user actually meant, written either way.
		{"explicit replacement literal", `"n�"`, "n�"},
		{"explicit replacement escape", `"n\ufffd"`, "n�"},
		{"only replacement char", `"�"`, "�"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			input := "[" + recordJSON(tc.nodeJSON) + "]"
			recs, err := ParseHeartbeats([]byte(input), testBase)
			if err != nil {
				t.Fatalf("legal node id must be accepted: %v", err)
			}
			if len(recs) != 1 || recs[0].NodeID != tc.want {
				t.Fatalf("decoded node = %q, want %q (all records: %+v)", recs[0].NodeID, tc.want, recs)
			}
		})
	}
}

func TestLiteralAndEscapedAstralNodeAreSameNode(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase
	// Same id text, first literal UTF-8 and then as a legal surrogate-pair
	// escape, with identical content: the second is a duplicate, not a second
	// node and not a conflict.
	input := "[" +
		recordJSON(`"😀"`) + "," +
		recordJSON(`"\ud83d\ude00"`) + "]"
	recs, err := ParseHeartbeats([]byte(input), receive)
	if err != nil {
		t.Fatalf("both spellings must parse: %v", err)
	}
	newC, dupC, err := store.Submit(recs, receive)
	if err != nil {
		t.Fatalf("submit failed: %v", err)
	}
	if newC != 1 || dupC != 1 {
		t.Errorf("same id text must be one node with one duplicate: new=%d dup=%d", newC, dupC)
	}
	hist, err := store.History("😀")
	if err != nil {
		t.Fatal(err)
	}
	if len(hist) != 1 || hist[0].NodeID != "😀" {
		t.Errorf("history for emoji node wrong: %+v", hist)
	}
	r, err := store.Health("😀", receive, "1.0", 0)
	if err != nil {
		t.Fatal(err)
	}
	if r.NodeID != "😀" {
		t.Errorf("health misidentified node: %q", r.NodeID)
	}
}

func TestDistinctLegalNodeIDsStayDistinct(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase
	// U+FFFD, the emoji and unrelated ids are different nodes; none collapses
	// onto another and no case/space normalisation happens.
	ids := []string{"�", "😀", "Node A", "node a"}
	var batch []Heartbeat
	for i, id := range ids {
		batch = append(batch, hb(id, 1, receive.Add(-time.Second), "1.0", int64(i), 0))
	}
	newC, _, err := store.Submit(batch, receive)
	if err != nil {
		t.Fatalf("submit failed: %v", err)
	}
	if newC != len(ids) {
		t.Fatalf("new=%d, want %d distinct nodes", newC, len(ids))
	}
	for _, id := range ids {
		hist, err := store.History(id)
		if err != nil || len(hist) != 1 || hist[0].NodeID != id {
			t.Errorf("node %q not stored under its own id: %+v err=%v", id, hist, err)
		}
	}
}

func TestSubmitRejectsDirectRecordWithInvalidNodeID(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase

	// Pre-existing healthy node that the rejected batch must not touch.
	if _, _, err := store.Submit([]Heartbeat{
		hb("existing", 1, receive.Add(-time.Minute), "1.0", 100, 0),
	}, receive); err != nil {
		t.Fatal(err)
	}

	invalidID := "n\xff" // invalid UTF-8 bytes a Go caller could pass directly
	if err := ValidateHeartbeat(hb(invalidID, 1, receive.Add(-time.Second), "1.0", 1, 0), receive); err == nil {
		t.Fatal("ValidateHeartbeat must reject an invalid UTF-8 node id")
	}

	batch := []Heartbeat{
		hb("fresh", 1, receive.Add(-time.Second), "1.0", 1, 0), // valid, must still not be saved
		hb(invalidID, 1, receive.Add(-time.Second), "1.0", 2, 0),
	}
	newC, dupC, err := store.Submit(batch, receive)
	if err == nil {
		t.Fatal("submit must fail")
	}
	if newC != 0 || dupC != 0 {
		t.Errorf("rejected batch must report new=0 duplicate=0, got new=%d dup=%d", newC, dupC)
	}
	if !strings.Contains(err.Error(), "node") {
		t.Errorf("error must explain the node id is invalid, got: %v", err)
	}

	// The valid record in the same batch was not saved.
	r, err := store.Health("fresh", receive, "1.0", 0)
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != "notelemetry" {
		t.Errorf("fresh node must have no telemetry, got %+v", r)
	}
	if _, err := os.Stat(store.nodePath("fresh")); !os.IsNotExist(err) {
		t.Errorf("fresh node file must not be created, stat err=%v", err)
	}
	// Pre-existing history and health are unchanged.
	hist, err := store.History("existing")
	if err != nil || len(hist) != 1 || hist[0].Seq != 1 {
		t.Errorf("existing history changed: %+v err=%v", hist, err)
	}
	r, err = store.Health("existing", receive, "1.0", 0)
	if err != nil || r.Seq != 1 || r.Height != 100 {
		t.Errorf("existing health changed: %+v err=%v", r, err)
	}
}

func TestJSONBatchWithOneLossyNodeIsAtomic(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase
	if _, _, err := store.Submit([]Heartbeat{
		hb("existing", 1, receive.Add(-time.Minute), "1.0", 100, 0),
	}, receive); err != nil {
		t.Fatal(err)
	}

	input := "[" +
		recordJSON(`"fresh"`) + "," +
		recordJSON(`"bad`+"\xff"+`"`) + "]"
	recs, err := ParseHeartbeats([]byte(input), receive)
	if err == nil {
		t.Fatalf("batch must be rejected, got %+v", recs)
	}
	if recs != nil {
		t.Errorf("no partially parsed records must be returned, got %+v", recs)
	}
	if _, err := os.Stat(store.nodePath("fresh")); !os.IsNotExist(err) {
		t.Errorf("fresh node file must not be created, stat err=%v", err)
	}
	r, err := store.Health("fresh", receive, "1.0", 0)
	if err != nil || r.Status != "notelemetry" {
		t.Errorf("fresh health must stay notelemetry, got %+v err=%v", r, err)
	}
	hist, err := store.History("existing")
	if err != nil || len(hist) != 1 {
		t.Errorf("existing history changed: %+v err=%v", hist, err)
	}
}

func TestStoredFileWithLossyNodeIDIsCorrupt(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	path := store.nodePath("n1")
	cases := []struct {
		name        string
		recordsJSON string
	}{
		// Invalid raw bytes in the stored node value are refused before the
		// checksum is consulted; they must not be read as a U+FFFD node.
		{"invalid utf8 bytes", `[{"node":"n` + "\xff" + `","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":100,"missed":0}]`},
		// A lone surrogate escape in a stored file is just as lossy on read.
		{"unpaired surrogate", `[{"node":"n\ud800","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":100,"missed":0}]`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			content := `{"format":"edgefleet-heartbeats-v1","checksum":"x","records":` + tc.recordsJSON + `}`
			if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
				t.Fatal(err)
			}
			_, err := loadNodeFile(path)
			if err == nil || !IsCorrupt(err) {
				t.Fatalf("lossy stored node id must be corruption, got %v", err)
			}
			msg := err.Error()
			if !strings.Contains(msg, "record 1") || !strings.Contains(msg, "node") {
				t.Errorf("corruption error must name record 1 and node, got: %v", err)
			}
			if _, err := store.Health("n1", testBase, "1.0", 0); err == nil || !IsCorrupt(err) {
				t.Errorf("health must refuse, got %v", err)
			}
			if _, err := store.History("n1"); err == nil || !IsCorrupt(err) {
				t.Errorf("history must refuse, got %v", err)
			}
		})
	}
}

func TestNormalNodeFilesWithUnicodeIDsStayReadable(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase
	ids := []string{"节点/甲", "edge node 1", "😀", "n�"}
	var batch []Heartbeat
	for i, id := range ids {
		batch = append(batch, hb(id, 1, receive.Add(-time.Second), "1.0", int64(i), 0))
	}
	if _, _, err := store.Submit(batch, receive); err != nil {
		t.Fatalf("legal unicode ids must submit: %v", err)
	}
	// Reopen from disk: existing normal node files keep reading with the id
	// intact, no migration or rejection.
	reopened, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	for i, id := range ids {
		hist, err := reopened.History(id)
		if err != nil || len(hist) != 1 || hist[0].NodeID != id {
			t.Errorf("id %q unreadable after restart: %+v err=%v", id, hist, err)
		}
		r, err := reopened.Health(id, receive, "1.0", 0)
		if err != nil || r.NodeID != id || r.Height != int64(i) {
			t.Errorf("id %q health wrong: %+v err=%v", id, r, err)
		}
	}
}
