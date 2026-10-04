package edgefleet

import (
	"strings"
	"testing"
	"time"
)

// These tests protect node identity at the two submit trust boundaries: the
// JSON command-line input and direct Store.Submit calls. A node id must be
// losslessly representable Unicode text. encoding/json silently rewrites
// invalid UTF-8 bytes and unpaired \u surrogate escapes to U+FFFD, which
// would merge distinct node ids into one and later break the ownership
// check between a node's file and its records — so such input is rejected
// outright, never stored as a replacement-char id. A literal "�" typed by
// the user is a legal character and stays accepted.

// recordJSON builds one heartbeat object around the given raw node token.
func recordJSON(nodeToken string) string {
	return `[{"node":` + nodeToken + `,"seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":1,"missed":0}]`
}

func TestParseHeartbeatsInvalidUTF8NodeRejected(t *testing.T) {
	// 0xFF is not valid UTF-8 anywhere; a lenient decode would turn it into
	// U+FFFD and silently rename the node.
	input := []byte(recordJSON(`"val-` + "\xff" + `1"`))
	_, err := ParseHeartbeats(input, testBase)
	if err == nil {
		t.Fatalf("invalid UTF-8 in node must be rejected")
	}
	if !strings.Contains(err.Error(), "record 1") || !strings.Contains(err.Error(), "node") {
		t.Errorf("error must name the record position and the node field, got: %v", err)
	}

	// The same failure at a later batch position reports that position.
	two := `[
		{"node":"good","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":1,"missed":0},
		{"node":"bad-` + "\xff" + `","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":1,"missed":0}
	]`
	_, err = ParseHeartbeats([]byte(two), testBase)
	if err == nil || !strings.Contains(err.Error(), "record 2") || !strings.Contains(err.Error(), "node") {
		t.Errorf("error must name record 2 and the node field, got: %v", err)
	}
}

func TestParseHeartbeatsUnpairedSurrogateRejected(t *testing.T) {
	cases := []struct {
		name      string
		nodeToken string
	}{
		{"lone high surrogate", `"val\uD83D"`},
		{"lone low surrogate", `"val\uDE00"`},
		{"high surrogate at end", `"\uD83D"`},
		{"high surrogate then plain char", `"\uD83Dx"`},
		{"high surrogate then non-surrogate escape", `"\uD83DA"`},
		{"high surrogate then another high", `"\uD83D\uD83D"`},
		{"high surrogate then escaped quote", `"\uD83D\""`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseHeartbeats([]byte(recordJSON(tc.nodeToken)), testBase)
			if err == nil {
				t.Fatalf("unpaired surrogate in node must be rejected")
			}
			if !strings.Contains(err.Error(), "record 1") || !strings.Contains(err.Error(), "node") {
				t.Errorf("error must name the record position and the node field, got: %v", err)
			}
		})
	}
}

func TestParseHeartbeatsValidUnicodeNodeAccepted(t *testing.T) {
	cases := []struct {
		name      string
		nodeToken string
		want      string
	}{
		{"chinese", `"节点甲"`, "节点甲"},
		{"spaces and slashes", `"node / with spaces"`, "node / with spaces"},
		{"literal replacement char", `"val-�"`, "val-�"},
		{"escaped replacement char", `"val-\uFFFD"`, "val-�"},
		{"surrogate pair emoji", `"val-\uD83D\uDE00"`, "val-😀"},
		{"literal emoji", `"val-😀"`, "val-😀"},
		{"escaped ascii", `"val-\u0031"`, "val-1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			records, err := ParseHeartbeats([]byte(recordJSON(tc.nodeToken)), testBase)
			if err != nil {
				t.Fatalf("valid node id rejected: %v", err)
			}
			if records[0].NodeID != tc.want {
				t.Errorf("node id = %q, want %q", records[0].NodeID, tc.want)
			}
		})
	}
}

func TestNodeIDEscapedAndLiteralAreSameNode(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase

	// Submit the literal form of a node id that needs a surrogate pair.
	literal := recordJSON(`"val-😀"`)
	records, err := ParseHeartbeats([]byte(literal), receive)
	if err != nil {
		t.Fatalf("literal form rejected: %v", err)
	}
	newC, dupC, err := store.Submit(records, receive)
	if err != nil || newC != 1 || dupC != 0 {
		t.Fatalf("literal submit: new=%d dup=%d err=%v, want 1/0/nil", newC, dupC, err)
	}

	// The same text written as a \u surrogate pair is the same node: the
	// record is a duplicate, not a new node.
	escaped := recordJSON(`"val-\uD83D\uDE00"`)
	records, err = ParseHeartbeats([]byte(escaped), receive)
	if err != nil {
		t.Fatalf("escaped form rejected: %v", err)
	}
	newC, dupC, err = store.Submit(records, receive)
	if err != nil || newC != 0 || dupC != 1 {
		t.Errorf("escaped submit: new=%d dup=%d err=%v, want 0/1/nil", newC, dupC, err)
	}

	hist, err := store.History("val-😀")
	if err != nil {
		t.Fatal(err)
	}
	if len(hist) != 1 {
		t.Errorf("history = %d records, want 1 (no split node)", len(hist))
	}
}

func TestSubmitDirectInvalidUTF8NodeRejected(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase

	// Pre-existing valid data must not be disturbed.
	good := hb("good", 1, receive.Add(-time.Minute), "1.0", 100, 0)
	if _, _, err := store.Submit([]Heartbeat{good}, receive); err != nil {
		t.Fatal(err)
	}

	// A Go caller submitting a record whose node id holds invalid UTF-8 gets
	// an error saying the node id is invalid; counts are zero and nothing
	// is added — not even the valid record in the same batch.
	bad := hb("bad-\xff", 1, receive.Add(-time.Minute), "1.0", 100, 0)
	valid := hb("other", 1, receive.Add(-time.Minute), "1.0", 100, 0)
	newC, dupC, err := store.Submit([]Heartbeat{bad, valid}, receive)
	if err == nil {
		t.Fatalf("invalid UTF-8 node id must be rejected")
	}
	if !strings.Contains(err.Error(), "node") {
		t.Errorf("error must say the node id is invalid, got: %v", err)
	}
	if newC != 0 || dupC != 0 {
		t.Errorf("counts = %d/%d, want 0/0", newC, dupC)
	}

	for _, node := range []string{"bad-\xff", "other"} {
		hist, err := store.History(node)
		if err != nil {
			t.Fatal(err)
		}
		if len(hist) != 0 {
			t.Errorf("node %q got records despite rejected batch: %+v", node, hist)
		}
	}

	// The pre-existing node is untouched and still healthy under the same
	// query.
	hist, err := store.History("good")
	if err != nil || len(hist) != 1 {
		t.Fatalf("good node history changed: %v %+v", err, hist)
	}
	r, err := store.Health("good", receive, "1.0", 0)
	if err != nil || r.Status != "online" {
		t.Errorf("good node health changed: %v %+v", err, r)
	}
}

func TestSubmitDirectInvalidNodeNotTreatedAsDuplicateOrConflict(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase

	// A node id that differs from a saved one only by an invalid byte must
	// not merge into the saved node as a duplicate or a conflict.
	if _, _, err := store.Submit([]Heartbeat{hb("node-", 1, receive.Add(-time.Minute), "1.0", 100, 0)}, receive); err != nil {
		t.Fatal(err)
	}
	_, _, err = store.Submit([]Heartbeat{hb("node-\xff", 1, receive.Add(-time.Minute), "1.0", 100, 0)}, receive)
	if err == nil {
		t.Fatalf("invalid UTF-8 node id must be rejected, not merged with %q", "node-")
	}
	hist, err := store.History("node-")
	if err != nil {
		t.Fatal(err)
	}
	if len(hist) != 1 {
		t.Errorf("existing node history changed: %+v", hist)
	}
}

func TestValidateHeartbeatRejectsInvalidUTF8Node(t *testing.T) {
	receive := testBase
	bad := hb("n\xff", 1, receive.Add(-time.Minute), "1.0", 10, 0)
	err := ValidateHeartbeat(bad, receive)
	if err == nil || !strings.Contains(err.Error(), "node") {
		t.Errorf("ValidateHeartbeat must reject invalid UTF-8 node id, got: %v", err)
	}
	// A literal replacement character is valid text and stays accepted.
	ok := hb("n-�", 1, receive.Add(-time.Minute), "1.0", 10, 0)
	if err := ValidateHeartbeat(ok, receive); err != nil {
		t.Errorf("literal U+FFFD must be accepted: %v", err)
	}
}
