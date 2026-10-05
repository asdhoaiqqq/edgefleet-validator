package edgefleet

import (
	"os"
	"strings"
	"testing"
	"time"
)

// These tests protect the version text at the two submit trust boundaries and
// the stored-data read boundary, mirroring the node-id rules in
// node_text_test.go. A version must be losslessly representable Unicode text.
// encoding/json silently rewrites invalid UTF-8 bytes and unpaired
// surrogate escapes to U+FFFD, which would merge distinct versions into one
// replacement-char text: different reports would be counted as duplicates of
// each other, and the version-skew judgement would compare text the node
// never reported. Such input is rejected outright, never stored as a
// replacement-char version. A literal "�" typed by the user, Chinese and
// emoji are legal characters and stay accepted; no digits-and-dots format is
// imposed and no character is ever deleted, replaced or normalised.

// recordJSONVersion builds one heartbeat object around the given raw version
// token.
func recordJSONVersion(versionToken string) string {
	return `[{"node":"n1","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":` + versionToken + `,"height":1,"missed":0}]`
}

// bs is a single backslash, used to build JSON \u escape tokens explicitly.
const bs = "\\"

func TestParseHeartbeatsInvalidUTF8VersionRejected(t *testing.T) {
	// 0xFF is not valid UTF-8 anywhere; a lenient decode would turn it into
	// U+FFFD and silently rename the version.
	input := []byte(recordJSONVersion(`"1.0-` + "\xff" + `"`))
	_, err := ParseHeartbeats(input, testBase)
	if err == nil {
		t.Fatalf("invalid UTF-8 in version must be rejected")
	}
	if !strings.Contains(err.Error(), "record 1") || !strings.Contains(err.Error(), "version") {
		t.Errorf("error must name the record position and the version field, got: %v", err)
	}

	// The same failure at a later batch position reports that position.
	two := `[
		{"node":"a","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":1,"missed":0},
		{"node":"b","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"2.0-` + "\xff" + `","height":1,"missed":0}
	]`
	_, err = ParseHeartbeats([]byte(two), testBase)
	if err == nil || !strings.Contains(err.Error(), "record 2") || !strings.Contains(err.Error(), "version") {
		t.Errorf("error must name record 2 and the version field, got: %v", err)
	}
}

func TestParseHeartbeatsUnpairedSurrogateVersionRejected(t *testing.T) {
	cases := []struct {
		name         string
		versionToken string
	}{
		{"lone high surrogate", `"1.0` + bs + `uD83D"`},
		{"lone low surrogate", `"1.0\uDE00"`},
		{"high surrogate at end", `"\uD83D"`},
		{"high surrogate then plain char", `"\uD83Dx"`},
		{"high surrogate then non-surrogate escape", `"\uD83DA"`},
		{"high surrogate then another high", `"\uD83D\uD83D"`},
		{"high surrogate then escaped quote", `"\uD83D\""`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseHeartbeats([]byte(recordJSONVersion(tc.versionToken)), testBase)
			if err == nil {
				t.Fatalf("unpaired surrogate in version must be rejected")
			}
			if !strings.Contains(err.Error(), "record 1") || !strings.Contains(err.Error(), "version") {
				t.Errorf("error must name the record position and the version field, got: %v", err)
			}
		})
	}
}

func TestParseHeartbeatsValidUnicodeVersionAccepted(t *testing.T) {
	cases := []struct {
		name         string
		versionToken string
		want         string
	}{
		{"plain", `"1.26.0"`, "1.26.0"},
		{"not restricted to digits and dots", `"v2 正式版 beta"`, "v2 正式版 beta"},
		{"chinese", `"版本甲"`, "版本甲"},
		{"literal replacement char", `"1.0-�"`, "1.0-�"},
		{"escaped replacement char", `"1.0-` + bs + `uFFFD"`, "1.0-�"},
		{"surrogate pair emoji", `"1.0-` + bs + `uD83D` + bs + `uDE00"`, "1.0-😀"},
		{"literal emoji", `"1.0-😀"`, "1.0-😀"},
		{"escaped ascii", `"1.0` + bs + `u0031"`, "1.01"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			records, err := ParseHeartbeats([]byte(recordJSONVersion(tc.versionToken)), testBase)
			if err != nil {
				t.Fatalf("valid version rejected: %v", err)
			}
			if records[0].Version != tc.want {
				t.Errorf("version = %q, want %q", records[0].Version, tc.want)
			}
		})
	}
}

func TestVersionEscapedAndLiteralAreSameVersion(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase

	// Submit the literal form of a version that needs a surrogate pair.
	literal := recordJSONVersion(`"1.0-😀"`)
	records, err := ParseHeartbeats([]byte(literal), receive)
	if err != nil {
		t.Fatalf("literal form rejected: %v", err)
	}
	newC, dupC, err := store.Submit(records, receive)
	if err != nil || newC != 1 || dupC != 0 {
		t.Fatalf("literal submit: new=%d dup=%d err=%v, want 1/0/nil", newC, dupC, err)
	}

	// The same text written as a \u surrogate pair is the same version: the
	// record is a duplicate, not a conflict and not new.
	escaped := recordJSONVersion(`"1.0-` + bs + `uD83D` + bs + `uDE00"`)
	records, err = ParseHeartbeats([]byte(escaped), receive)
	if err != nil {
		t.Fatalf("escaped form rejected: %v", err)
	}
	newC, dupC, err = store.Submit(records, receive)
	if err != nil || newC != 0 || dupC != 1 {
		t.Errorf("escaped submit: new=%d dup=%d err=%v, want 0/1/nil", newC, dupC, err)
	}

	hist, err := store.History("n1")
	if err != nil {
		t.Fatal(err)
	}
	if len(hist) != 1 || hist[0].Version != "1.0-😀" {
		t.Errorf("history = %+v, want the single literal-form record", hist)
	}
}

func TestVersionLiteralBackslashIsNotReinterpreted(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase

	// The version genuinely contains a backslash followed by "u0041": in
	// JSON the backslash itself is escaped, so the decoded text keeps it. It
	// must not be read as the escape "1.0A" at any point.
	literalBackslash := "1.0" + bs + "u0041" // 9 chars: 1.0 then a literal backslash, u, 0041
	backslash := recordJSONVersion(`"1.0` + bs + bs + `u0041"`)
	records, err := ParseHeartbeats([]byte(backslash), receive)
	if err != nil {
		t.Fatalf("literal backslash version rejected: %v", err)
	}
	if records[0].Version != literalBackslash {
		t.Fatalf("version = %q, want the literal text %q", records[0].Version, literalBackslash)
	}
	if _, _, err := store.Submit(records, receive); err != nil {
		t.Fatal(err)
	}

	// Saved and read back, the text is still the literal backslash form, and
	// the health query compares it exactly.
	hist, err := store.History("n1")
	if err != nil {
		t.Fatal(err)
	}
	if len(hist) != 1 || hist[0].Version != literalBackslash {
		t.Errorf("history = %+v, want the literal backslash version", hist)
	}
	r, err := store.Health("n1", receive, literalBackslash, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range r.Findings {
		if strings.Contains(f, "version skew") {
			t.Errorf("exact expected version must not skew, findings=%v", r.Findings)
		}
	}

	// The escape form decodes to "1.0A" — different text: same node+seq with
	// a different version is a conflict, never a duplicate.
	escapeForm := recordJSONVersion(`"1.0` + bs + `u0041"`)
	records, err = ParseHeartbeats([]byte(escapeForm), receive)
	if err != nil {
		t.Fatalf("escape form rejected: %v", err)
	}
	if records[0].Version != "1.0A" {
		t.Fatalf("escape form decoded to %q, want %q", records[0].Version, "1.0A")
	}
	_, _, err = store.Submit(records, receive)
	if err == nil || !strings.Contains(err.Error(), "conflict") {
		t.Errorf("different version text must conflict, got: %v", err)
	}
}

func TestSubmitDirectInvalidUTF8VersionRejected(t *testing.T) {
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

	// A Go caller submitting a record whose version holds invalid UTF-8 gets
	// an error saying the version is invalid; counts are zero and nothing is
	// added — not even the valid record in the same batch.
	bad := hb("n1", 1, receive.Add(-time.Minute), "1.0-\xff", 100, 0)
	valid := hb("other", 1, receive.Add(-time.Minute), "1.0", 100, 0)
	newC, dupC, err := store.Submit([]Heartbeat{bad, valid}, receive)
	if err == nil {
		t.Fatalf("invalid UTF-8 version must be rejected")
	}
	if !strings.Contains(err.Error(), "version") {
		t.Errorf("error must say the version is invalid, got: %v", err)
	}
	if newC != 0 || dupC != 0 {
		t.Errorf("counts = %d/%d, want 0/0", newC, dupC)
	}

	for _, node := range []string{"n1", "other"} {
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

func TestSubmitDirectInvalidVersionNotTreatedAsDuplicateOrConflict(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase

	// A version that differs from a saved one only by an invalid byte must
	// not merge into the saved record as a duplicate or a conflict.
	if _, _, err := store.Submit([]Heartbeat{hb("n1", 1, receive.Add(-time.Minute), "1.0-", 100, 0)}, receive); err != nil {
		t.Fatal(err)
	}
	_, _, err = store.Submit([]Heartbeat{hb("n1", 1, receive.Add(-time.Minute), "1.0-\xff", 100, 0)}, receive)
	if err == nil {
		t.Fatalf("invalid UTF-8 version must be rejected, not merged with %q", "1.0-")
	}
	hist, err := store.History("n1")
	if err != nil {
		t.Fatal(err)
	}
	if len(hist) != 1 || hist[0].Version != "1.0-" {
		t.Errorf("existing history changed: %+v", hist)
	}
}

func TestValidateHeartbeatRejectsInvalidUTF8Version(t *testing.T) {
	receive := testBase
	bad := hb("n1", 1, receive.Add(-time.Minute), "v\xff", 10, 0)
	err := ValidateHeartbeat(bad, receive)
	if err == nil || !strings.Contains(err.Error(), "version") {
		t.Errorf("ValidateHeartbeat must reject invalid UTF-8 version, got: %v", err)
	}
	// A literal replacement character is valid text and stays accepted.
	ok := hb("n1", 1, receive.Add(-time.Minute), "v-�", 10, 0)
	if err := ValidateHeartbeat(ok, receive); err != nil {
		t.Errorf("literal U+FFFD must be accepted: %v", err)
	}
}

// TestStoredInvalidVersionTextIsCorrupt protects the read boundary: a saved
// file whose version holds text that cannot decode losslessly is corrupt even
// when its checksum matches the replacement-char interpretation a lenient
// reader would compute. Every query for the node refuses, a submit batch
// touching the node fails without overwriting the file, and other nodes are
// unaffected.
func TestStoredInvalidVersionTextIsCorrupt(t *testing.T) {
	cases := []struct {
		name        string
		versionJSON string // raw version token in the tampered file
	}{
		{"invalid UTF-8 byte", `"1.26.0-` + "\xff" + `"`},
		{"unpaired high surrogate escape", `"1.26.0-` + bs + `uD83D"`},
		{"unpaired low surrogate escape", `"1.26.0-` + bs + `uDE00"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			store, err := OpenStore(dir)
			if err != nil {
				t.Fatal(err)
			}

			// Another node holds genuine data and keeps working throughout.
			receive := testBase
			if _, _, err := store.Submit([]Heartbeat{
				hb("good", 1, receive.Add(-time.Minute), "1.26.0", 100, 0),
			}, receive); err != nil {
				t.Fatal(err)
			}

			path := store.nodePath("bad")
			tampered := `[{"node":"bad","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":` + tc.versionJSON + `,"height":100,"missed":0}]`
			tamperedFile(t, path, tampered)
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}

			// The file is refused as corrupt even though its checksum matches
			// the replacement-char interpretation.
			if _, err := loadNodeFile(path); err == nil || !IsCorrupt(err) {
				t.Fatalf("read must report corruption, got %v", err)
			} else if !strings.Contains(err.Error(), "record 1") || !strings.Contains(err.Error(), "version") {
				t.Errorf("error must name record 1 and the version field, got: %v", err)
			}

			// History, plain health and baseline health all refuse with no
			// partial result.
			if _, err := store.History("bad"); err == nil || !IsCorrupt(err) {
				t.Errorf("history must refuse corrupt data, got %v", err)
			}
			if _, err := store.Health("bad", receive, "1.26.0", 0); err == nil || !IsCorrupt(err) {
				t.Errorf("health must refuse corrupt data, got %v", err)
			}
			if _, err := store.HealthSince("bad", receive, "1.26.0", 0, 1); err == nil || !IsCorrupt(err) {
				t.Errorf("health with baseline must refuse corrupt data, got %v", err)
			}

			// A submit batch touching the corrupt node fails as a whole: the
			// corrupt file is not overwritten and the valid record for the
			// healthy node in the same batch is not added either.
			newC, dupC, err := store.Submit([]Heartbeat{
				hb("bad", 2, receive.Add(-time.Minute), "1.26.0", 101, 0),
				hb("good", 2, receive.Add(-time.Minute), "1.26.0", 101, 0),
			}, receive)
			if err == nil || !IsCorrupt(err) {
				t.Errorf("submit touching corrupt node must fail with corruption, got new=%d dup=%d err=%v", newC, dupC, err)
			}
			if newC != 0 || dupC != 0 {
				t.Errorf("counts = %d/%d, want 0/0", newC, dupC)
			}
			after, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if string(after) != string(before) {
				t.Errorf("corrupt file was overwritten by the rejected submit")
			}
			hist, err := store.History("good")
			if err != nil || len(hist) != 1 {
				t.Errorf("healthy node gained records from the rejected batch: %v %+v", err, hist)
			}

			// The healthy node's queries are unaffected.
			r, err := store.Health("good", receive, "1.26.0", 0)
			if err != nil || r.Status != "online" {
				t.Errorf("healthy node health changed: %v %+v", err, r)
			}
		})
	}
}
