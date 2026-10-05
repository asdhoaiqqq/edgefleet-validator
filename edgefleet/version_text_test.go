package edgefleet

import (
	"os"
	"strings"
	"testing"
	"time"
)

// These tests protect the version text rule at every trust boundary, mirroring
// the node-id rule: a version submitted through heartbeat submit or through a
// direct Store.Submit call must be losslessly representable Unicode text.
// encoding/json silently rewrites invalid UTF-8 bytes and unpaired \u
// surrogate escapes to U+FFFD, which would turn different reports into
// duplicates of a version the node never reported and skew the health
// version comparison. A literal "�" typed by the user, Chinese and
// emoji text stay accepted; no number-dot format is imposed.

// recordWithVersion builds one heartbeat object around the given raw version
// token, for a fixed node and seq.
func recordWithVersion(versionToken string) string {
	return `[{"node":"val-1","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":` +
		versionToken + `,"height":1,"missed":0}]`
}

func TestParseHeartbeatsInvalidUTF8VersionRejected(t *testing.T) {
	// 0xFF is not valid UTF-8; a lenient decode would rewrite it to U+FFFD
	// and silently change the reported version.
	input := []byte(recordWithVersion(`"1.0-` + "\xff" + `"`))
	_, err := ParseHeartbeats(input, testBase)
	if err == nil {
		t.Fatalf("invalid UTF-8 in version must be rejected")
	}
	if !strings.Contains(err.Error(), "record 1") || !strings.Contains(err.Error(), "version") {
		t.Errorf("error must name the record position and the version field, got: %v", err)
	}

	// The same failure at a later batch position reports that position.
	two := `[
		{"node":"val-1","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":1,"missed":0},
		{"node":"val-2","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"2.0-` + "\xff" + `","height":1,"missed":0}
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
		{"lone high surrogate", `"1.0\uD83D"`},
		{"lone low surrogate", `"1.0\uDE00"`},
		{"high surrogate at end", `"\uD83D"`},
		{"high surrogate then plain char", `"\uD83Dx"`},
		{"high surrogate then non-surrogate escape", `"\uD83DA"`},
		{"high surrogate then another high", `"\uD83D\uD83D"`},
		{"high surrogate then escaped quote", `"\uD83D\""`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseHeartbeats([]byte(recordWithVersion(tc.versionToken)), testBase)
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
		{"dotted", `"1.26.0"`, "1.26.0"},
		{"chinese", `"版本甲"`, "版本甲"},
		{"literal replacement char", `"1.0-�"`, "1.0-�"},
		{"escaped replacement char", `"1.0-\uFFFD"`, "1.0-\ufffd"},
		{"surrogate pair emoji", `"v-\uD83D\uDE00"`, "v-\U0001F600"},
		{"literal emoji", `"v-😀"`, "v-\U0001F600"},
		{"escaped ascii dot", `"1\u002e0"`, "1.0"},
		// A backslash that genuinely belongs to the version text stays a
		// literal backslash: the b is part of the string content,
		// not a JSON escape, so it is never re-interpreted as "b".
		{"literal backslash escape text", `"1.0\\u0062eta"`, "1.0\\u0062eta"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			records, err := ParseHeartbeats([]byte(recordWithVersion(tc.versionToken)), testBase)
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
	literal := recordWithVersion(`"v-😀"`)
	records, err := ParseHeartbeats([]byte(literal), receive)
	if err != nil {
		t.Fatalf("literal form rejected: %v", err)
	}
	newC, dupC, err := store.Submit(records, receive)
	if err != nil || newC != 1 || dupC != 0 {
		t.Fatalf("literal submit: new=%d dup=%d err=%v, want 1/0/nil", newC, dupC, err)
	}

	// The same text written as a surrogate pair is the same version: the
	// record is a duplicate, not a conflict.
	escaped := recordWithVersion(`"v-\uD83D\uDE00"`)
	records, err = ParseHeartbeats([]byte(escaped), receive)
	if err != nil {
		t.Fatalf("escaped form rejected: %v", err)
	}
	newC, dupC, err = store.Submit(records, receive)
	if err != nil || newC != 0 || dupC != 1 {
		t.Errorf("escaped submit: new=%d dup=%d err=%v, want 0/1/nil", newC, dupC, err)
	}

	hist, err := store.History("val-1")
	if err != nil || len(hist) != 1 || hist[0].Version != "v-\U0001F600" {
		t.Errorf("history mismatch: %+v err=%v", hist, err)
	}
}

func TestLiteralBackslashVersionNotReinterpreted(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase

	// The literal version text is 1.0\u0062eta (a real backslash
	// followed by letters), which must not decode a second time into
	// "1.0beta".
	literalBackslash, err := ParseHeartbeats(
		[]byte(recordWithVersion(`"1.0\\u0062eta"`)), receive)
	if err != nil {
		t.Fatalf("literal-backslash version rejected: %v", err)
	}
	if _, _, err := store.Submit(literalBackslash, receive); err != nil {
		t.Fatalf("literal-backslash submit: %v", err)
	}

	// A genuinely different record whose version decodes the escape to "1.0beta"
	// is different telemetry: a conflict, not a duplicate.
	decoded := hb("val-1", 1, receive.Add(-time.Minute), "1.0beta", 1, 0)
	if _, _, err := store.Submit([]Heartbeat{decoded}, receive); err == nil {
		t.Errorf("literal %q must not equal decoded %q: expected conflict",
			literalBackslash[0].Version, decoded.Version)
	}

	// Health compares versions exactly: the expected literal text matches only
	// the literal record.
	r, err := store.Health("val-1", receive, "1.0\\u0062eta", 0)
	if err != nil {
		t.Fatalf("exact literal expected version must match: %v", err)
	}
	for _, f := range r.Findings {
		if strings.Contains(f, "version skew") {
			t.Errorf("literal expected version must not skew, findings=%v", r.Findings)
		}
	}
	r, err = store.Health("val-1", receive, "1.0beta", 0)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.Join(r.Findings, ";"), "version skew") {
		t.Errorf("decoded expected version must skew against the literal record, findings=%v", r.Findings)
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
	bad := hb("bad", 1, receive.Add(-time.Minute), "2.0-\xff", 100, 0)
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

	for _, node := range []string{"bad", "other"} {
		hist, err := store.History(node)
		if err != nil {
			t.Fatal(err)
		}
		if len(hist) != 0 {
			t.Errorf("node %q got records despite rejected batch: %+v", node, hist)
		}
	}

	// The pre-existing node is untouched and still healthy.
	hist, err := store.History("good")
	if err != nil || len(hist) != 1 {
		t.Fatalf("good node history changed: %v %+v", err, hist)
	}
	r, err := store.Health("good", receive, "1.0", 0)
	if err != nil || r.Status != "online" {
		t.Errorf("good node health changed: %v %+v", err, r)
	}
}

func TestValidateHeartbeatRejectsInvalidUTF8Version(t *testing.T) {
	receive := testBase
	bad := hb("n1", 1, receive.Add(-time.Minute), "v\xff", 10, 0)
	err := ValidateHeartbeat(bad, receive)
	if err == nil || !strings.Contains(err.Error(), "version") {
		t.Errorf("ValidateHeartbeat must reject invalid UTF-8 version, got: %v", err)
	}
	// A literal replacement character, Chinese and emoji are valid text.
	for _, v := range []string{"v-�", "版本甲", "v-😀"} {
		ok := hb("n1", 1, receive.Add(-time.Minute), v, 10, 0)
		if err := ValidateHeartbeat(ok, receive); err != nil {
			t.Errorf("valid version %q must be accepted: %v", v, err)
		}
	}
}

// TestStoredInvalidVersionTextIsCorrupt protects the read boundary: a saved
// record whose version holds invalid UTF-8 or an unpaired surrogate is
// refused as corrupt even when the checksum matches the U+FFFD-rewritten
// text a lenient reader would reconstruct. No query returns partial records
// or a health conclusion, a submit touching the node does not overwrite
// the file, and other nodes keep working.
func TestStoredInvalidVersionTextIsCorrupt(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase
	// A clean node with one saved record.
	if _, _, err := store.Submit([]Heartbeat{
		hb("good", 1, receive.Add(-time.Minute), "1.0", 100, 0),
	}, receive); err != nil {
		t.Fatal(err)
	}
	path := store.nodePath("n1")

	cases := []struct {
		name        string
		recordsJSON string
	}{
		{"invalid utf-8 bytes", `[{"node":"n1","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0-` + "\xff" + `","height":100,"missed":0}]`},
		{"unpaired high surrogate", `[{"node":"n1","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0\uD83D","height":100,"missed":0}]`},
		{"unpaired low surrogate", `[{"node":"n1","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0\uDE00","height":100,"missed":0}]`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// tamperedFile computes the checksum over the U+FFFD-rewritten
			// interpretation, exactly what a lenient reader would trust.
			tamperedFile(t, path, tc.recordsJSON)
			badBefore, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}

			if _, err := loadNodeFile(path); err == nil || !IsCorrupt(err) {
				t.Fatalf("read must report corruption, got %v", err)
			} else {
				msg := err.Error()
				if !strings.Contains(msg, path) || !strings.Contains(msg, "record 1") || !strings.Contains(msg, "version") {
					t.Errorf("error must name file, record 1 and the version field: %v", err)
				}
			}

			// History, plain health and baseline health all refuse with no
			// result and no partial records.
			if recs, err := store.History("n1"); err == nil || !IsCorrupt(err) {
				t.Errorf("history must refuse corrupt data, got %v records=%v", err, recs)
			}
			if r, err := store.Health("n1", receive, "1.0-�", 0); err == nil || !IsCorrupt(err) {
				t.Errorf("health must refuse corrupt data, got %v result=%+v", err, r)
			}
			if r, err := store.HealthSince("n1", receive, "1.0-�", 0, 1); err == nil || !IsCorrupt(err) {
				t.Errorf("baseline health must refuse corrupt data, got %v result=%+v", err, r)
			}

			// A submit batch touching the corrupt node fails and does not
			// overwrite the file — even with a valid new record.
			batch := []Heartbeat{
				hb("n1", 2, receive.Add(-time.Second), "1.0", 102, 0),
				hb("fresh", 1, receive.Add(-time.Second), "1.0", 1, 0),
			}
			if _, _, err := store.Submit(batch, receive); err == nil || !IsCorrupt(err) {
				t.Fatalf("submit must refuse the batch with a corruption error, got %v", err)
			}
			if after, err := os.ReadFile(path); err != nil || string(after) != string(badBefore) {
				t.Fatalf("corrupt file must stay byte-for-byte intact: %v", err)
			}
			if _, err := os.Stat(store.nodePath("fresh")); !os.IsNotExist(err) {
				t.Errorf("fresh node file must not be created, stat err=%v", err)
			}
		})
	}

	// Other, untouched nodes keep working normally.
	r, err := store.Health("good", receive, "1.0", 0)
	if err != nil || r.Status != "online" || r.Seq != 1 {
		t.Errorf("good node must stay queryable: %v %+v", err, r)
	}
	hist, err := store.History("good")
	if err != nil || len(hist) != 1 {
		t.Errorf("good node history must stay available: %v %+v", err, hist)
	}
}

// TestStoredValidUnicodeVersionsStayReadable ensures the tightened read
// boundary keeps genuine non-ASCII version text trusted, including a
// literal U+FFFD the user really reported.
func TestStoredValidUnicodeVersionsStayReadable(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase
	for i, v := range []string{"版本甲", "v-😀", "1.0-�"} {
		seq := int64(i + 1)
		r := hb("uni", seq, receive.Add(-time.Duration(int(3-seq))*time.Second), v, 100+seq, 0)
		if _, _, err := store.Submit([]Heartbeat{r}, receive); err != nil {
			t.Fatalf("valid version %q rejected: %v", v, err)
		}
	}
	hist, err := store.History("uni")
	if err != nil {
		t.Fatalf("saved non-ASCII versions must stay readable: %v", err)
	}
	want := []string{"版本甲", "v-😀", "1.0-�"}
	if len(hist) != len(want) {
		t.Fatalf("history len=%d, want %d", len(hist), len(want))
	}
	for i, v := range want {
		if hist[i].Version != v {
			t.Errorf("record %d version=%q, want %q", i+1, hist[i].Version, v)
		}
	}
	// Health compares the latest record's version exactly.
	r, err := store.Health("uni", receive, "1.0-�", 0)
	if err != nil {
		t.Fatal(err)
	}
	if r.Version != "1.0-�" || strings.Contains(strings.Join(r.Findings, ";"), "version skew") {
		t.Errorf("exact version comparison failed: %+v", r)
	}
}
