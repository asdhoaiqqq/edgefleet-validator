package edgefleet

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

// These tests pin the one object rule shared by the submit boundary and the
// stored-file boundary: within a single JSON object, a decoded field name may
// appear at most once. Plain text and JSON Unicode escape spellings of the
// same name are equal after decoding; the rule never reaches across objects.
// They complement stored_corruption_test.go (which already writes real
// \uXXXX escapes) by exercising genuine escape spellings on the submit path
// and on the envelope's outer keys.

// Escaped spellings of known keys, kept as literal backslash-u bytes so the
// JSON under test genuinely contains an escape rather than the plain name.
var (
	escMissedKey = "\"m\\u0069ssed\"" // i == U+0069
	escHeightKey = "\"h\\u0065ight\"" // e == U+0065
	escNodeKey   = "\"n\\u006fde\""   // o == U+006F
	escFormatKey = "\"\\u0066ormat\"" // f == U+0066
)

func TestParseHeartbeatsDuplicateFieldViaUnicodeEscape(t *testing.T) {
	base := `"node":"val-1","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":1,"missed":0`
	cases := []struct {
		name  string
		input string
		field string
	}{
		{"missed plain then escaped", `[{` + base + `,` + escMissedKey + `:1}]`, "missed"},
		{"missed escaped then plain", `[{` + escMissedKey + `:1,` + base + `}]`, "missed"},
		{"height plain then escaped", `[{` + base + `,` + escHeightKey + `:2}]`, "height"},
		{"node escaped then plain", `[{` + escNodeKey + `:"a","node":"b","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":1,"missed":0}]`, "node"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseHeartbeats([]byte(tc.input), testBase)
			if err == nil {
				t.Fatalf("expected error for escaped duplicate %s, got nil", tc.field)
			}
			msg := err.Error()
			if !strings.Contains(msg, "record 1") {
				t.Errorf("error must name the 1-based record position, got: %v", err)
			}
			if !strings.Contains(msg, tc.field) || !strings.Contains(msg, "duplicate") {
				t.Errorf("error must mention duplicate field %q, got: %v", tc.field, err)
			}
			// Input wording stays distinct from saved-data corruption wording.
			if strings.Contains(msg, "corruption") || strings.Contains(msg, "envelope") {
				t.Errorf("submit error must keep input wording, got: %v", err)
			}
		})
	}
}

func TestParseHeartbeatsSingleEscapedKeyAccepted(t *testing.T) {
	// Every field present exactly once, with missed written only through its
	// Unicode escape spelling, keys reordered and extra whitespace: this is a
	// legal object and must submit unchanged.
	input := `[ {
		` + escMissedKey + ` : 7,
		"height" : 12345,
		"version" : "1.0",
		"collected_at" : "2026-10-01T11:59:00Z",
		"seq" : 1,
		"node" : "val-1"
	} ]`
	records, err := ParseHeartbeats([]byte(input), testBase)
	if err != nil {
		t.Fatalf("a single escaped key, reordered fields and whitespace must be accepted: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	got := records[0]
	if got.NodeID != "val-1" || got.Seq != 1 || got.Height != 12345 || got.Missed != 7 || got.Version != "1.0" {
		t.Errorf("decoded record wrong: %+v", got)
	}
}

func TestParseHeartbeatsSameFieldNameInDifferentRecordsAllowed(t *testing.T) {
	// The duplicate rule is scoped to one object. Two records each carrying
	// their own "missed" are two distinct fields, not a duplicate; record-level
	// dedup is a separate mechanism (and these are different nodes anyway).
	input := `[
		{"node":"n1","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":1,"missed":0},
		{"node":"n2","seq":2,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":2,"missed":0}
	]`
	records, err := ParseHeartbeats([]byte(input), testBase)
	if err != nil {
		t.Fatalf("same field name in separate records must not be a duplicate: %v", err)
	}
	if len(records) != 2 {
		t.Errorf("expected 2 records, got %d", len(records))
	}
}

func TestStoredEnvelopeDuplicateOuterKeyViaEscapeIsCorrupt(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	path := store.nodePath("n1")

	escChecksumKey := "\"\\u0063hecksum\"" // c == U+0063
	cases := []struct {
		name    string
		content string
		field   string
	}{
		{"duplicate format via escape", `{` + escFormatKey + `:"edgefleet-heartbeats-v1","format":"edgefleet-heartbeats-v1","checksum":"x","records":[]}`, "format"},
		{"duplicate checksum via escape", `{"format":"edgefleet-heartbeats-v1",` + escChecksumKey + `:"x","checksum":"y","records":[]}`, "checksum"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := os.WriteFile(path, []byte(tc.content), 0o644); err != nil {
				t.Fatal(err)
			}
			_, err := loadNodeFile(path)
			if err == nil || !IsCorrupt(err) {
				t.Fatalf("duplicate outer key %s must be corruption, got %v", tc.field, err)
			}
			msg := err.Error()
			if !strings.Contains(msg, path) {
				t.Errorf("error must keep the file source, got: %v", err)
			}
			if !strings.Contains(msg, fmt.Sprintf("duplicate envelope field %q", tc.field)) {
				t.Errorf("error must name the duplicate outer field %q, got: %v", tc.field, err)
			}
			// An outer-envelope problem must not be dressed up as a record
			// position inside records.
			if strings.Contains(msg, "record 1") || strings.Contains(msg, "record 0") {
				t.Errorf("outer envelope error must not carry a fabricated record number, got: %v", err)
			}

			// History and health (plain and baseline) refuse normal results.
			if _, err := store.History("n1"); err == nil || !IsCorrupt(err) {
				t.Errorf("history must refuse the corrupt file, got %v", err)
			}
			if _, err := store.Health("n1", testBase, "1.0", 0); err == nil || !IsCorrupt(err) {
				t.Errorf("health must refuse the corrupt file, got %v", err)
			}
			if _, err := store.HealthSince("n1", testBase, "1.0", 0, 1); err == nil || !IsCorrupt(err) {
				t.Errorf("baseline health must refuse the corrupt file, got %v", err)
			}
		})
	}
}

func TestStoredEnvelopeDuplicateOuterKeyBlocksSubmitWithoutOverwrite(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	path := store.nodePath("n1")
	// Outer key duplicated with a Unicode escape; duplicate detection happens
	// during envelope decode, before the (irrelevant) checksum.
	content := []byte(`{` + escFormatKey + `:"edgefleet-heartbeats-v1","format":"edgefleet-heartbeats-v1","checksum":"x","records":[]}`)
	if err := os.WriteFile(path, content, 0o644); err != nil {
		t.Fatal(err)
	}

	// Submitting new data for that node fails as corruption and must not
	// overwrite the original file.
	_, _, err = store.Submit([]Heartbeat{{
		NodeID: "n1", Seq: 1, CollectedAt: testBase.Add(-time.Minute),
		Version: "1.0", Height: 1, Missed: 0,
	}}, testBase)
	if err == nil || !IsCorrupt(err) {
		t.Fatalf("submit onto a corrupt node must fail with a corruption error, got %v", err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(content) {
		t.Errorf("corrupt file must be left exactly as found:\nbefore=%s\nafter =%s", content, after)
	}
}

func TestStoredSingleEscapedOuterOrRecordKeyIsReadable(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	path := store.nodePath("n1")

	// A record whose "missed" key is written once via a Unicode escape, and an
	// outer "checksum" key written once via an escape, with reordered/whitespace
	// spelling elsewhere. The checksum covers the canonical decoded records, so
	// the file is genuine and must read back normally.
	record := Heartbeat{
		NodeID:      "n1",
		Seq:         1,
		CollectedAt: time.Date(2026, 10, 1, 11, 59, 0, 0, time.UTC),
		Version:     "1.0",
		Height:      100,
		Missed:      0,
	}
	escRecord := `{"node":"n1","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":100,` + escMissedKey + `:0}`
	escChecksumKey := "\"\\u0063hecksum\"" // c == U+0063
	fileContent := fmt.Sprintf(
		`{ "format" : %q , `+escChecksumKey+` : %q , "records" : [ %s ] }`+"\n",
		formatMarker, checksumRecords([]Heartbeat{record}), escRecord)
	if err := os.WriteFile(path, []byte(fileContent), 0o644); err != nil {
		t.Fatal(err)
	}

	nf, err := loadNodeFile(path)
	if err != nil {
		t.Fatalf("a key written once via escape must be readable: %v", err)
	}
	if nf == nil || len(nf.Records) != 1 || !nf.Records[0].Equal(record) {
		t.Fatalf("decoded records wrong: %+v", nf)
	}
	hist, err := store.History("n1")
	if err != nil {
		t.Fatalf("history must serve the file: %v", err)
	}
	if len(hist) != 1 || !hist[0].Equal(record) {
		t.Errorf("history mismatch: %+v", hist)
	}
}
