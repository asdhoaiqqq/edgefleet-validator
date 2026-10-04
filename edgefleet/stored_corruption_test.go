package edgefleet

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

// These tests protect the read-side trust boundary. Submit input is parsed
// strictly, but the saved file itself must be held to the same rules: every
// telemetry field explicitly present, non-null and written once. An attacker
// who can edit the file must not be able to delete a field whose saved value
// happened to be zero (missed=0, height=0), write null, or repeat a field,
// and then have health queries treat the zero-filled interpretation as real
// telemetry — not even when the stored checksum matches that interpretation.

// tamperedFile writes a node file whose checksum matches the records *as a
// lenient struct unmarshal would interpret them*: a missing field decodes to
// zero and duplicate keys keep the last value, exactly the values over which
// a naive reader would (re)compute a matching checksum. This simulates the
// strongest version of the attack.
func tamperedFile(t *testing.T, path, recordsJSON string) {
	t.Helper()
	var interpreted []Heartbeat
	if err := json.Unmarshal([]byte(recordsJSON), &interpreted); err != nil {
		t.Fatalf("test setup: lenient decode failed: %v", err)
	}
	content := fmt.Sprintf(`{"format":%q,"checksum":%q,"records":%s}`,
		formatMarker, checksumRecords(interpreted), recordsJSON)
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

// sha256Hex mirrors checksumRecords for raw JSON the test builds by hand.
func sha256Hex(t *testing.T, data []byte) string {
	t.Helper()
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

const validRecordJSON = `{"node":"n1","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":100,"missed":0}`

func TestStoredMissingFieldWithMatchingChecksumIsCorrupt(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	path := store.nodePath("n1")

	cases := []struct {
		name        string
		recordsJSON string
		field       string
	}{
		{"missing node", `[{"seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":100,"missed":0}]`, "node"},
		{"missing seq", `[{"node":"n1","collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":100,"missed":0}]`, "seq"},
		{"missing collected_at", `[{"node":"n1","seq":1,"version":"1.0","height":100,"missed":0}]`, "collected_at"},
		{"missing version", `[{"node":"n1","seq":1,"collected_at":"2026-10-01T11:59:00Z","height":100,"missed":0}]`, "version"},
		// The exact regression: the record really had height 0; deleting the
		// field and reusing a checksum computed over the zero-filled record
		// must not make height 0 look like reported telemetry.
		{"missing height that was zero", `[{"node":"n1","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","missed":0}]`, "height"},
		{"missing missed that was zero", `[{"node":"n1","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":100}]`, "missed"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tamperedFile(t, path, tc.recordsJSON)

			_, err := loadNodeFile(path, "n1")
			if err == nil || !IsCorrupt(err) {
				t.Fatalf("read must report corruption, got %v", err)
			}
			msg := err.Error()
			if !strings.Contains(msg, path) || !strings.Contains(msg, "record 1") || !strings.Contains(msg, tc.field) {
				t.Errorf("error must name file, record 1 and field %q: %v", tc.field, err)
			}

			// Health, baseline health and history all refuse with no result.
			if _, err := store.Health("n1", testBase, "1.0", 0); err == nil || !IsCorrupt(err) {
				t.Errorf("health must refuse corrupt data, got %v", err)
			}
			if _, err := store.HealthSince("n1", testBase, "1.0", 0, 1); err == nil || !IsCorrupt(err) {
				t.Errorf("health with baseline must refuse corrupt data, got %v", err)
			}
			if _, err := store.History("n1"); err == nil || !IsCorrupt(err) {
				t.Errorf("history must refuse corrupt data, got %v", err)
			}
		})
	}
}

func TestStoredNullFieldWithMatchingChecksumIsCorrupt(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	path := store.nodePath("n1")

	cases := []struct {
		name        string
		recordsJSON string
		field       string
	}{
		{"null node", `[{"node":null,"seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":100,"missed":0}]`, "node"},
		{"null seq", `[{"node":"n1","seq":null,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":100,"missed":0}]`, "seq"},
		{"null collected_at", `[{"node":"n1","seq":1,"collected_at":null,"version":"1.0","height":100,"missed":0}]`, "collected_at"},
		{"null version", `[{"node":"n1","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":null,"height":100,"missed":0}]`, "version"},
		{"null height", `[{"node":"n1","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":null,"missed":0}]`, "height"},
		{"null missed", `[{"node":"n1","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":100,"missed":null}]`, "missed"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tamperedFile(t, path, tc.recordsJSON)
			_, err := loadNodeFile(path, "n1")
			if err == nil || !IsCorrupt(err) {
				t.Fatalf("null %s must be corruption, got %v", tc.field, err)
			}
			msg := err.Error()
			if !strings.Contains(msg, tc.field) || !strings.Contains(msg, "null") {
				t.Errorf("error must mention field %q and null: %v", tc.field, err)
			}
		})
	}
}

func TestStoredDuplicateFieldWithMatchingChecksumIsCorrupt(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	path := store.nodePath("n1")

	const obj = `"node":"n1","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":100,"missed":0`
	// Literal JSON Unicode escapes denoting the same keys as "missed"
	// (i=U+0069), "height" (e=U+0065) and "node" (o=U+006F).
	escMissed := "\"m\\u0069ssed\""
	escHeight := "\"h\\u0065ight\""
	escNode := "\"n\\u006fde\""
	cases := []struct {
		name        string
		recordsJSON string
		field       string
	}{
		{"duplicate missed same value", `[{` + obj + `,"missed":0}]`, "missed"},
		{"duplicate missed diff value", `[{` + obj + `,"missed":1}]`, "missed"},
		{"duplicate height same value", `[{` + obj + `,"height":100}]`, "height"},
		{"duplicate node", `[{"node":"n1",` + obj + `}]`, "node"},
		{"duplicate seq", `[{` + obj + `,"seq":2}]`, "seq"},
		{"duplicate collected_at", `[{` + obj + `,"collected_at":"2026-10-01T11:59:00Z"}]`, "collected_at"},
		{"duplicate version", `[{` + obj + `,"version":"2.0"}]`, "version"},
		// A Unicode-escaped spelling of the same key counts as a duplicate.
		{"duplicate missed via unicode escape", `[{` + obj + `,` + escMissed + `:1}]`, "missed"},
		{"duplicate height via unicode escape", `[{` + obj + `,` + escHeight + `:101}]`, "height"},
		{"duplicate node via unicode escape", `[{` + escNode + `:"n2",` + obj + `}]`, "node"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tamperedFile(t, path, tc.recordsJSON)
			_, err := loadNodeFile(path, "n1")
			if err == nil || !IsCorrupt(err) {
				t.Fatalf("duplicate %s must be corruption even with a matching checksum, got %v", tc.field, err)
			}
			msg := err.Error()
			if !strings.Contains(msg, tc.field) || !strings.Contains(msg, "duplicate") {
				t.Errorf("error must mention duplicate field %q: %v", tc.field, err)
			}
			if _, err := store.History("n1"); err == nil || !IsCorrupt(err) {
				t.Errorf("history must refuse duplicate-field data, got %v", err)
			}
		})
	}
}

func TestStoredCorruptionNamesRecordPosition(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	path := store.nodePath("n1")
	good := validRecordJSON
	// Second of three records drops "missed"; the checksum matches the
	// zero-filled interpretation of all three.
	recordsJSON := "[" + good + "," +
		`{"node":"n1","seq":2,"collected_at":"2026-10-01T11:59:30Z","version":"1.0","height":101},` +
		`{"node":"n1","seq":3,"collected_at":"2026-10-01T11:59:45Z","version":"1.0","height":102,"missed":0}]`
	tamperedFile(t, path, recordsJSON)
	_, err = loadNodeFile(path, "n1")
	if err == nil {
		t.Fatal("expected corruption")
	}
	msg := err.Error()
	if !strings.Contains(msg, "record 2") {
		t.Errorf("error must identify record 2, got: %v", err)
	}
	if !strings.Contains(msg, "missed") {
		t.Errorf("error must name the missing field, got: %v", err)
	}
}

func TestStoredEnvelopeTamperingIsCorrupt(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	path := store.nodePath("n1")
	records := "[" + validRecordJSON + "]"
	recordsSum := sha256Hex(t, mustMarshal(t, []Heartbeat{{
		NodeID: "n1", Seq: 1,
		CollectedAt: time.Date(2026, 10, 1, 11, 59, 0, 0, time.UTC),
		Version:     "1.0", Height: 100,
	}}))

	cases := []struct {
		name    string
		content string
	}{
		{"missing checksum", fmt.Sprintf(`{"format":%q,"records":%s}`, formatMarker, records)},
		{"missing format", fmt.Sprintf(`{"checksum":%q,"records":%s}`, recordsSum, records)},
		{"missing records", fmt.Sprintf(`{"format":%q,"checksum":%q}`, formatMarker, recordsSum)},
		{"null records", fmt.Sprintf(`{"format":%q,"checksum":%q,"records":null}`, formatMarker, recordsSum)},
		{"null checksum", fmt.Sprintf(`{"format":%q,"checksum":null,"records":%s}`, formatMarker, records)},
		{"unknown envelope field", fmt.Sprintf(`{"format":%q,"checksum":%q,"records":%s,"extra":1}`, formatMarker, recordsSum, records)},
		{"duplicate format", fmt.Sprintf(`{"format":%q,"format":%q,"checksum":%q,"records":%s}`, formatMarker, formatMarker, recordsSum, records)},
		{"duplicate records via escape", "{\"format\":" + fmt.Sprintf("%q", formatMarker) + ",\"checksum\":" + fmt.Sprintf("%q", recordsSum) + ",\"records\":" + records + ",\"\\u0072ecords\":[]}"},
		{"trailing garbage", fmt.Sprintf(`{"format":%q,"checksum":%q,"records":%s} junk`, formatMarker, recordsSum, records)},
		{"records not an array", fmt.Sprintf(`{"format":%q,"checksum":%q,"records":%s}`, formatMarker, recordsSum, validRecordJSON)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := os.WriteFile(path, []byte(tc.content), 0o644); err != nil {
				t.Fatal(err)
			}
			if _, err := loadNodeFile(path, "n1"); err == nil || !IsCorrupt(err) {
				t.Errorf("expected corruption, got %v", err)
			}
		})
	}
}

func TestSubmitRejectsBatchTouchingCorruptNodeWithoutWrites(t *testing.T) {
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
	badPath := store.nodePath("bad")
	tamperedFile(t, badPath, `[{"node":"bad","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":100}]`)

	badBefore, err := os.ReadFile(badPath)
	if err != nil {
		t.Fatal(err)
	}
	goodPath := store.nodePath("good")
	goodBefore, err := os.ReadFile(goodPath)
	if err != nil {
		t.Fatal(err)
	}

	// Batch spans the corrupt node, the existing good node and a brand new
	// node. The whole batch must be refused before anything is saved.
	batch := []Heartbeat{
		hb("bad", 2, receive.Add(-time.Second), "1.0", 102, 0),
		hb("good", 2, receive.Add(-time.Second), "1.0", 101, 0),
		hb("fresh", 1, receive.Add(-time.Second), "1.0", 1, 0),
	}
	if _, _, err := store.Submit(batch, receive); err == nil || !IsCorrupt(err) {
		t.Fatalf("submit must refuse the batch with a corruption error, got %v", err)
	}

	badAfter, err := os.ReadFile(badPath)
	if err != nil {
		t.Fatal(err)
	}
	goodAfter, err := os.ReadFile(goodPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(badAfter) != string(badBefore) {
		t.Errorf("corrupt node file was modified: must stay exactly as found")
	}
	if string(goodAfter) != string(goodBefore) {
		t.Errorf("good node file was rewritten during the rejected batch")
	}
	if _, err := os.Stat(store.nodePath("fresh")); !os.IsNotExist(err) {
		t.Errorf("fresh node file must not be created, stat err=%v", err)
	}

	// Other, untouched nodes keep working normally.
	r, err := store.Health("good", receive, "1.0", 0)
	if err != nil {
		t.Fatalf("good node must stay queryable: %v", err)
	}
	if r.Seq != 1 {
		t.Errorf("good node seq=%d, want 1 (rejected batch changed nothing)", r.Seq)
	}
	if _, _, err := store.Submit([]Heartbeat{hb("good", 3, receive.Add(-time.Second), "1.0", 103, 0)}, receive); err != nil {
		t.Errorf("submitting to a different, healthy node must work: %v", err)
	}
}

func TestCorruptNodeDoesNotAffectOthers(t *testing.T) {
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
	tamperedFile(t, store.nodePath("bad"), `[{"node":"bad","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0"}]`)

	hist, err := store.History("good")
	if err != nil {
		t.Fatalf("history of healthy node must work: %v", err)
	}
	if len(hist) != 1 {
		t.Errorf("good history len=%d, want 1", len(hist))
	}
	r, err := store.Health("good", receive, "1.0", 0)
	if err != nil {
		t.Fatalf("health of healthy node must work: %v", err)
	}
	if r.Status != "online" || r.Height != 100 || r.Missed != 0 {
		t.Errorf("good node result must use real telemetry: %+v", r)
	}
}

func TestStoredLegitimateFilesStayReadable(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase

	// Explicit zero values are genuine telemetry and stay valid.
	zeros := []Heartbeat{hb("z", 1, receive.Add(-time.Second), "1.0", 0, 0)}
	// Large legal integers must survive exactly.
	const big = int64(9223372036854775807)
	bigs := []Heartbeat{hb("big", 1, receive.Add(-time.Second), "1.0", big, big-2)}
	if _, _, err := store.Submit(append(append([]Heartbeat{}, zeros...), bigs...), receive); err != nil {
		t.Fatal(err)
	}

	r, err := store.Health("z", receive, "1.0", 0)
	if err != nil {
		t.Fatalf("explicit zeros must stay readable: %v", err)
	}
	if r.Height != 0 || r.Missed != 0 {
		t.Errorf("zero telemetry misread: height=%d missed=%d", r.Height, r.Missed)
	}
	r, err = store.Health("big", receive, "1.0", 0)
	if err != nil {
		t.Fatalf("large integers must stay readable: %v", err)
	}
	if r.Height != big || r.Missed != big-2 {
		t.Errorf("large integers lost precision: height=%d missed=%d", r.Height, r.Missed)
	}

	// Reformat the file by hand: reordered envelope keys, reordered record
	// keys, changed whitespace, trailing newline. Same checksum semantics.
	path := store.nodePath("z")
	raw := fmt.Sprintf("  {\n\t\"records\" : [ %s ],\n"+
		"  \"checksum\":%q,\n\"format\":%q }\n",
		`{"missed":0 , "height":0,"version":"1.0","collected_at":"2026-10-01T11:59:59Z","seq":1,"node":"z"}`,
		checksumRecords(zeros), formatMarker)
	if err := os.WriteFile(path, []byte(raw), 0o644); err != nil {
		t.Fatal(err)
	}
	hist, err := store.History("z")
	if err != nil {
		t.Fatalf("whitespace/key-order changes must not look like corruption: %v", err)
	}
	if len(hist) != 1 || !hist[0].Equal(zeros[0]) {
		t.Errorf("reformatted file misread: %+v", hist)
	}

	// An empty record set with a correctly computed empty-set checksum is
	// still a valid (telemetry-less) file.
	emptyPath := store.nodePath("empty")
	emptyContent := fmt.Sprintf(`{"format":%q,"checksum":%q,"records":[]}`,
		formatMarker, checksumRecords([]Heartbeat{}))
	if err := os.WriteFile(emptyPath, []byte(emptyContent), 0o644); err != nil {
		t.Fatal(err)
	}
	hist, err = store.History("empty")
	if err != nil {
		t.Fatalf("empty records file must be readable: %v", err)
	}
	if len(hist) != 0 {
		t.Errorf("expected no records, got %+v", hist)
	}
	r, err = store.Health("empty", receive, "1.0", 0)
	if err != nil {
		t.Fatalf("empty records file must behave as no telemetry: %v", err)
	}
	if r.Status != "notelemetry" {
		t.Errorf("status=%q, want notelemetry", r.Status)
	}
}

// mustMarshal fails the test on JSON encoding errors.
func mustMarshal(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
