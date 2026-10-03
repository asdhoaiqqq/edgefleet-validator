package edgefleet

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// writeTamperedFile writes a node file whose recordsJSON may omit fields, use
// null or repeat keys. The checksum is computed over the records *as a lenient
// decoder would interpret them* (missing/null become zero, duplicates take the
// last value), i.e. exactly the forged checksum a damaged file would carry.
// Strict loading must reject the file even though the checksum matches the
// interpreted values.
func writeTamperedFile(t *testing.T, path, recordsJSON string) {
	t.Helper()
	var interpreted []Heartbeat
	if err := json.Unmarshal([]byte(recordsJSON), &interpreted); err != nil {
		t.Fatalf("test setup: lenient parse failed: %v", err)
	}
	content := fmt.Sprintf(`{"format":%q,"checksum":%q,"records":%s}`,
		formatMarker, checksumRecords(interpreted), recordsJSON)
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

// corruptChecksum computes the forged (interpreted-value) checksum for a
// records JSON array without writing anything.
func corruptChecksum(t *testing.T, recordsJSON string) string {
	t.Helper()
	var interpreted []Heartbeat
	if err := json.Unmarshal([]byte(recordsJSON), &interpreted); err != nil {
		t.Fatalf("test setup: %v", err)
	}
	return checksumRecords(interpreted)
}

func TestStoredMissingFieldWithMatchingChecksumIsCorrupt(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase
	if _, _, err := store.Submit([]Heartbeat{hb("n1", 1, receive.Add(-time.Minute), "1.0", 100, 5)}, receive); err != nil {
		t.Fatal(err)
	}
	path := store.nodePath("n1")

	cases := []struct {
		name        string
		recordsJSON string
		field       string
		record      string
	}{
		{
			"missing missed",
			`[{"node":"n1","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":100}]`,
			"missed", "record 1",
		},
		{
			"missing height",
			`[{"node":"n1","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","missed":5}]`,
			"height", "record 1",
		},
		{
			"missing node",
			`[{"seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":100,"missed":5}]`,
			"node", "record 1",
		},
		{
			"missing seq",
			`[{"node":"n1","collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":100,"missed":5}]`,
			"seq", "record 1",
		},
		{
			"missing collected_at",
			`[{"node":"n1","seq":1,"version":"1.0","height":100,"missed":5}]`,
			"collected_at", "record 1",
		},
		{
			"missing version",
			`[{"node":"n1","seq":1,"collected_at":"2026-10-01T11:59:00Z","height":100,"missed":5}]`,
			"version", "record 1",
		},
		{
			"null missed",
			`[{"node":"n1","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":100,"missed":null}]`,
			"missed", "record 1",
		},
		{
			"null height",
			`[{"node":"n1","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":null,"missed":5}]`,
			"height", "record 1",
		},
		{
			"missing field on second record",
			`[{"node":"n1","seq":1,"collected_at":"2026-10-01T11:58:00Z","version":"1.0","height":99,"missed":4},` +
				`{"node":"n1","seq":2,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":100}]`,
			"missed", "record 2",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			writeTamperedFile(t, path, tc.recordsJSON)

			_, err := store.Health("n1", receive, "1.0", 0)
			if !assertCorrupt(t, err, path, tc.record, tc.field) {
				t.Errorf("health did not report corruption: %v", err)
			}
			_, err = store.HealthSince("n1", receive, "1.0", 0, 1)
			if !assertCorrupt(t, err, path, tc.record, tc.field) {
				t.Errorf("health with baseline did not report corruption: %v", err)
			}
			_, err = store.History("n1")
			if !assertCorrupt(t, err, path, tc.record, tc.field) {
				t.Errorf("history did not report corruption: %v", err)
			}
			_, _, err = store.Submit([]Heartbeat{hb("n1", 9, receive.Add(-time.Minute), "1.0", 1, 0)}, receive)
			if !assertCorrupt(t, err, path, tc.record, tc.field) {
				t.Errorf("submit did not report corruption: %v", err)
			}
		})
	}
}

func TestStoredDuplicateFieldWithMatchingChecksumIsCorrupt(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase
	if _, _, err := store.Submit([]Heartbeat{hb("n1", 1, receive.Add(-time.Minute), "1.0", 100, 0)}, receive); err != nil {
		t.Fatal(err)
	}
	path := store.nodePath("n1")

	const obj = `"node":"n1","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0"`
	cases := []struct {
		name        string
		recordsJSON string
		field       string
	}{
		{"duplicate missed same value", `[{` + obj + `,"height":100,"missed":0,"missed":0}]`, "missed"},
		{"duplicate missed diff value", `[{` + obj + `,"height":100,"missed":0,"missed":1}]`, "missed"},
		{"duplicate height", `[{` + obj + `,"height":100,"height":101,"missed":0}]`, "height"},
		{"duplicate seq", `[{` + obj + `,"seq":1,"seq":2,"height":100,"missed":0}]`, "seq"},
		{"duplicate node", `[{` + obj + `,"node":"n2","height":100,"missed":0}]`, "node"},
		{"duplicate version", `[{` + obj + `,"version":"2.0","height":100,"missed":0}]`, "version"},
		{"duplicate collected_at", `[{` + obj + `,"collected_at":"2026-10-01T11:59:00Z","height":100,"missed":0}]`, "collected_at"},
		// "missed" written with a Unicode escape decodes to the same JSON key
		// and must still count as a duplicate, in either position.
		{"duplicate via unicode escape", `[{` + obj + `,"height":100,"missed":0,"m\u0069ssed":1}]`, "missed"},
		{"duplicate via unicode escape first", `[{` + obj + `,"height":100,"m\u0069ssed":0,"missed":1}]`, "missed"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			writeTamperedFile(t, path, tc.recordsJSON)
			_, err := store.Health("n1", receive, "1.0", 0)
			if !assertCorrupt(t, err, path, "record 1", tc.field) {
				t.Errorf("health: %v", err)
			}
			if err != nil && !strings.Contains(err.Error(), "duplicate") {
				t.Errorf("error should mention duplicate, got: %v", err)
			}
			_, err = store.History("n1")
			if !assertCorrupt(t, err, path, "record 1", tc.field) {
				t.Errorf("history: %v", err)
			}
		})
	}
}

func TestStoredDuplicateLastValueNotUsed(t *testing.T) {
	// A duplicated missed field whose last value is non-zero must not be
	// accepted with that value either — the record is simply corrupt.
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase
	recordsJSON := `[{"node":"n1","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":100,"missed":5,"missed":5}]`
	writeTamperedFile(t, store.nodePath("n1"), recordsJSON)

	if _, err := store.Health("n1", receive, "1.0", 0); !IsCorrupt(err) {
		t.Fatalf("duplicate key must corrupt even with identical values: %v", err)
	}
}

func TestEnvelopeTamperingIsCorrupt(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase
	recordsJSON := `[{"node":"n1","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":100,"missed":5}]`
	sum := corruptChecksum(t, recordsJSON)
	path := store.nodePath("n1")

	cases := []struct {
		name    string
		content string
		want    string
	}{
		{"null records", `{"format":"` + formatMarker + `","checksum":"` + sum + `","records":null}`, "records"},
		{"null checksum", `{"format":"` + formatMarker + `","checksum":null,"records":` + recordsJSON + `}`, "checksum"},
		{"null format", `{"format":null,"checksum":"` + sum + `","records":` + recordsJSON + `}`, "format"},
		{"missing records", `{"format":"` + formatMarker + `","checksum":"` + sum + `"}`, "records"},
		{"unknown header", `{"format":"` + formatMarker + `","checksum":"` + sum + `","records":` + recordsJSON + `,"extra":1}`, "extra"},
		{"duplicate header", `{"format":"` + formatMarker + `","format":"` + formatMarker + `","checksum":"` + sum + `","records":` + recordsJSON + `}`, "duplicate"},
		{"null array element", `{"format":"` + formatMarker + `","checksum":"` + corruptChecksum(t, `[]`) + `","records":[null]}`, "record 1"},
		{"trailing data", `{"format":"` + formatMarker + `","checksum":"` + sum + `","records":` + recordsJSON + `} junk`, "invalid JSON"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := os.WriteFile(path, []byte(tc.content), 0o644); err != nil {
				t.Fatal(err)
			}
			_, err := store.History("n1")
			if !assertCorruptContains(t, err, path, tc.want) {
				t.Errorf("history: %v", err)
			}
			_, err = store.Health("n1", receive, "1.0", 0)
			if !assertCorruptContains(t, err, path, tc.want) {
				t.Errorf("health: %v", err)
			}
		})
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
		hb("bad", 1, receive.Add(-time.Minute), "1.0", 100, 7),
	}, receive); err != nil {
		t.Fatal(err)
	}
	writeTamperedFile(t, store.nodePath("bad"),
		`[{"node":"bad","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":100}]`)

	// The good node keeps working through every entry point.
	hist, err := store.History("good")
	if err != nil || len(hist) != 1 || hist[0].Missed != 0 {
		t.Fatalf("good node history broken: %v %+v", err, hist)
	}
	r, err := store.Health("good", receive, "1.0", 0)
	if err != nil || r.Status != "online" || r.Missed != 0 {
		t.Fatalf("good node health broken: %v %+v", err, r)
	}
	if _, _, err := store.Submit([]Heartbeat{hb("good", 2, receive.Add(-time.Minute), "1.0", 101, 0)}, receive); err != nil {
		t.Fatalf("submit to good node failed: %v", err)
	}

	// The bad node must never serve a fabricated "missed=0" result, including
	// with a history baseline.
	if _, err := store.Health("bad", receive, "1.0", 0); !IsCorrupt(err) {
		t.Errorf("bad node health should be corrupt, got %v", err)
	}
	if _, err := store.HealthSince("bad", receive, "1.0", 0, 1); !IsCorrupt(err) {
		t.Errorf("bad node baseline health should be corrupt, got %v", err)
	}
}

func TestSubmitBatchTouchingCorruptNodeSavesNothing(t *testing.T) {
	// Map iteration order in Submit is randomised, so repeat to cover both
	// "bad node loaded first" and "good node loaded first".
	for iter := 0; iter < 10; iter++ {
		dir := t.TempDir()
		store, err := OpenStore(dir)
		if err != nil {
			t.Fatal(err)
		}
		receive := testBase
		if _, _, err := store.Submit([]Heartbeat{hb("bad", 1, receive.Add(-time.Minute), "1.0", 100, 7)}, receive); err != nil {
			t.Fatal(err)
		}
		badPath := store.nodePath("bad")
		before, err := os.ReadFile(badPath)
		if err != nil {
			t.Fatal(err)
		}
		writeTamperedFile(t, badPath,
			`[{"node":"bad","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":100}]`)
		forged, err := os.ReadFile(badPath)
		if err != nil {
			t.Fatal(err)
		}

		batch := []Heartbeat{
			hb("bad", 2, receive.Add(-time.Minute), "1.0", 101, 7),
			hb("newgood", 1, receive.Add(-time.Minute), "1.0", 1, 0),
		}
		_, _, err = store.Submit(batch, receive)
		if !assertCorruptContains(t, err, badPath, "missed") {
			t.Fatalf("iteration %d: batch should be rejected as corrupt, got %v", iter, err)
		}

		// The corrupt file is left exactly as found: no repair, no checksum
		// regeneration, no truncation.
		after, err := os.ReadFile(badPath)
		if err != nil {
			t.Fatal(err)
		}
		if string(after) != string(forged) {
			t.Fatalf("iteration %d: corrupt file was modified by submit", iter)
		}
		if string(after) == string(before) {
			t.Fatalf("iteration %d: test setup did not actually replace the file", iter)
		}
		// The innocent node in the same batch must have no file at all.
		if _, err := os.Stat(store.nodePath("newgood")); !os.IsNotExist(err) {
			t.Fatalf("iteration %d: newgood file created despite rejected batch: %v", iter, err)
		}

		// A follow-up batch containing only healthy nodes works normally.
		if _, _, err := store.Submit([]Heartbeat{hb("newgood", 1, receive.Add(-time.Minute), "1.0", 1, 0)}, receive); err != nil {
			t.Fatalf("iteration %d: submit to other node should work: %v", iter, err)
		}
	}
}

func TestStoredFileFormattingVariationsStayReadable(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase

	recordsJSON := ` [
		{ "missed" : 5 , "height" : 100 , "version" : "1.0" ,
		  "collected_at" : "2026-10-01T11:59:00Z" , "seq" : 1 , "node" : "n1" }
	] `
	var interpreted []Heartbeat
	if err := json.Unmarshal([]byte(recordsJSON), &interpreted); err != nil {
		t.Fatal(err)
	}
	sum := checksumRecords(interpreted)

	variants := []string{
		// Envelope keys reordered, generous whitespace and a trailing newline.
		`{ "records" : ` + recordsJSON + ` , "checksum" : "` + sum + `" , "format" : "` + formatMarker + `" }
`,
		// Compact, keys in the usual order.
		fmt.Sprintf(`{"format":"%s","checksum":"%s","records":%s}`, formatMarker, sum, recordsJSON),
		// Escaped spelling of a key still denotes the same field when it
		// occurs exactly once.
		`{"format":"` + formatMarker + `","checksum":"` + sum + `","records":[` +
			`{"node":"n1","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":100,"m\u0069ssed":5}]}`,
	}
	path := store.nodePath("n1")
	for i, content := range variants {
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
		hist, err := store.History("n1")
		if err != nil {
			t.Fatalf("variant %d must be readable: %v", i, err)
		}
		if len(hist) != 1 || hist[0].Height != 100 || hist[0].Missed != 5 {
			t.Fatalf("variant %d decoded wrong: %+v", i, hist)
		}
		r, err := store.Health("n1", receive, "1.0", 5)
		if err != nil {
			t.Fatalf("variant %d health failed: %v", i, err)
		}
		if r.Status != "online" || r.Missed != 5 {
			t.Fatalf("variant %d health wrong: %+v", i, r)
		}
	}
}

func TestStoredExplicitZeroValuesAndLargeIntegers(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase

	// Explicit height=0 and missed=0 are genuine telemetry, both through a
	// normal submit and when read back from disk.
	if _, _, err := store.Submit([]Heartbeat{hb("zero", 1, receive.Add(-time.Minute), "1.0", 0, 0)}, receive); err != nil {
		t.Fatal(err)
	}
	hist, err := store.History("zero")
	if err != nil {
		t.Fatal(err)
	}
	if len(hist) != 1 || hist[0].Height != 0 || hist[0].Missed != 0 {
		t.Fatalf("explicit zero values not preserved: %+v", hist)
	}

	// Large legal integers survive the strict on-disk parse exactly.
	const big = int64(9223372036854775807)
	large := fmt.Sprintf(`[{"node":"big","seq":%d,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":%d,"missed":%d}]`,
		big, big-1, big-2)
	var interpreted []Heartbeat
	if err := json.Unmarshal([]byte(large), &interpreted); err != nil {
		t.Fatal(err)
	}
	content := fmt.Sprintf(`{"format":%q,"checksum":%q,"records":%s}`,
		formatMarker, checksumRecords(interpreted), large)
	if err := os.WriteFile(store.nodePath("big"), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	hist, err = store.History("big")
	if err != nil {
		t.Fatalf("large integer file should be readable: %v", err)
	}
	if hist[0].Seq != big || hist[0].Height != big-1 || hist[0].Missed != big-2 {
		t.Fatalf("large integers lost precision: %+v", hist[0])
	}
}

func TestNormalWritesRemainStrictlyValid(t *testing.T) {
	// Every file the store itself writes must pass the new strict reader,
	// including zero-valued telemetry fields.
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase
	records := []Heartbeat{
		hb("n1", 1, receive.Add(-2*time.Minute), "1.0", 0, 0),
		hb("n1", 2, receive.Add(-time.Minute), "2.0", 12345, 9),
	}
	if _, _, err := store.Submit(records, receive); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(store.nodePath("n1"))
	if err != nil {
		t.Fatal(err)
	}
	nf, err := parseNodeFile(data)
	if err != nil {
		t.Fatalf("store-written file must pass strict parse: %v", err)
	}
	if len(nf.Records) != 2 || nf.Format != formatMarker || nf.Checksum == "" {
		t.Fatalf("parsed file wrong: %+v", nf)
	}
}

func assertCorrupt(t *testing.T, err error, path, record, field string) bool {
	t.Helper()
	if !assertCorruptContains(t, err, path, record) {
		return false
	}
	if !strings.Contains(err.Error(), field) {
		t.Errorf("error %q should name the field %q", err.Error(), field)
		return false
	}
	return true
}

func assertCorruptContains(t *testing.T, err error, path string, wants ...string) bool {
	t.Helper()
	if err == nil {
		t.Errorf("expected a corruption error, got nil")
		return false
	}
	if !IsCorrupt(err) {
		t.Errorf("expected CorruptError, got %T: %v", err, err)
	}
	msg := err.Error()
	if !strings.Contains(msg, filepath.Clean(path)) {
		t.Errorf("error %q should name the corrupt file %q", msg, path)
	}
	for _, w := range wants {
		if !strings.Contains(msg, w) {
			t.Errorf("error %q should contain %q", msg, w)
		}
	}
	return !t.Failed()
}
