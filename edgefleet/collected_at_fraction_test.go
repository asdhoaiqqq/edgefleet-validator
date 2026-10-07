package edgefleet

import (
	"os"
	"strings"
	"testing"
	"time"
)

// These tests pin the collected_at fractional-second rule at every boundary:
//
//   - a fraction of at most nine digits is exact and accepted as before;
//   - a longer fraction is accepted only when every digit past the ninth is
//     zero (".1234567890" is the same instant as ".123456789");
//   - any non-zero digit past the ninth rejects the record — Go's time.Parse
//     silently truncates such input, merging distinct collection moments
//     (".1234567891" and ".1234567892") into one nanosecond instant, which
//     would miscount genuinely different records as duplicates;
//   - the same rule guards the stored-file read path: a saved file carrying
//     an unrepresentable fraction is corruption naming the record and field,
//     even when its checksum matches the truncated interpretation.
//
// Any such failure rejects the whole batch before the first file is written.

func TestParseHeartbeatsRejectsFractionBeyondNanoseconds(t *testing.T) {
	cases := []struct {
		name  string
		stamp string
		frac  string // the offending fraction text the error must quote
	}{
		{"tenth digit non-zero", "2026-10-01T11:59:00.1234567891Z", ".1234567891"},
		{"same prefix, different tenth digit", "2026-10-01T11:59:00.1234567892Z", ".1234567892"},
		{"eleven digits, last non-zero", "2026-10-01T11:59:00.12345678901Z", ".12345678901"},
		{"non-zero far past the ninth", "2026-10-01T11:59:00.0000000000001Z", ".0000000000001"},
		{"non-zero tenth digit with offset", "2026-10-01T11:59:00.1234567891+08:00", ".1234567891"},
	}
	receive := testBase
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			input := `[{"node":"n1","seq":1,"collected_at":"` + tc.stamp + `","version":"1.0","height":1,"missed":0}]`
			records, err := ParseHeartbeats([]byte(input), receive)
			if err == nil {
				t.Fatalf("fraction %q must be rejected, not truncated", tc.frac)
			}
			if records != nil {
				t.Errorf("ParseHeartbeats must return no partial records, got %v", records)
			}
			msg := err.Error()
			for _, want := range []string{"record 1", "collected_at", tc.frac, "nanosecond"} {
				if !strings.Contains(msg, want) {
					t.Errorf("error %q must mention %q", msg, want)
				}
			}
		})
	}
}

// TestParseHeartbeatsRejectsTruncationCollision is the reported regression:
// two collection moments that differ only past the ninth fractional digit
// must not collapse into one instant. Both records are refused outright —
// they may never be silently merged and counted as duplicates.
func TestParseHeartbeatsRejectsTruncationCollision(t *testing.T) {
	input := `[
	  {"node":"n1","seq":1,"collected_at":"2026-10-01T11:59:00.1234567891Z","version":"1.0","height":1,"missed":0},
	  {"node":"n1","seq":1,"collected_at":"2026-10-01T11:59:00.1234567892Z","version":"1.0","height":1,"missed":0}
	]`
	records, err := ParseHeartbeats([]byte(input), testBase)
	if err == nil {
		t.Fatal("records distinguishable only past nanosecond precision must be rejected")
	}
	if records != nil {
		t.Errorf("no partial records may be returned, got %v", records)
	}
	if !strings.Contains(err.Error(), "record 1") || !strings.Contains(err.Error(), "collected_at") {
		t.Errorf("error must locate record 1 and collected_at, got %v", err)
	}
}

func TestParseHeartbeatsAcceptsExactFractionForms(t *testing.T) {
	cases := []struct {
		name  string
		stamp string
		want  time.Time
	}{
		{"no fraction", "2026-10-01T11:59:00Z", time.Date(2026, 10, 1, 11, 59, 0, 0, time.UTC)},
		{"one digit", "2026-10-01T11:59:00.5Z", time.Date(2026, 10, 1, 11, 59, 0, 500000000, time.UTC)},
		{"nine digits", "2026-10-01T11:59:00.123456789Z", time.Date(2026, 10, 1, 11, 59, 0, 123456789, time.UTC)},
		{"tenth digit zero", "2026-10-01T11:59:00.1234567890Z", time.Date(2026, 10, 1, 11, 59, 0, 123456789, time.UTC)},
		{"many trailing zeros", "2026-10-01T11:59:00.12345678900000000Z", time.Date(2026, 10, 1, 11, 59, 0, 123456789, time.UTC)},
		{"all-zero long fraction", "2026-10-01T11:59:00.0000000000000Z", time.Date(2026, 10, 1, 11, 59, 0, 0, time.UTC)},
		{"trailing zero with offset", "2026-10-01T11:59:00.1234567890+08:00", time.Date(2026, 10, 1, 11, 59, 0, 123456789, time.FixedZone("", 8*3600))},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			receive := time.Date(2026, 10, 2, 0, 0, 0, 0, time.UTC)
			input := `[{"node":"n1","seq":1,"collected_at":"` + tc.stamp + `","version":"1.0","height":1,"missed":0}]`
			records, err := ParseHeartbeats([]byte(input), receive)
			if err != nil {
				t.Fatalf("exactly representable fraction %q must be accepted, got %v", tc.stamp, err)
			}
			if !records[0].CollectedAt.Equal(tc.want) {
				t.Errorf("decoded instant %v, want %v", records[0].CollectedAt, tc.want)
			}
		})
	}
}

// TestParseHeartbeatsFractionJSONEscapeEquivalence pins that a time text
// spelled with JSON escapes is judged on the decoded string: an escaped
// trailing-zero fraction is accepted exactly like its literal form, and an
// escaped unrepresentable digit is rejected exactly like its literal form.
func TestParseHeartbeatsFractionJSONEscapeEquivalence(t *testing.T) {
	receive := testBase

	// "2026-10-01T11:59:00.1234567890Z" with the dot (.) and the
	// trailing zero (0) escaped: the same instant as the literal form.
	accepted := `[{"node":"n1","seq":1,"collected_at":"2026-10-01T11:59:00\u002E123456789\u0030Z","version":"1.0","height":1,"missed":0}]`
	records, err := ParseHeartbeats([]byte(accepted), receive)
	if err != nil {
		t.Fatalf("escaped trailing-zero fraction must be accepted: %v", err)
	}
	if records[0].CollectedAt.Nanosecond() != 123456789 {
		t.Errorf("escaped form decoded to %d ns, want 123456789", records[0].CollectedAt.Nanosecond())
	}

	// "2026-10-01T11:59:00.1234567891Z" with the tenth digit (1)
	// escaped: the same rejection as the literal spelling.
	rejected := `[{"node":"n1","seq":1,"collected_at":"2026-10-01T11:59:00.123456789\u0031Z","version":"1.0","height":1,"missed":0}]`
	_, err = ParseHeartbeats([]byte(rejected), receive)
	if err == nil {
		t.Fatal("escaped unrepresentable fraction must be rejected like the literal form")
	}
	if !strings.Contains(err.Error(), "collected_at") || !strings.Contains(err.Error(), ".1234567891") {
		t.Errorf("error must name collected_at and the decoded fraction, got %v", err)
	}
}

// TestSubmitRejectsUnrepresentableFractionWholeBatch mirrors the atomicity
// contract for the fraction rule: one record whose collection moment cannot
// be represented exactly fails the whole batch — the existing node's file
// stays byte-for-byte unchanged, no new node gains telemetry, and a record
// whose truncated form would match a saved one is not counted as a duplicate.
func TestSubmitRejectsUnrepresentableFractionWholeBatch(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase

	// "old" already has telemetry, including a record whose saved instant
	// equals the *truncated* form of the offending record below.
	existing := hb("old", 1, time.Date(2026, 10, 1, 11, 59, 0, 123456789, time.UTC), "1.0", 100, 0)
	if _, _, err := store.Submit([]Heartbeat{existing}, receive); err != nil {
		t.Fatal(err)
	}
	oldBefore, err := os.ReadFile(store.nodePath("old"))
	if err != nil {
		t.Fatal(err)
	}

	// The offending record truncates to the exact saved instant of old seq 1;
	// it must still be rejected, never counted as a duplicate of it.
	input := `[
	  {"node":"fresh","seq":1,"collected_at":"2026-10-01T11:59:30Z","version":"1.0","height":1,"missed":0},
	  {"node":"old","seq":2,"collected_at":"2026-10-01T11:59:30Z","version":"1.0","height":101,"missed":0},
	  {"node":"old","seq":1,"collected_at":"2026-10-01T11:59:00.1234567891Z","version":"1.0","height":100,"missed":0}
	]`
	records, err := ParseHeartbeats([]byte(input), receive)
	if err == nil {
		t.Fatal("batch with an unrepresentable fraction must be rejected at parse")
	}
	if records != nil {
		t.Fatalf("no partial records may be returned, got %v", records)
	}
	if !strings.Contains(err.Error(), "record 3") || !strings.Contains(err.Error(), "collected_at") {
		t.Errorf("error must locate record 3 and collected_at, got %v", err)
	}

	// Nothing was submitted, so the store is untouched.
	if after, rerr := os.ReadFile(store.nodePath("old")); rerr != nil || string(after) != string(oldBefore) {
		t.Fatalf("existing node file changed: err=%v", rerr)
	}
	hist, herr := store.History("fresh")
	if herr != nil {
		t.Fatal(herr)
	}
	if len(hist) != 0 {
		t.Errorf("new node gained telemetry despite rejected batch: %+v", hist)
	}
	if _, serr := os.Stat(store.nodePath("fresh")); !os.IsNotExist(serr) {
		t.Errorf("no file may be created for fresh, stat err=%v", serr)
	}
}

// TestStoredUnrepresentableFractionFileIsCorrupt forges a node file whose raw
// JSON carries a fraction past nanosecond precision, with a checksum matching
// the truncated interpretation. History and health must report corruption
// naming the record and field, and a submit to that node must refuse the
// whole batch without overwriting the file.
func TestStoredUnrepresentableFractionFileIsCorrupt(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	path := store.nodePath("n1")
	tamperedFile(t, path, `[{"node":"n1","seq":1,"collected_at":"2026-10-01T11:59:00.12345678901Z","version":"1.0","height":100,"missed":0}]`)
	corruptBefore, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	_, err = loadNodeFileFor(store, "n1")
	if err == nil || !IsCorrupt(err) {
		t.Fatalf("unrepresentable stored fraction must be corruption, got %v", err)
	}
	msg := err.Error()
	for _, want := range []string{"record 1", "collected_at", ".12345678901", "nanosecond"} {
		if !strings.Contains(msg, want) {
			t.Errorf("corruption error must mention %q, got %v", want, err)
		}
	}
	if _, err := store.Health("n1", testBase, "1.0", 0); err == nil || !IsCorrupt(err) {
		t.Errorf("health must refuse the corrupt data, got %v", err)
	}
	if _, err := store.History("n1"); err == nil || !IsCorrupt(err) {
		t.Errorf("history must refuse the corrupt data, got %v", err)
	}

	// Submitting to the corrupt node refuses the whole batch and leaves the
	// file byte-for-byte intact.
	good := hb("n1", 2, testBase.Add(-time.Minute), "1.0", 101, 0)
	if _, _, err := store.Submit([]Heartbeat{good}, testBase); err == nil || !IsCorrupt(err) {
		t.Fatalf("submit to a corrupt node must be refused as corruption, got %v", err)
	}
	if after, rerr := os.ReadFile(path); rerr != nil || string(after) != string(corruptBefore) {
		t.Fatalf("corrupt file was overwritten by the refused submit: err=%v", rerr)
	}
}

// TestStoredTrailingZeroFractionFileReads proves the legal long form on disk:
// a saved record whose fraction only adds trailing zeros past the ninth digit
// denotes an exactly representable instant and stays readable.
func TestStoredTrailingZeroFractionFileReads(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	path := store.nodePath("n1")
	tamperedFile(t, path, `[{"node":"n1","seq":1,"collected_at":"2026-10-01T11:59:00.1234567890Z","version":"1.0","height":100,"missed":0}]`)

	nf, err := loadNodeFileFor(store, "n1")
	if err != nil {
		t.Fatalf("trailing-zero fraction must stay readable, got %v", err)
	}
	want := time.Date(2026, 10, 1, 11, 59, 0, 123456789, time.UTC)
	if len(nf.Records) != 1 || !nf.Records[0].CollectedAt.Equal(want) {
		t.Fatalf("stored instant changed: %+v", nf.Records)
	}
	hist, err := store.History("n1")
	if err != nil {
		t.Fatal(err)
	}
	if len(hist) != 1 || hist[0].CollectedAt.Nanosecond() != 123456789 {
		t.Errorf("history lost the nanosecond fraction: %+v", hist)
	}
}

// TestSameSeqOneNanosecondApartConflicts pins the other side of the rule:
// two legal records for the same node and seq whose collection instants
// differ by a single nanosecond are a content conflict, not a duplicate.
func TestSameSeqOneNanosecondApartConflicts(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase
	base := time.Date(2026, 10, 1, 11, 59, 0, 123456789, time.UTC)
	first := hb("n1", 1, base, "1.0", 100, 0)
	if _, _, err := store.Submit([]Heartbeat{first}, receive); err != nil {
		t.Fatal(err)
	}
	second := hb("n1", 1, base.Add(time.Nanosecond), "1.0", 100, 0)
	newC, dupC, err := store.Submit([]Heartbeat{second}, receive)
	if err == nil {
		t.Fatal("same node+seq one nanosecond apart must conflict, not dedup")
	}
	if newC != 0 || dupC != 0 {
		t.Errorf("conflicted batch counters must be 0/0, got new=%d dup=%d", newC, dupC)
	}
	if !strings.Contains(err.Error(), "conflict") {
		t.Errorf("error must report a content conflict, got %v", err)
	}
	hist, herr := store.History("n1")
	if herr != nil {
		t.Fatal(herr)
	}
	if len(hist) != 1 || !hist[0].Equal(first) {
		t.Errorf("original record must survive the conflict: %+v", hist)
	}
}
