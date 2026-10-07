package edgefleet

import (
	"os"
	"strings"
	"testing"
	"time"
)

// These tests pin that the collected_at fractional-second rule is identical
// for the two decimal separators time.Parse accepts, "." and ",". Before the
// fix the validator only searched for ".", so a comma fraction bypassed the
// nanosecond-exactness check: ",1234567891" and ",1234567892" truncated to
// the same instant and were miscounted as duplicates of one another.
//
// The rule for commas is exactly the dot rule:
//
//   - at most nine digits are exact and accepted;
//   - more than nine digits is accepted only when every digit past the ninth
//     is zero (",123456789000" is the same instant as ".123456789") and
//     duplicates are judged on that instant;
//   - any non-zero digit past the ninth rejects the record and the whole
//     batch, before any duplicate/conflict judgement;
//   - a saved file carrying an unrepresentable comma fraction is corruption,
//     even with a checksum matching the truncated interpretation;
//   - JSON-escaped spellings of the comma and digits are judged on the
//     decoded text, exactly like the dot spelling.

func TestParseHeartbeatsRejectsCommaFractionBeyondNanoseconds(t *testing.T) {
	cases := []struct {
		name  string
		stamp string
		frac  string // the offending fraction text the error must quote
	}{
		{"tenth digit non-zero", "2026-10-01T11:59:00,1234567891Z", ",1234567891"},
		{"same prefix, different tenth digit", "2026-10-01T11:59:00,1234567892Z", ",1234567892"},
		{"eleven digits, last non-zero", "2026-10-01T11:59:00,12345678901Z", ",12345678901"},
		{"non-zero far past the ninth", "2026-10-01T11:59:00,0000000000001Z", ",0000000000001"},
		{"non-zero tenth digit with offset", "2026-10-01T11:59:00,1234567891+08:00", ",1234567891"},
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

// TestParseHeartbeatsRejectsCommaTruncationCollision is the reported
// regression: two collection moments written with a comma that differ only
// past the ninth fractional digit must not collapse into one instant and be
// counted as duplicates. Both records are refused outright.
func TestParseHeartbeatsRejectsCommaTruncationCollision(t *testing.T) {
	input := `[
	  {"node":"n1","seq":1,"collected_at":"2026-10-01T11:59:00,1234567891Z","version":"1.0","height":1,"missed":0},
	  {"node":"n1","seq":1,"collected_at":"2026-10-01T11:59:00,1234567892Z","version":"1.0","height":1,"missed":0}
	]`
	records, err := ParseHeartbeats([]byte(input), testBase)
	if err == nil {
		t.Fatal("comma fractions distinguishable only past nanosecond precision must be rejected")
	}
	if records != nil {
		t.Errorf("no partial records may be returned, got %v", records)
	}
	if !strings.Contains(err.Error(), "record 1") || !strings.Contains(err.Error(), "collected_at") {
		t.Errorf("error must locate record 1 and collected_at, got %v", err)
	}
	if !strings.Contains(err.Error(), ",1234567891") {
		t.Errorf("error must quote the offending comma fraction, got %v", err)
	}
}

func TestParseHeartbeatsAcceptsExactCommaForms(t *testing.T) {
	cases := []struct {
		name  string
		stamp string
		want  time.Time
	}{
		{"nine digits", "2026-10-01T11:59:00,123456789Z", time.Date(2026, 10, 1, 11, 59, 0, 123456789, time.UTC)},
		{"tenth digit zero", "2026-10-01T11:59:00,1234567890Z", time.Date(2026, 10, 1, 11, 59, 0, 123456789, time.UTC)},
		{"trailing zeros past the ninth", "2026-10-01T11:59:00,123456789000Z", time.Date(2026, 10, 1, 11, 59, 0, 123456789, time.UTC)},
		{"many trailing zeros", "2026-10-01T11:59:00,12345678900000000Z", time.Date(2026, 10, 1, 11, 59, 0, 123456789, time.UTC)},
		{"all-zero long fraction", "2026-10-01T11:59:00,0000000000000Z", time.Date(2026, 10, 1, 11, 59, 0, 0, time.UTC)},
		{"trailing zero with offset", "2026-10-01T11:59:00,1234567890+08:00", time.Date(2026, 10, 1, 11, 59, 0, 123456789, time.FixedZone("", 8*3600))},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			receive := time.Date(2026, 10, 2, 0, 0, 0, 0, time.UTC)
			input := `[{"node":"n1","seq":1,"collected_at":"` + tc.stamp + `","version":"1.0","height":1,"missed":0}]`
			records, err := ParseHeartbeats([]byte(input), receive)
			if err != nil {
				t.Fatalf("exactly representable comma fraction %q must be accepted, got %v", tc.stamp, err)
			}
			if !records[0].CollectedAt.Equal(tc.want) {
				t.Errorf("decoded instant %v, want %v", records[0].CollectedAt, tc.want)
			}
		})
	}
}

// TestCommaFractionSameInstantAsDotDeduplicates pins the equality the task
// calls out: the comma form ",123456789000" denotes exactly the same
// fractional second as the dot form ".123456789", so the two are duplicates
// both within one batch and against saved history, with dot or comma saved
// first in either order.
func TestCommaFractionSameInstantAsDotDeduplicates(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase

	forms := []string{
		`{"node":"n1","seq":1,"collected_at":"2026-10-01T11:59:00.123456789Z","version":"1.0","height":1,"missed":0}`,
		`{"node":"n1","seq":1,"collected_at":"2026-10-01T11:59:00,123456789000Z","version":"1.0","height":1,"missed":0}`,
		`{"node":"n1","seq":1,"collected_at":"2026-10-01T19:59:00,123456789+08:00","version":"1.0","height":1,"missed":0}`,
	}

	// Within one batch: dot first, comma and offset respellings are duplicates.
	records, err := ParseHeartbeats([]byte("["+strings.Join(forms, ",")+"]"), receive)
	if err != nil {
		t.Fatalf("same-instant comma/dot forms must parse: %v", err)
	}
	newC, dupC, err := store.Submit(records, receive)
	if err != nil {
		t.Fatalf("same-instant comma/dot forms must dedup, got %v", err)
	}
	if newC != 1 || dupC != 2 {
		t.Errorf("want new=1 duplicate=2, got new=%d duplicate=%d", newC, dupC)
	}

	// Against saved history, each spelling on its own is still a duplicate,
	// regardless of which separator it uses.
	for _, form := range forms {
		recs, err := ParseHeartbeats([]byte("["+form+"]"), receive)
		if err != nil {
			t.Fatalf("form %q must parse: %v", form, err)
		}
		newC, dupC, err := store.Submit(recs, receive)
		if err != nil {
			t.Fatalf("form %q must be a duplicate, got %v", form, err)
		}
		if newC != 0 || dupC != 1 {
			t.Errorf("form %q: want new=0 duplicate=1, got new=%d duplicate=%d", form, newC, dupC)
		}
	}

	hist, err := store.History("n1")
	if err != nil {
		t.Fatal(err)
	}
	if len(hist) != 1 || hist[0].CollectedAt.Nanosecond() != 123456789 {
		t.Errorf("history must keep one record at the shared instant, got %+v", hist)
	}
}

// TestCommaFractionOneNanosecondApartConflicts proves legal comma records
// keep the ordinary conflict behaviour: two same-node/same-seq instants a
// nanosecond apart (one dot, one comma) conflict rather than dedup.
func TestCommaFractionOneNanosecondApartConflicts(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase
	input := `[
	  {"node":"n1","seq":1,"collected_at":"2026-10-01T11:59:00,123456789Z","version":"1.0","height":1,"missed":0},
	  {"node":"n1","seq":1,"collected_at":"2026-10-01T11:59:00.123456790Z","version":"1.0","height":1,"missed":0}
	]`
	records, err := ParseHeartbeats([]byte(input), receive)
	if err != nil {
		t.Fatalf("legal comma fraction must parse, got %v", err)
	}
	newC, dupC, err := store.Submit(records, receive)
	if err == nil {
		t.Fatal("same node+seq one nanosecond apart (comma/dot) must conflict")
	}
	if newC != 0 || dupC != 0 {
		t.Errorf("conflicted batch counters must be 0/0, got new=%d dup=%d", newC, dupC)
	}
}

// TestParseHeartbeatsCommaFractionJSONEscapeEquivalence pins that a time text
// whose comma or digits use JSON escapes is judged on the decoded string,
// just like an escaped dot: an escaped trailing-zero comma fraction is
// accepted as the same instant; an escaped non-zero tenth digit rejects.
func TestParseHeartbeatsCommaFractionJSONEscapeEquivalence(t *testing.T) {
	receive := testBase

	// "2026-10-01T11:59:00,123456789000Z" with the comma (U+002C) and the
	// final trailing zero (U+0030) escaped: same instant as ".123456789".
	accepted := `[{"node":"n1","seq":1,"collected_at":"2026-10-01T11:59:00\u002C12345678900\u0030Z","version":"1.0","height":1,"missed":0}]`
	records, err := ParseHeartbeats([]byte(accepted), receive)
	if err != nil {
		t.Fatalf("escaped trailing-zero comma fraction must be accepted: %v", err)
	}
	if records[0].CollectedAt.Nanosecond() != 123456789 {
		t.Errorf("escaped form decoded to %d ns, want 123456789", records[0].CollectedAt.Nanosecond())
	}

	// "2026-10-01T11:59:00,1234567891Z" with the comma (U+002C) and the
	// tenth digit, 1 (U+0031), escaped: the same rejection as literal text.
	rejected := `[{"node":"n1","seq":1,"collected_at":"2026-10-01T11:59:00\u002C123456789\u0031Z","version":"1.0","height":1,"missed":0}]`
	_, err = ParseHeartbeats([]byte(rejected), receive)
	if err == nil {
		t.Fatal("escaped unrepresentable comma fraction must be rejected like the literal form")
	}
	msg := err.Error()
	if !strings.Contains(msg, "collected_at") || !strings.Contains(msg, ",1234567891") {
		t.Errorf("error must name collected_at and the decoded fraction, got %v", err)
	}
}

// TestSubmitRejectsUnrepresentableCommaFractionWholeBatch mirrors the
// atomicity contract for the comma rule: one record whose comma fraction
// cannot be represented exactly fails the whole batch — the existing node's
// file stays byte-for-byte unchanged even though the truncated instant would
// match a saved record and count as a duplicate.
func TestSubmitRejectsUnrepresentableCommaFractionWholeBatch(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase

	existing := hb("old", 1, time.Date(2026, 10, 1, 11, 59, 0, 123456789, time.UTC), "1.0", 100, 0)
	if _, _, err := store.Submit([]Heartbeat{existing}, receive); err != nil {
		t.Fatal(err)
	}
	oldBefore, err := os.ReadFile(store.nodePath("old"))
	if err != nil {
		t.Fatal(err)
	}

	input := `[
	  {"node":"fresh","seq":1,"collected_at":"2026-10-01T11:59:30Z","version":"1.0","height":1,"missed":0},
	  {"node":"old","seq":2,"collected_at":"2026-10-01T11:59:30Z","version":"1.0","height":101,"missed":0},
	  {"node":"old","seq":1,"collected_at":"2026-10-01T11:59:00,1234567891Z","version":"1.0","height":100,"missed":0}
	]`
	records, err := ParseHeartbeats([]byte(input), receive)
	if err == nil {
		t.Fatal("batch with an unrepresentable comma fraction must be rejected at parse")
	}
	if records != nil {
		t.Fatalf("no partial records may be returned, got %v", records)
	}
	if !strings.Contains(err.Error(), "record 3") || !strings.Contains(err.Error(), ",1234567891") {
		t.Errorf("error must locate record 3 and the comma fraction, got %v", err)
	}

	if after, rerr := os.ReadFile(store.nodePath("old")); rerr != nil || string(after) != string(oldBefore) {
		t.Fatalf("existing node file changed: err=%v", rerr)
	}
	if hist, herr := store.History("fresh"); herr != nil || len(hist) != 0 {
		t.Fatalf("new node must have no telemetry: hist=%+v err=%v", hist, herr)
	}
	if _, serr := os.Stat(store.nodePath("fresh")); !os.IsNotExist(serr) {
		t.Errorf("no file may be created for fresh, stat err=%v", serr)
	}
}

// TestStoredUnrepresentableCommaFractionFileIsCorrupt forges a node file
// whose raw JSON carries a comma fraction past nanosecond precision, with a
// checksum matching the truncated interpretation. History and health must
// report corruption naming the record and field, and a submit to that node
// must refuse the whole batch without overwriting the file.
func TestStoredUnrepresentableCommaFractionFileIsCorrupt(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	path := store.nodePath("n1")
	tamperedFile(t, path, `[{"node":"n1","seq":1,"collected_at":"2026-10-01T11:59:00,12345678901Z","version":"1.0","height":100,"missed":0}]`)
	corruptBefore, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	_, err = loadNodeFileFor(store, "n1")
	if err == nil || !IsCorrupt(err) {
		t.Fatalf("unrepresentable stored comma fraction must be corruption, got %v", err)
	}
	msg := err.Error()
	for _, want := range []string{"record 1", "collected_at", ",12345678901", "nanosecond"} {
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

	good := hb("n1", 2, testBase.Add(-time.Minute), "1.0", 101, 0)
	if _, _, err := store.Submit([]Heartbeat{good}, testBase); err == nil || !IsCorrupt(err) {
		t.Fatalf("submit to a corrupt node must be refused as corruption, got %v", err)
	}
	if after, rerr := os.ReadFile(path); rerr != nil || string(after) != string(corruptBefore) {
		t.Fatalf("corrupt file was overwritten by the refused submit: err=%v", rerr)
	}
}

// TestStoredExactCommaFractionFileReads proves the legal comma forms on disk
// stay readable: a comma fraction of at most nine digits, and a longer one
// whose digits past the ninth are all zero (denoting the same instant as a
// dot respelling), including inside a batch alongside dot records.
func TestStoredExactCommaFractionFileReads(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name string
		json string
		want int
	}{
		{"nine comma digits", `[{"node":"n1","seq":1,"collected_at":"2026-10-01T11:59:00,123456789Z","version":"1.0","height":100,"missed":0}]`, 123456789},
		{"comma trailing zero", `[{"node":"n1","seq":1,"collected_at":"2026-10-01T11:59:00,1234567890Z","version":"1.0","height":100,"missed":0}]`, 123456789},
		{"comma extra trailing zeros equal dot instant", `[{"node":"n1","seq":1,"collected_at":"2026-10-01T11:59:00,123456789000Z","version":"1.0","height":100,"missed":0}]`, 123456789},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := store.nodePath("n1")
			tamperedFile(t, path, tc.json)
			nf, err := loadNodeFileFor(store, "n1")
			if err != nil {
				t.Fatalf("exact comma fraction must stay readable, got %v", err)
			}
			if len(nf.Records) != 1 || nf.Records[0].CollectedAt.Nanosecond() != tc.want {
				t.Fatalf("stored instant changed: %+v", nf.Records)
			}
			hist, err := store.History("n1")
			if err != nil {
				t.Fatal(err)
			}
			if len(hist) != 1 || hist[0].CollectedAt.Nanosecond() != tc.want {
				t.Errorf("history lost the nanosecond fraction: %+v", hist)
			}
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
		})
	}
}
