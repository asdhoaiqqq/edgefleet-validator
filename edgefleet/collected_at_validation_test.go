package edgefleet

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// These tests pin the collected_at acceptance window at every boundary:
//
//   - JSON text must carry a numeric timezone offset whose own hour field is
//     00..23 and minute field 00..59; the standard library folds +00:60 to
//     +01:00 and accepts +24:00, both of which must be refused here.
//   - A Heartbeat built directly in Go must additionally lie in the four-digit
//     year range 0000..9999 and carry a whole-minute offset within ±23:59,
//     the values the on-disk RFC3339 form can write back exactly.
//
// Any such failure rejects the whole batch before the first file is written.

func TestParseHeartbeatsRejectsOutOfRangeOffsets(t *testing.T) {
	cases := []struct {
		name    string
		stamp   string
		offset  string // the exact offending offset substring the message must name
		wantMsg string // text the specific error must carry besides collected_at
	}{
		{"plus 24 hours", "2026-10-01T11:59:00+24:00", "+24:00", "00-23"},
		{"minus 24 hours", "2026-10-01T11:59:00-24:00", "-24:00", "00-23"},
		{"60 offset minutes folds to one hour", "2026-10-01T11:59:00+00:60", "+00:60", "00-59"},
		{"23 hours 60 minutes folds to 24 hours", "2026-10-01T11:59:00+23:60", "+23:60", "00-59"},
	}
	// A receive time after every candidate so the timing rule never masks the
	// offset rule.
	receive := time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			input := `[{"node":"n1","seq":1,"collected_at":"` + tc.stamp + `","version":"1.0","height":1,"missed":0}]`
			_, err := ParseHeartbeats([]byte(input), receive)
			if err == nil {
				t.Fatalf("offset %q must be rejected, got nil", tc.stamp)
			}
			msg := err.Error()
			for _, want := range []string{"record 1", "collected_at", tc.offset, tc.wantMsg} {
				if !strings.Contains(msg, want) {
					t.Errorf("error %q must mention %q", msg, want)
				}
			}
			// The fold must not be accepted silently as another offset.
			if strings.Contains(msg, "later than receive time") {
				t.Errorf("out-of-range offset must fail as an offset error, not a timing error: %v", err)
			}
		})
	}
}

func TestParseHeartbeatsAcceptsOffsetAndYearBoundaries(t *testing.T) {
	// receive must be at or after the candidate instant; a far-future instant
	// (year 9999) is its own receive, and the -23:59 wall clock lands nearly a
	// day later in UTC than its literal text.
	cases := []struct {
		name    string
		stamp   string
		receive time.Time
	}{
		{"max positive offset", "2026-10-01T11:59:00+23:59", testBase.Add(48 * time.Hour)},
		{"max negative offset", "2026-10-01T11:59:00-23:59", testBase.Add(48 * time.Hour)},
		{"zero offset zulu", "2026-10-01T11:59:00Z", testBase},
		{"negative zero offset is zulu", "2026-10-01T11:59:00-00:00", testBase},
		{"nanosecond fraction kept", "2026-10-01T11:59:00.123456789+08:00", testBase},
		{"year 0000", "0000-01-01T00:00:00Z", time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"year 9999", "9999-12-31T23:59:59Z", time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			input := `[{"node":"n1","seq":1,"collected_at":"` + tc.stamp + `","version":"1.0","height":1,"missed":0}]`
			records, err := ParseHeartbeats([]byte(input), tc.receive)
			if err != nil {
				t.Fatalf("boundary value %q must be accepted, got %v", tc.stamp, err)
			}
			want, perr := time.Parse(time.RFC3339, tc.stamp)
			if perr != nil {
				t.Fatal(perr)
			}
			if !records[0].CollectedAt.Equal(want) {
				t.Errorf("decoded instant %v, want %v", records[0].CollectedAt, want)
			}
		})
	}
}

func TestValidateHeartbeatRejectsUnwritableTimes(t *testing.T) {
	receive := testBase
	cases := []struct {
		name      string
		collected time.Time
		want      string
	}{
		{"year above 9999", time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC), "10000"},
		{"year below 0000", time.Date(-1, 1, 1, 0, 0, 0, 0, time.UTC), "year"},
		{"positive 24 hour offset", time.Date(2026, 10, 1, 11, 59, 0, 0, time.FixedZone("x", 24*3600)), "+24:00"},
		{"negative 24 hour offset", time.Date(2026, 10, 1, 11, 59, 0, 0, time.FixedZone("x", -24*3600)), "-24:00"},
		{"positive sub-minute offset", time.Date(2026, 10, 1, 11, 59, 0, 0, time.FixedZone("x", 8*3600+45)), "+08:00:45"},
		{"negative sub-minute offset", time.Date(2026, 10, 1, 11, 59, 0, 0, time.FixedZone("x", -(8*3600+45))), "-08:00:45"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateHeartbeat(hb("n1", 1, tc.collected, "1.0", 1, 0), receive)
			if err == nil {
				t.Fatalf("unwritable time must be rejected, got nil")
			}
			if !strings.Contains(err.Error(), "collected_at") || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q must mention collected_at and %q", err, tc.want)
			}
		})
	}

	// The boundary values are accepted directly too.
	accepted := []time.Time{
		time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC),
		time.Date(2026, 10, 1, 11, 59, 0, 0, time.FixedZone("x", 23*3600+59*60)),
		time.Date(2026, 10, 1, 11, 59, 0, 0, time.FixedZone("x", -(23*3600+59*60))),
	}
	for i, at := range accepted {
		receive := at
		if err := ValidateHeartbeat(hb("n1", 1, at, "1.0", 1, 0), receive); err != nil {
			t.Errorf("accepted case %d rejected: %v", i, err)
		}
	}
}

// TestSubmitRejectsUnwritableTimeWholeBatch mirrors the reported regression on
// the direct Go entry point: when one record in a multi-node batch cannot be
// saved exactly, the other nodes' brand-new records must not be saved first,
// an existing node's history must stay byte-for-byte unchanged, and the caller
// must get an error naming the node and seq with both counters at zero.
func TestSubmitRejectsUnwritableTimeWholeBatch(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase

	// "old" already has telemetry; capture its file before the failed batch.
	existing := hb("old", 1, receive.Add(-2*time.Minute), "1.0", 100, 0)
	if _, _, err := store.Submit([]Heartbeat{existing}, receive); err != nil {
		t.Fatal(err)
	}
	oldBefore, err := os.ReadFile(store.nodePath("old"))
	if err != nil {
		t.Fatal(err)
	}

	batch := []Heartbeat{
		// Brand-new node record that must not survive.
		hb("fresh", 1, receive.Add(-time.Minute), "1.0", 1, 0),
		// Appends to an existing node; its file must not be rewritten.
		hb("old", 2, receive.Add(-time.Minute), "1.0", 101, 0),
		// Duplicate of an existing record: must not be counted either.
		existing,
		// The offending record (year 10000), identified by node and seq.
		hb("future", 1, time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC), "1.0", 1, 0),
	}
	newC, dupC, err := store.Submit(batch, receive)
	if err == nil {
		t.Fatal("batch with an unsavable collected_at must be rejected")
	}
	if newC != 0 || dupC != 0 {
		t.Errorf("rejected batch counters must be 0/0, got new=%d dup=%d", newC, dupC)
	}
	if !strings.Contains(err.Error(), `"future"`) || !strings.Contains(err.Error(), "seq 1") {
		t.Errorf("error must name node and seq, got %v", err)
	}

	// The existing node file is byte-for-byte unchanged.
	if after, rerr := os.ReadFile(store.nodePath("old")); rerr != nil || string(after) != string(oldBefore) {
		t.Fatalf("existing node file changed: err=%v", rerr)
	}
	// No telemetry for either new node.
	for _, node := range []string{"fresh", "future"} {
		hist, herr := store.History(node)
		if herr != nil {
			t.Fatal(herr)
		}
		if len(hist) != 0 {
			t.Errorf("node %s got records despite rejected batch: %+v", node, hist)
		}
		if _, serr := os.Stat(store.nodePath(node)); !os.IsNotExist(serr) {
			t.Errorf("no file may be created for %q, stat err=%v", node, serr)
		}
	}
	// The existing node is still queryable and unchanged.
	r, err := store.Health("old", receive, "1.0", 0)
	if err != nil {
		t.Fatal(err)
	}
	if r.Seq != 1 || r.Height != 100 {
		t.Errorf("existing node changed after rejected batch: %+v", r)
	}
}

// TestWritableBoundaryTimesRoundTrip proves the accepted edge values are not
// just validated but persisted exactly: after Submit, History and Health read
// back the same instant, including nanoseconds, so a successful save always
// reflects the moment the node actually reported.
func TestWritableBoundaryTimesRoundTrip(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	zone := time.FixedZone("x", 23*3600+59*60)
	collected := time.Date(2026, 10, 1, 11, 59, 0, 123456789, zone)
	r := hb("edge", 1, collected, "1.0", 7, 0)
	if _, _, err := store.Submit([]Heartbeat{r}, collected); err != nil {
		t.Fatalf("boundary record must save: %v", err)
	}
	hist, err := store.History("edge")
	if err != nil {
		t.Fatal(err)
	}
	if len(hist) != 1 || !hist[0].CollectedAt.Equal(collected) {
		t.Fatalf("saved instant changed: %+v", hist)
	}
	if got := hist[0].CollectedAt.Location(); got == nil {
		t.Fatal("saved location lost")
	}
	if _, off := hist[0].CollectedAt.Zone(); off != 23*3600+59*60 {
		t.Errorf("saved offset = %d, want %d", off, 23*3600+59*60)
	}
	// Re-read via the strict file decoder path by reopening the store.
	reopened, err := OpenStore(store.Dir())
	if err != nil {
		t.Fatal(err)
	}
	hist2, err := reopened.History("edge")
	if err != nil {
		t.Fatal(err)
	}
	if !hist2[0].Equal(r) {
		t.Errorf("round trip mismatch: %+v vs %+v", hist2[0], r)
	}
}

// TestParseHeartbeatsTextOffsetRejectsWholeBatchJSON pins the JSON-side
// atomicity: a leading valid record must not survive when a later record
// carries an out-of-range offset (ParseHeartbeats returns no partial records).
func TestParseHeartbeatsTextOffsetRejectsWholeBatchJSON(t *testing.T) {
	input := `[
	  {"node":"n1","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":1,"missed":0},
	  {"node":"n2","seq":1,"collected_at":"2026-10-01T11:59:00+00:60","version":"1.0","height":1,"missed":0}
	]`
	records, err := ParseHeartbeats([]byte(input), testBase.Add(48*time.Hour))
	if err == nil {
		t.Fatal("batch with folded offset must be rejected")
	}
	if records != nil {
		t.Errorf("ParseHeartbeats must return no partial records, got %v", records)
	}
	if !strings.Contains(err.Error(), "record 2") || !strings.Contains(err.Error(), "collected_at") {
		t.Errorf("error must locate record 2 and collected_at, got %v", err)
	}
}

// TestStoredFoldedOffsetFileIsCorrupt forges a node file whose raw JSON
// contains an out-of-range offset that decodes to a folded instant, with a
// checksum matching the folded interpretation. The read path must reject it as
// corruption naming the record and field, exactly like any other invalid
// stored value.
func TestStoredFoldedOffsetFileIsCorrupt(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	path := store.nodePath("n1")
	tamperedFile(t, path, `[{"node":"n1","seq":1,"collected_at":"2026-10-01T11:59:00+00:60","version":"1.0","height":100,"missed":0}]`)

	_, err = loadNodeFileFor(store, "n1")
	if err == nil || !IsCorrupt(err) {
		t.Fatalf("folded-offset stored file must be corruption, got %v", err)
	}
	msg := err.Error()
	if !strings.Contains(msg, filepath.Base(path)) || !strings.Contains(msg, "record 1") ||
		!strings.Contains(msg, "collected_at") || !strings.Contains(msg, "00-59") {
		t.Errorf("corruption error must name file, record 1 and the offset rule, got %v", err)
	}
	if _, err := store.Health("n1", testBase.Add(48*time.Hour), "1.0", 0); err == nil || !IsCorrupt(err) {
		t.Errorf("health must refuse folded-offset data, got %v", err)
	}
	if _, err := store.History("n1"); err == nil || !IsCorrupt(err) {
		t.Errorf("history must refuse folded-offset data, got %v", err)
	}
}

// TestStoredOutOfRangeHourOffsetFileIsCorrupt covers the hour bound on disk:
// a hand-written file carrying +24:00 (which time.Parse accepts) has to be
// rejected while reading, not folded or accepted.
func TestStoredOutOfRangeHourOffsetFileIsCorrupt(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	path := store.nodePath("n1")
	tamperedFile(t, path, `[{"node":"n1","seq":1,"collected_at":"2026-10-01T11:59:00+24:00","version":"1.0","height":100,"missed":0}]`)

	_, err = loadNodeFileFor(store, "n1")
	if err == nil || !IsCorrupt(err) {
		t.Fatalf("+24:00 stored file must be corruption, got %v", err)
	}
	if !strings.Contains(err.Error(), "record 1") || !strings.Contains(err.Error(), "collected_at") {
		t.Errorf("corruption error must name record 1 and collected_at, got %v", err)
	}
}

// TestParseHeartbeatsRejectsSubNanosecondFraction pins the reported
// regression: time.Parse silently truncates fractional seconds beyond the
// ninth digit, so ".1234567891" and ".1234567892" would both save as the
// instant ".123456789" and two genuinely different collection moments would
// merge into one record. Any non-zero digit past the ninth must be rejected
// — never truncated or rounded — with an error naming the record position,
// collected_at and the precision problem.
func TestParseHeartbeatsRejectsSubNanosecondFraction(t *testing.T) {
	receive := testBase
	cases := []struct {
		name  string
		stamp string
	}{
		{"tenth digit non-zero", "2026-10-01T11:59:00.1234567891Z"},
		{"tenth digit non-zero with offset", "2026-10-01T11:59:00.1234567891+08:00"},
		{"eleventh digit non-zero after zero tenth", "2026-10-01T11:59:00.12345678901Z"},
		{"trailing non-zero far out", "2026-10-01T11:59:00.000000000000001Z"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			input := `[{"node":"n1","seq":1,"collected_at":"` + tc.stamp + `","version":"1.0","height":1,"missed":0}]`
			records, err := ParseHeartbeats([]byte(input), receive)
			if err == nil {
				t.Fatalf("sub-nanosecond fraction %q must be rejected, got nil", tc.stamp)
			}
			if records != nil {
				t.Errorf("no partial records may be returned, got %v", records)
			}
			msg := err.Error()
			for _, want := range []string{"record 1", "collected_at", "fractional seconds", "nanosecond"} {
				if !strings.Contains(msg, want) {
					t.Errorf("error %q must mention %q", msg, want)
				}
			}
		})
	}

	// A JSON-escaped spelling of the same text decodes to the same string and
	// must get the identical verdict: the tenth digit written as the
	// escape '1' still carries the unrepresentable precision.
	escaped := `[{"node":"n1","seq":1,"collected_at":"2026-10-01T11:59:00.123456789\u0031Z","version":"1.0","height":1,"missed":0}]`
	if _, err := ParseHeartbeats([]byte(escaped), receive); err == nil {
		t.Errorf("escaped spelling of the same unrepresentable fraction must be rejected identically")
	}
}

// TestParseHeartbeatsAcceptsExactNanosecondFractions pins the legal forms: no
// fraction, up to nine digits, and longer fractions whose digits past the
// ninth are all zero — ".1234567890" is exactly ".123456789" and decodes to
// the same instant.
func TestParseHeartbeatsAcceptsExactNanosecondFractions(t *testing.T) {
	cases := []struct {
		name  string
		stamp string
		want  string // canonical instant it must decode to
	}{
		{"no fraction", "2026-10-01T11:59:00Z", "2026-10-01T11:59:00Z"},
		{"one digit", "2026-10-01T11:59:00.1Z", "2026-10-01T11:59:00.1Z"},
		{"nine digits", "2026-10-01T11:59:00.123456789Z", "2026-10-01T11:59:00.123456789Z"},
		{"ten digits trailing zero", "2026-10-01T11:59:00.1234567890Z", "2026-10-01T11:59:00.123456789Z"},
		{"many trailing zeros", "2026-10-01T11:59:00.12345678900000Z", "2026-10-01T11:59:00.123456789Z"},
		{"trailing zeros with offset", "2026-10-01T11:59:00.5000000000+08:00", "2026-10-01T11:59:00.5+08:00"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			input := `[{"node":"n1","seq":1,"collected_at":"` + tc.stamp + `","version":"1.0","height":1,"missed":0}]`
			records, err := ParseHeartbeats([]byte(input), testBase)
			if err != nil {
				t.Fatalf("exactly representable fraction %q must be accepted, got %v", tc.stamp, err)
			}
			want, perr := time.Parse(time.RFC3339, tc.want)
			if perr != nil {
				t.Fatal(perr)
			}
			if !records[0].CollectedAt.Equal(want) {
				t.Errorf("%q decoded to %v, want %v", tc.stamp, records[0].CollectedAt, want)
			}
		})
	}
}

// TestSubmitSubNanosecondFractionRejectsWholeBatch mirrors the whole-batch
// guarantee for the precision rule: a record whose collected_at cannot be
// saved exactly fails the parse of the entire batch — no partial records are
// returned — even when its truncated form would exactly match a record
// already saved for that node and seq (it must be refused as invalid input,
// never counted as a duplicate of the saved one).
func TestSubmitSubNanosecondFractionRejectsWholeBatch(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase

	// "old" already has a record collected at exactly 11:58:00.123456789.
	saved := hb("old", 1, time.Date(2026, 10, 1, 11, 58, 0, 123456789, time.UTC), "1.0", 100, 0)
	if _, _, err := store.Submit([]Heartbeat{saved}, receive); err != nil {
		t.Fatal(err)
	}
	oldBefore, err := os.ReadFile(store.nodePath("old"))
	if err != nil {
		t.Fatal(err)
	}

	// The offending record's truncated form would equal the saved instant of
	// "old" seq 1; it must still fail as invalid input, and the legal record
	// ahead of it must not survive either.
	bad := `{"node":"old","seq":1,"collected_at":"2026-10-01T11:58:00.1234567891Z","version":"1.0","height":100,"missed":0}`
	good := `{"node":"fresh","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":1,"missed":0}`
	records, err := ParseHeartbeats([]byte("["+good+","+bad+"]"), receive)
	if err == nil {
		t.Fatal("batch with a sub-nanosecond fraction must be rejected at parse")
	}
	if records != nil {
		t.Errorf("no partial records may be returned, got %v", records)
	}
	msg := err.Error()
	for _, want := range []string{"record 2", "collected_at", "fractional seconds"} {
		if !strings.Contains(msg, want) {
			t.Errorf("error %q must mention %q", msg, want)
		}
	}

	// Nothing was submitted, so the store is exactly as before.
	if after, rerr := os.ReadFile(store.nodePath("old")); rerr != nil || string(after) != string(oldBefore) {
		t.Fatalf("existing node file changed: err=%v", rerr)
	}
	if _, serr := os.Stat(store.nodePath("fresh")); !os.IsNotExist(serr) {
		t.Errorf("no file may be created for fresh, stat err=%v", serr)
	}
}

// TestStoredSubNanosecondFractionFileIsCorrupt forges a node file whose raw
// JSON carries a fraction time.Parse would truncate, with a checksum matching
// the truncated interpretation (exactly what a lenient reader reconstructs).
// Every query must report corruption naming the record and the collected_at
// precision problem, and a submit touching the node must refuse the whole
// batch without overwriting the file.
func TestStoredSubNanosecondFractionFileIsCorrupt(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase
	path := store.nodePath("n1")
	tamperedFile(t, path, `[{"node":"n1","seq":1,"collected_at":"2026-10-01T11:59:00.1234567891Z","version":"1.0","height":100,"missed":0}]`)
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	_, err = loadNodeFileFor(store, "n1")
	if err == nil || !IsCorrupt(err) {
		t.Fatalf("sub-nanosecond stored fraction must be corruption, got %v", err)
	}
	msg := err.Error()
	for _, want := range []string{"record 1", "collected_at", "fractional seconds", "nanosecond"} {
		if !strings.Contains(msg, want) {
			t.Errorf("corruption error must mention %q, got %v", want, err)
		}
	}
	if _, err := store.Health("n1", receive, "1.0", 0); err == nil || !IsCorrupt(err) {
		t.Errorf("health must refuse the unrepresentable instant, got %v", err)
	}
	if _, err := store.History("n1"); err == nil || !IsCorrupt(err) {
		t.Errorf("history must refuse the unrepresentable instant, got %v", err)
	}

	// Submitting to the corrupt node refuses the whole batch and leaves the
	// file exactly as found.
	_, _, err = store.Submit([]Heartbeat{hb("n1", 2, receive.Add(-time.Second), "1.0", 101, 0)}, receive)
	if err == nil || !IsCorrupt(err) {
		t.Fatalf("submit touching the corrupt node must be refused, got %v", err)
	}
	if after, rerr := os.ReadFile(path); rerr != nil || string(after) != string(before) {
		t.Fatalf("corrupt file was overwritten: err=%v", rerr)
	}
}

// TestStoredTrailingZeroFractionFileStaysReadable proves the legal long form
// on disk: a hand-written record whose fraction only adds trailing zeros past
// the ninth digit decodes to the same nanosecond instant and stays readable.
func TestStoredTrailingZeroFractionFileStaysReadable(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	path := store.nodePath("n1")
	collected := time.Date(2026, 10, 1, 11, 59, 0, 123456789, time.UTC)
	tamperedFile(t, path, `[{"node":"n1","seq":1,"collected_at":"2026-10-01T11:59:00.1234567890Z","version":"1.0","height":100,"missed":0}]`)

	hist, err := store.History("n1")
	if err != nil {
		t.Fatalf("trailing-zero fraction must stay readable: %v", err)
	}
	if len(hist) != 1 || !hist[0].CollectedAt.Equal(collected) {
		t.Fatalf("stored instant changed: %+v", hist)
	}
}

// TestSameSeqOneNanosecondApartConflicts pins the instant comparison at full
// nanosecond resolution: two legal records for the same node and seq whose
// collection times differ by a single nanosecond are a content conflict, not
// a duplicate.
func TestSameSeqOneNanosecondApartConflicts(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase
	base := time.Date(2026, 10, 1, 11, 59, 0, 123456789, time.UTC)
	if _, _, err := store.Submit([]Heartbeat{hb("n1", 1, base, "1.0", 100, 0)}, receive); err != nil {
		t.Fatal(err)
	}
	_, _, err = store.Submit([]Heartbeat{hb("n1", 1, base.Add(time.Nanosecond), "1.0", 100, 0)}, receive)
	if err == nil || !strings.Contains(err.Error(), "conflict") {
		t.Fatalf("records one nanosecond apart must conflict, got %v", err)
	}
	hist, herr := store.History("n1")
	if herr != nil {
		t.Fatal(herr)
	}
	if len(hist) != 1 || !hist[0].CollectedAt.Equal(base) {
		t.Errorf("conflict must not overwrite the saved record: %+v", hist)
	}
}
