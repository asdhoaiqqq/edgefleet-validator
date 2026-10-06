package edgefleet

import (
	"os"
	"strings"
	"testing"
	"time"
)

// TestParseCollectedAtOffsetBounds pins the literal timezone-offset rule at
// the JSON boundary: hours must be 00..23 and minutes 00..59 exactly as
// written. Go's time.Parse accepts +24:00 as a raw 86400-second zone (which
// cannot be marshaled back) and folds +00:60/-23:60 into the next hour; both
// must be reported with the offending suffix rather than accepted.
func TestParseCollectedAtOffsetBounds(t *testing.T) {
	cases := []struct {
		name  string
		value string
		want  string // text the error must contain
	}{
		{"plus 24 hours", "2026-10-01T11:59:00+24:00", "+24:00"},
		{"minus 24 hours", "2026-10-01T11:59:00-24:00", "-24:00"},
		{"minute 60 plus", "2026-10-01T11:59:00+00:60", "+00:60"},
		{"minute 60 folded minus", "2026-10-01T11:59:00-23:60", "-23:60"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			input := `[{"node":"n1","seq":1,"collected_at":"` + tc.value + `","version":"1.0","height":1,"missed":0}]`
			_, err := ParseHeartbeats([]byte(input), testBase)
			if err == nil {
				t.Fatalf("%s must be rejected", tc.value)
			}
			msg := err.Error()
			for _, want := range []string{"record 1", "collected_at", tc.want} {
				if !strings.Contains(msg, want) {
					t.Errorf("error %q must contain %q", msg, want)
				}
			}
		})
	}
}

// TestParseCollectedAtOffsetBoundsAccepted pins the legal complements:
// every offset expressible as sign + two-digit hour 00..23 + minute 00..59
// is accepted (subject to the receive-time rule), including Z, -00:00 and
// the largest magnitude offset +23:59.
func TestParseCollectedAtOffsetBoundsAccepted(t *testing.T) {
	// 11:59 at -23:59 is the next day in UTC, so pick a wall-clock date that
	// still lands before testBase (2026-09-30T12:00:00-23:59 == 11:59Z).
	cases := []string{
		"2026-10-01T11:59:00Z",
		"2026-10-01T11:59:00-00:00",
		"2026-10-01T11:59:00+00:00",
		"2026-10-01T11:59:00+23:59",
		"2026-09-30T12:00:00-23:59",
		"2026-10-01T11:59:00+08:00",
	}
	for _, value := range cases {
		input := `[{"node":"n1","seq":1,"collected_at":"` + value + `","version":"1.0","height":1,"missed":0}]`
		if _, err := ParseHeartbeats([]byte(input), testBase); err != nil {
			t.Errorf("legal offset %q rejected: %v", value, err)
		}
	}
}

// TestParseCollectedAtBadOffsetRejectsWholeBatch exercises the all-or-nothing
// contract for an offset defect that previously surfaced only at save time:
// a batch whose first record is valid and whose second carries +24:00 must
// fail parsing before the store is touched, and the error locates record 2
// and names collected_at.
func TestParseCollectedAtBadOffsetRejectsWholeBatch(t *testing.T) {
	input := `[
		{"node":"new-a","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":1,"missed":0},
		{"node":"new-b","seq":1,"collected_at":"2026-10-01T11:59:00+24:00","version":"1.0","height":1,"missed":0},
		{"node":"new-a","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":1,"missed":0}
	]`
	records, err := ParseHeartbeats([]byte(input), testBase)
	if err == nil {
		t.Fatalf("batch with +24:00 must be rejected")
	}
	if records != nil {
		t.Errorf("rejected parse must return no records, got %d", len(records))
	}
	msg := err.Error()
	for _, want := range []string{"record 2", "collected_at", "+24:00"} {
		if !strings.Contains(msg, want) {
			t.Errorf("error %q must contain %q", msg, want)
		}
	}
}

// TestValidateCollectedAtRepresentability covers the direct Go submit entry,
// where collected_at arrives as a time.Time rather than JSON text. Values the
// save format cannot preserve — years outside 0000..9999, offsets beyond
// ±23:59 and offsets carrying seconds — must be rejected with a collected_at
// message identifying the specific problem.
func TestValidateCollectedAtRepresentability(t *testing.T) {
	receive := testBase
	good := func(collected time.Time) Heartbeat {
		return hb("n1", 1, collected, "1.0", 1, 0)
	}
	cases := []struct {
		name      string
		collected time.Time
		want      string
	}{
		{"year 10000", time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC), "10000"},
		{"year -1", time.Date(-1, 1, 1, 0, 0, 0, 0, time.UTC), "-1"},
		{"offset +24:00", time.Date(2026, 10, 1, 11, 59, 0, 0, time.FixedZone("+24:00", 24*3600)), "+24:00"},
		{"offset -24:00", time.Date(2026, 9, 30, 12, 0, 0, 0, time.FixedZone("-24:00", -24*3600)), "-24:00"},
		{"offset with seconds", time.Date(2026, 10, 1, 11, 59, 0, 0, time.FixedZone("+08:00:30", 8*3600+30)), "second"},
		{"offset with negative seconds", time.Date(2026, 10, 1, 11, 59, 0, 0, time.FixedZone("-08:00:30", -(8*3600+30))), "second"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateHeartbeat(good(tc.collected), receive)
			if err == nil {
				t.Fatalf("%s must be rejected", tc.name)
			}
			msg := err.Error()
			if !strings.Contains(msg, "collected_at") {
				t.Errorf("error %q must name collected_at", msg)
			}
			if !strings.Contains(msg, tc.want) {
				t.Errorf("error %q must contain %q", msg, tc.want)
			}
		})
	}

	// Legal representable boundaries pass the value rule itself (the
	// receive-time comparison is a separate, submit-only rule).
	for _, c := range []time.Time{
		time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC),
		time.Date(2026, 10, 1, 11, 59, 0, 0, time.FixedZone("+23:59", 23*3600+59*60)),
		time.Date(2026, 9, 30, 12, 0, 0, 0, time.FixedZone("-23:59", -(23*3600+59*60))),
	} {
		if e := checkCollectedAtValue(c); e != nil {
			t.Errorf("representable boundary %s rejected: %v", c.Format(time.RFC3339Nano), e)
		}
	}
	// And collected == receive remains allowed end to end.
	if err := ValidateHeartbeat(good(receive), receive); err != nil {
		t.Errorf("collected_at == receive_time should be allowed: %v", err)
	}
}

// TestSubmitRejectsUnrepresentableCollectedAtAtomically is the store-level
// guarantee: one record with a year, offset-magnitude or offset-seconds
// problem fails the whole batch before any file is written — other nodes'
// new records and in-batch duplicates produce no saves, counts are both zero,
// and the error names node and seq so a direct caller can locate the record.
func TestSubmitRejectsUnrepresentableCollectedAtAtomically(t *testing.T) {
	cases := []struct {
		name      string
		bad       time.Time
		errMarker string
	}{
		{"year too large", time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC), "10000"},
		{"year negative", time.Date(-1, 1, 1, 0, 0, 0, 0, time.UTC), "-1"},
		{"offset too large", time.Date(2026, 10, 1, 11, 59, 0, 0, time.FixedZone("+24:00", 24*3600)), "+24:00"},
		{"offset seconds", time.Date(2026, 10, 1, 11, 59, 0, 0, time.FixedZone("+08:00:30", 8*3600+30)), "second"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store, err := OpenStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			receive := testBase
			batch := []Heartbeat{
				hb("new-a", 1, receive.Add(-time.Minute), "1.0", 1, 0),
				hb("bad-node", 7, tc.bad, "1.0", 1, 0),
				hb("new-a", 1, receive.Add(-time.Minute), "1.0", 1, 0), // in-batch duplicate
			}
			newC, dupC, err := store.Submit(batch, receive)
			if err == nil {
				t.Fatalf("batch must be rejected")
			}
			if newC != 0 || dupC != 0 {
				t.Errorf("rejected batch counts: new=%d dup=%d, want 0/0", newC, dupC)
			}
			msg := err.Error()
			for _, want := range []string{`node "bad-node"`, "seq 7", "collected_at", tc.errMarker} {
				if !strings.Contains(msg, want) {
					t.Errorf("error %q must contain %q", msg, want)
				}
			}
			for _, node := range []string{"new-a", "bad-node"} {
				hist, herr := store.History(node)
				if herr != nil {
					t.Fatal(herr)
				}
				if len(hist) != 0 {
					t.Errorf("node %s got records despite rejected batch: %+v", node, hist)
				}
				r, herr := store.Health(node, receive, "1.0", 0)
				if herr != nil {
					t.Fatal(herr)
				}
				if r.Status != "notelemetry" {
					t.Errorf("node %s status=%s, want notelemetry", node, r.Status)
				}
			}
		})
	}
}

// TestSubmitUnrepresentableCollectedAtLeavesHistoryUntouched covers a batch
// rejected for invalid time data when one node already has history: the
// existing node's file is byte-for-byte unchanged, the new node gets no file,
// and a later valid submit still works with the original counts.
func TestSubmitUnrepresentableCollectedAtLeavesHistoryUntouched(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase
	existing := []Heartbeat{
		hb("old", 1, receive.Add(-3*time.Minute), "1.0", 10, 0),
		hb("old", 2, receive.Add(-2*time.Minute), "1.0", 11, 0),
	}
	if n, d, err := store.Submit(existing, receive); err != nil || n != 2 || d != 0 {
		t.Fatalf("seed submit: n=%d d=%d err=%v", n, d, err)
	}
	oldPath := store.nodePath("old")
	before, err := os.ReadFile(oldPath)
	if err != nil {
		t.Fatal(err)
	}

	bad := []Heartbeat{
		hb("old", 3, receive.Add(-time.Minute), "1.0", 12, 0),
		hb("brand-new", 1, time.Date(2026, 10, 1, 11, 59, 0, 0, time.FixedZone("+08:00:30", 8*3600+30)), "1.0", 1, 0),
	}
	n, d, err := store.Submit(bad, receive)
	if err == nil {
		t.Fatalf("seconds-offset batch must be rejected")
	}
	if n != 0 || d != 0 {
		t.Errorf("rejected batch counts new=%d dup=%d, want 0/0", n, d)
	}
	after, err := os.ReadFile(oldPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Errorf("existing node file changed:\nbefore=%s\nafter=%s", before, after)
	}
	if hist, err := store.History("old"); err != nil || len(hist) != 2 {
		t.Errorf("old history = %v (err=%v), want 2 untouched records", hist, err)
	}
	if r, err := store.Health("brand-new", receive, "1.0", 0); err != nil || r.Status != "notelemetry" {
		t.Errorf("brand-new health=%v err=%v, want notelemetry", r, err)
	}

	// A subsequent valid batch behaves exactly as if the rejected one never
	// happened: the old node's seq 3 counts as one new record.
	n, d, err = store.Submit([]Heartbeat{hb("old", 3, receive.Add(-time.Minute), "1.0", 12, 0)}, receive)
	if err != nil {
		t.Fatalf("follow-up valid submit failed: %v", err)
	}
	if n != 1 || d != 0 {
		t.Errorf("follow-up submit new=%d dup=%d, want 1/0", n, d)
	}
}

// TestSubmitNanosecondCollectedAtPreserved confirms the pre-existing
// nanosecond semantics survive the stricter rules: fractional seconds carry
// no offset problem, save through the JSON format exactly, and the same
// instant in another zone is a duplicate.
func TestSubmitNanosecondCollectedAtPreserved(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase.Add(time.Second)
	withNanos := time.Date(2026, 10, 1, 12, 0, 0, 500000001, time.UTC)
	if n, d, err := store.Submit([]Heartbeat{hb("n1", 1, withNanos, "1.0", 1, 0)}, receive); err != nil {
		t.Fatalf("nanosecond submit failed: %v", err)
	} else if n != 1 || d != 0 {
		t.Errorf("nanosecond submit counts new=%d dup=%d, want 1/0", n, d)
	}
	hist, err := store.History("n1")
	if err != nil {
		t.Fatal(err)
	}
	if len(hist) != 1 || !hist[0].CollectedAt.Equal(withNanos) || hist[0].CollectedAt.Nanosecond() != 500000001 {
		t.Errorf("nanoseconds not preserved: %+v", hist)
	}
	// Same instant in +08:00 is still one heartbeat (duplicate), even though
	// the wall-clock fields differ.
	otherZone := withNanos.In(time.FixedZone("+08", 8*3600))
	if n, d, err := store.Submit([]Heartbeat{hb("n1", 1, otherZone, "1.0", 1, 0)}, receive); err != nil {
		t.Fatalf("cross-zone resubmit failed: %v", err)
	} else if n != 0 || d != 1 {
		t.Errorf("cross-zone same instant new=%d dup=%d, want 0/1", n, d)
	}
}
