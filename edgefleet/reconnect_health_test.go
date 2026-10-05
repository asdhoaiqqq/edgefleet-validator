package edgefleet

// Regression coverage for health queries over disconnected-and-replayed
// telemetry. A node may hold heartbeats whose seqs have gaps and whose arrival
// order disagrees with both seq order and collection-time order: a record
// buffered during an outage can arrive late, a smaller seq can arrive after a
// larger one, and the same record can be re-submitted at an even later receive
// time. These tests pin the query contract:
//
//   - "latest telemetry" is strictly the record with the greatest seq, never
//     the most recently received one or the one with the newest collection
//     time; every displayed field (time, version, height, cumulative missed)
//     comes from that one record and fields are never spliced across records;
//   - replaying an older, smaller-seq record at a later receive time neither
//     refreshes the latest collection time nor flips any judgement;
//   - the online boundary is exact at sub-second precision: age == 60s stays
//     online, age == 60s + 1 nanosecond is offline, independently of the
//     whole-second time the terminal happens to print, and independently of
//     the timezone spelling of the instants;
//   - a baseline query obeys the same latest-record selection and online time
//     rule — the baseline only changes the quantity used by the missed-duty
//     alarm;
//   - a query time earlier than the greatest-seq record's collection time is
//     an error for both plain and baseline queries, even when a smaller-seq
//     record had already been collected by then, and no query or query failure
//     rewrites saved heartbeats.

import (
	"os"
	"strings"
	"testing"
	"time"
)

// mustParseRFC3339 parses a fixed test instant, failing the test rather than
// returning the zero time. Fractional seconds and explicit offsets are part of
// the input on purpose.
func mustParseRFC3339(t *testing.T, s string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t.Fatalf("test setup: parse %q: %v", s, err)
	}
	return parsed
}

// reconnectFixture is the disordered-history scenario: the greater-seq
// record was collected two minutes before the query (stale, offline), while
// the smaller-seq record was collected only ten seconds before the query
// (fresh). They also disagree on version, height and cumulative missed.
type reconnectFixture struct {
	query     time.Time
	bigSeq    Heartbeat // seq 10, collected 2 minutes ago
	smallSeq  Heartbeat // seq 5, collected 10 seconds ago
	latestAge time.Duration
	freshAge  time.Duration
}

func newReconnectFixture(t *testing.T) reconnectFixture {
	t.Helper()
	q := testBase
	return reconnectFixture{
		query:     q,
		latestAge: 2 * time.Minute,
		freshAge:  10 * time.Second,
		bigSeq:    hb("recon", 10, q.Add(-2*time.Minute), "1.25.0", 1000, 5),
		smallSeq:  hb("recon", 5, q.Add(-10*time.Second), "1.26.0", 900, 2),
	}
}

// assertHealthUsesBigSeq verifies a health result is derived entirely from the
// seq-10 record: offline because of its two-minute-old collection time, with
// its version, height and cumulative missed. A selector that instead picked
// the fresh seq-5 record (by receive order or newest collection time) would
// report online with version 1.26.0, height 900 and missed 2, and a selector
// that spliced fields would mismatch one of these.
func (f reconnectFixture) assertHealthUsesBigSeq(t *testing.T, r HealthResult) {
	t.Helper()
	if r.Seq != f.bigSeq.Seq {
		t.Errorf("latest seq = %d, want %d (greatest seq, not freshest collection)", r.Seq, f.bigSeq.Seq)
	}
	if r.Status != "offline" {
		t.Errorf("status = %q, want offline: the greatest-seq record is 2 minutes old; the fresh seq-%d record must not make the node online", r.Status, f.smallSeq.Seq)
	}
	if !r.CollectedAt.Equal(f.bigSeq.CollectedAt) {
		t.Errorf("collected_at = %v, want %v (from seq %d, not the newer collection of seq %d)",
			r.CollectedAt, f.bigSeq.CollectedAt, f.bigSeq.Seq, f.smallSeq.Seq)
	}
	if r.Version != f.bigSeq.Version {
		t.Errorf("version = %q, want %q from seq %d; fields must not be spliced", r.Version, f.bigSeq.Version, f.bigSeq.Seq)
	}
	if r.Height != f.bigSeq.Height {
		t.Errorf("height = %d, want %d from seq %d", r.Height, f.bigSeq.Height, f.bigSeq.Seq)
	}
	if r.Missed != f.bigSeq.Missed {
		t.Errorf("cumulative missed = %d, want %d from seq %d", r.Missed, f.bigSeq.Missed, f.bigSeq.Seq)
	}
	joined := strings.Join(r.Findings, ";")
	if !strings.Contains(joined, "offline") {
		t.Errorf("findings %v must contain offline", r.Findings)
	}
	if !strings.Contains(joined, "version skew: 1.25.0 != 1.26.0") {
		t.Errorf("findings %v must judge version skew from seq %d (1.25.0); the fresh seq-%d record's 1.26.0 must not mask it", r.Findings, f.bigSeq.Seq, f.smallSeq.Seq)
	}
}

// TestHealthLatestIsGreatestSeqRegardlessOfArrivalOrder saves the same two
// records in both possible arrival orders — stale big seq first (genuine
// backfill of the fresh small seq), and fresh small seq first (late arrival of
// the stale big seq) — and requires identical health results.
func TestHealthLatestIsGreatestSeqRegardlessOfArrivalOrder(t *testing.T) {
	fx := newReconnectFixture(t)

	results := map[string]HealthResult{}
	for _, order := range []struct {
		name    string
		submit1 func(*Store)
		submit2 func(*Store)
	}{
		{
			name: "stale big seq arrives first, fresh small seq backfilled later",
			// seq 10 (collected 2m ago) is received 90s before the query; the
			// smaller-seq record is not even submittable yet because its
			// collection time is still in the future. It is backfilled at the
			// query instant.
			submit1: func(s *Store) { mustSubmit(t, s, fx.query.Add(-90*time.Second), fx.bigSeq) },
			submit2: func(s *Store) { mustSubmit(t, s, fx.query, fx.smallSeq) },
		},
		{
			name:    "fresh small seq arrives first, stale big seq delivered after",
			submit1: func(s *Store) { mustSubmit(t, s, fx.query, fx.smallSeq) },
			submit2: func(s *Store) { mustSubmit(t, s, fx.query, fx.bigSeq) },
		},
	} {
		t.Run(order.name, func(t *testing.T) {
			store, err := OpenStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			order.submit1(store)
			order.submit2(store)

			// Cumulative missed 5 (seq 10) strictly exceeds tolerance 4; the
			// seq-5 record's count 2 would not. This makes the alarm another
			// witness of which record drives the query.
			r, err := store.Health("recon", fx.query, "1.26.0", 4)
			if err != nil {
				t.Fatal(err)
			}
			fx.assertHealthUsesBigSeq(t, r)
			if !hasFinding(r.Findings, "missed duties above tolerance") {
				t.Errorf("findings %v must alarm on cumulative missed=5 from seq %d", r.Findings, fx.bigSeq.Seq)
			}
			results[order.name] = r

			hist, err := store.History("recon")
			if err != nil {
				t.Fatal(err)
			}
			if len(hist) != 2 || hist[0].Seq != 5 || hist[1].Seq != 10 {
				t.Fatalf("history must keep both gapped seqs ascending: %+v", hist)
			}
			if !hist[1].Equal(fx.bigSeq) || !hist[0].Equal(fx.smallSeq) {
				t.Errorf("stored records altered: %+v / %+v", hist[0], hist[1])
			}
		})
	}

	// Arrival order must leave no trace at all in the judgement.
	a, b := results["stale big seq arrives first, fresh small seq backfilled later"],
		results["fresh small seq arrives first, stale big seq delivered after"]
	if a.Status != b.Status || a.Seq != b.Seq || a.Version != b.Version ||
		a.Height != b.Height || a.Missed != b.Missed || !a.CollectedAt.Equal(b.CollectedAt) ||
		strings.Join(a.Findings, "|") != strings.Join(b.Findings, "|") {
		t.Errorf("health result depends on arrival order:\n%+v\nvs\n%+v", a, b)
	}
}

// TestHealthLateReplayOfSmallerSeqDoesNotRefreshLatest replays the smaller-seq
// record at a strictly later receive time, both as a first-time backfill and
// as a verbatim duplicate, and checks the latest telemetry's collection time
// and the offline judgement never move.
func TestHealthLateReplayOfSmallerSeqDoesNotRefreshLatest(t *testing.T) {
	fx := newReconnectFixture(t)
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	// The stale big-seq record is the only telemetry known at the query time.
	mustSubmit(t, store, fx.query.Add(-90*time.Second), fx.bigSeq)
	before, err := store.Health("recon", fx.query, "1.26.0", 4)
	if err != nil {
		t.Fatal(err)
	}
	fx.assertHealthUsesBigSeq(t, before)

	// One minute later, the node re-connects and backfills the fresh small-seq
	// record for the first time. Its receive time is later, but latest stays
	// seq 10.
	later := fx.query.Add(time.Minute)
	newCount, dupCount, err := store.Submit([]Heartbeat{fx.smallSeq}, later)
	if err != nil {
		t.Fatalf("backfill of older seq must be accepted: %v", err)
	}
	if newCount != 1 || dupCount != 0 {
		t.Errorf("first backfill: new=%d dup=%d, want 1/0", newCount, dupCount)
	}
	after, err := store.Health("recon", later, "1.26.0", 4)
	if err != nil {
		t.Fatal(err)
	}
	fx.assertHealthUsesBigSeq(t, after)
	if !after.CollectedAt.Equal(before.CollectedAt) {
		t.Errorf("backfill refreshed latest collection time: %v -> %v", before.CollectedAt, after.CollectedAt)
	}

	// Re-submitting the very same record at yet another receive time is a
	// duplicate and again changes nothing.
	newCount, dupCount, err = store.Submit([]Heartbeat{fx.smallSeq}, later.Add(time.Minute))
	if err != nil {
		t.Fatalf("duplicate replay failed: %v", err)
	}
	if newCount != 0 || dupCount != 1 {
		t.Errorf("duplicate replay: new=%d dup=%d, want 0/1", newCount, dupCount)
	}
	replayed, err := store.Health("recon", later.Add(time.Minute), "1.26.0", 4)
	if err != nil {
		t.Fatal(err)
	}
	fx.assertHealthUsesBigSeq(t, replayed)
	if !replayed.CollectedAt.Equal(fx.bigSeq.CollectedAt) {
		t.Errorf("duplicate replay moved collection time: %v", replayed.CollectedAt)
	}

	hist, err := store.History("recon")
	if err != nil {
		t.Fatal(err)
	}
	if len(hist) != 2 {
		t.Errorf("history len=%d, want 2 (duplicate not stored)", len(hist))
	}
}

// TestHealthOnlineBoundarySubSecondPrecision pins the exact 60-second edge
// using instants that carry sub-second fractions. Truncating collection or
// query time to whole seconds before comparing, or comparing the printed
// second numbers, would put the 60s+1ns case at exactly 60s and wrongly call
// it online.
func TestHealthOnlineBoundarySubSecondPrecision(t *testing.T) {
	cases := []struct {
		name      string
		collected string
		exact     string // age == 60s, still online
		oneOver   string // age == 60s + 1 nanosecond, offline
	}{
		{
			name:      "half-second fractions",
			collected: "2026-10-01T12:00:00.5Z",
			exact:     "2026-10-01T12:01:00.5Z",
			oneOver:   "2026-10-01T12:01:00.500000001Z",
		},
		{
			name:      "fraction rolls across the whole second",
			collected: "2026-10-01T12:00:00.999999999Z",
			exact:     "2026-10-01T12:01:00.999999999Z",
			oneOver:   "2026-10-01T12:01:01Z",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store, err := OpenStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			collected := mustParseRFC3339(t, tc.collected)
			if _, _, err := store.Submit(
				[]Heartbeat{hb("edge", 1, collected, "1.0", 1, 0)}, collected.Add(2*time.Minute)); err != nil {
				t.Fatal(err)
			}

			// Querying at the exact collection instant is allowed (age 0).
			r, err := store.Health("edge", collected, "1.0", 0)
			if err != nil {
				t.Fatalf("query at collection instant must not error: %v", err)
			}
			if r.Status != "online" {
				t.Errorf("age 0: status=%q, want online", r.Status)
			}

			at := mustParseRFC3339(t, tc.exact)
			if got := at.Sub(collected); got != 60*time.Second {
				t.Fatalf("test setup: exact case age = %v, want 60s", got)
			}
			r, err = store.Health("edge", at, "1.0", 0)
			if err != nil {
				t.Fatalf("offline is a successful query, never an error: %v", err)
			}
			if r.Status != "online" {
				t.Errorf("age exactly 60s: status=%q, want online", r.Status)
			}

			over := mustParseRFC3339(t, tc.oneOver)
			if got := over.Sub(collected); got != 60*time.Second+time.Nanosecond {
				t.Fatalf("test setup: over case age = %v, want 60s+1ns", got)
			}
			r, err = store.Health("edge", over, "1.0", 0)
			if err != nil {
				t.Fatalf("offline query must succeed (exit-zero status): %v", err)
			}
			if r.Status != "offline" {
				t.Errorf("age 60s+1ns: status=%q, want offline; whole-second truncation would misjudge this", r.Status)
			}
			if !hasFinding(r.Findings, "offline") {
				t.Errorf("findings %v must contain the offline notice", r.Findings)
			}
		})
	}
}

// TestHealthOnlineBoundaryTimezoneInvariant states the same boundary pair in
// different timezone spellings and requires the same status and the preserved
// sub-second instant. Online/offline compares instants, not local wall-clock
// text.
func TestHealthOnlineBoundaryTimezoneInvariant(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	collectedZ := mustParseRFC3339(t, "2026-10-01T12:00:00.5Z")
	if _, _, err := store.Submit(
		[]Heartbeat{hb("tz", 1, collectedZ, "1.0", 1, 0)},
		mustParseRFC3339(t, "2026-10-01T12:02:00Z")); err != nil {
		t.Fatal(err)
	}

	// The same instant written with +08:00 is the same heartbeat: duplicate,
	// not a second record.
	sameInstantOffset := hb("tz", 1, mustParseRFC3339(t, "2026-10-01T20:00:00.5+08:00"), "1.0", 1, 0)
	newCount, dupCount, err := store.Submit([]Heartbeat{sameInstantOffset},
		mustParseRFC3339(t, "2026-10-01T20:02:00+08:00"))
	if err != nil {
		t.Fatal(err)
	}
	if newCount != 0 || dupCount != 1 {
		t.Errorf("same instant in another timezone: new=%d dup=%d, want 0/1", newCount, dupCount)
	}

	// Exactly 60s and 60s+1ns, each expressed in two different zones.
	onlineZ := mustParseRFC3339(t, "2026-10-01T12:01:00.5Z")
	onlineOffset := mustParseRFC3339(t, "2026-10-01T20:01:00.5+08:00")
	if !onlineZ.Equal(onlineOffset) {
		t.Fatal("test setup: online instants differ")
	}
	offlineZ := mustParseRFC3339(t, "2026-10-01T12:01:00.500000001Z")
	offlineMinus := mustParseRFC3339(t, "2026-10-01T08:01:00.500000001-04:00")
	if !offlineZ.Equal(offlineMinus) {
		t.Fatal("test setup: offline instants differ")
	}

	for _, q := range []time.Time{onlineZ, onlineOffset} {
		r, err := store.Health("tz", q, "1.0", 0)
		if err != nil {
			t.Fatal(err)
		}
		if r.Status != "online" {
			t.Errorf("query %s: status=%q, want online at exactly 60s", q.Format(time.RFC3339Nano), r.Status)
		}
		if r.CollectedAt.Nanosecond() != 500_000_000 || !r.CollectedAt.Equal(collectedZ) {
			t.Errorf("fractional precision lost: collected=%v (nanos=%d), want %v",
				r.CollectedAt.Format(time.RFC3339Nano), r.CollectedAt.Nanosecond(), collectedZ.Format(time.RFC3339Nano))
		}
	}
	for _, q := range []time.Time{offlineZ, offlineMinus} {
		r, err := store.Health("tz", q, "1.0", 0)
		if err != nil {
			t.Fatal(err)
		}
		if r.Status != "offline" {
			t.Errorf("query %s: status=%q, want offline at 60s+1ns regardless of zone", q.Format(time.RFC3339Nano), r.Status)
		}
		if r.CollectedAt.Nanosecond() != 500_000_000 {
			t.Errorf("fractional precision lost in offline result: nanos=%d", r.CollectedAt.Nanosecond())
		}
	}

	// The fraction survives persistence across a reopen.
	reopened, err := OpenStore(store.Dir())
	if err != nil {
		t.Fatal(err)
	}
	hist, err := reopened.History("tz")
	if err != nil {
		t.Fatal(err)
	}
	if len(hist) != 1 || !hist[0].CollectedAt.Equal(collectedZ) || hist[0].CollectedAt.Nanosecond() != 500_000_000 {
		t.Errorf("fractional collection time not preserved in stored history: %+v", hist)
	}
}

// TestHealthSinceBaselineFollowsSameLatestAndOnlineRules checks that a
// missed-since baseline changes only the missed-duty alarm quantity: the
// latest record is still the greatest seq, status is still set by its
// collection time, version skew is still judged from it, and the first-line
// missed is still its cumulative count.
func TestHealthSinceBaselineFollowsSameLatestAndOnlineRules(t *testing.T) {
	fx := newReconnectFixture(t)
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	mustSubmit(t, store, fx.query.Add(-90*time.Second), fx.bigSeq)
	mustSubmit(t, store, fx.query, fx.smallSeq)

	// Baseline is the fresh seq-5 record. Its version happens to match the
	// expected version and its missed count 2 would look healthy — neither may
	// influence the result: new missed duties are 5-2=3, but status is offline
	// and the skew comes from the stale seq-10 record.
	r, err := store.HealthSince("recon", fx.query, "1.26.0", 2, fx.smallSeq.Seq)
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != "offline" {
		t.Errorf("baseline must not determine online status: got %q, want offline", r.Status)
	}
	if r.Seq != fx.bigSeq.Seq || !r.CollectedAt.Equal(fx.bigSeq.CollectedAt) {
		t.Errorf("latest must stay seq %d, got seq %d at %v", fx.bigSeq.Seq, r.Seq, r.CollectedAt)
	}
	if r.Version != fx.bigSeq.Version || r.Height != fx.bigSeq.Height {
		t.Errorf("displayed fields must come from seq %d: version=%q height=%d", fx.bigSeq.Seq, r.Version, r.Height)
	}
	if r.Missed != fx.bigSeq.Missed {
		t.Errorf("first-line cumulative missed = %d, want %d from the latest record", r.Missed, fx.bigSeq.Missed)
	}
	if r.BaselineSeq != fx.smallSeq.Seq || r.BaselineMissed != fx.smallSeq.Missed {
		t.Errorf("baseline = seq %d/missed %d, want %d/%d", r.BaselineSeq, r.BaselineMissed, fx.smallSeq.Seq, fx.smallSeq.Missed)
	}
	if !r.NewMissedKnown || r.NewMissed != 3 {
		t.Errorf("new missed = known:%v %d, want true/3 (5-2)", r.NewMissedKnown, r.NewMissed)
	}
	joined := strings.Join(r.Findings, ";")
	if !strings.Contains(joined, "offline") || !strings.Contains(joined, "version skew: 1.25.0 != 1.26.0") {
		t.Errorf("baseline must not suppress offline/version-skew findings: %v", r.Findings)
	}
	if !strings.Contains(joined, "above tolerance") {
		t.Errorf("new=3 strictly over tolerance 2 must alarm: %v", r.Findings)
	}

	// Equality with the tolerance must not alarm, while status stays offline.
	r, err = store.HealthSince("recon", fx.query, "1.26.0", 3, fx.smallSeq.Seq)
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != "offline" {
		t.Errorf("status=%q, want offline even when the miss alarm is silent", r.Status)
	}
	if hasFinding(r.Findings, "missed duties above tolerance") {
		t.Errorf("new=3 with tolerance 3 must not alarm: %v", r.Findings)
	}

	// Baseline equal to the latest record: new missed is 0, latest rules unchanged.
	r, err = store.HealthSince("recon", fx.query, "1.26.0", 0, fx.bigSeq.Seq)
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != "offline" || r.Seq != fx.bigSeq.Seq || r.Missed != fx.bigSeq.Missed {
		t.Errorf("baseline==latest: %+v, want offline seq %d missed %d", r, fx.bigSeq.Seq, fx.bigSeq.Missed)
	}
	if !r.NewMissedKnown || r.NewMissed != 0 || r.BaselineMissed != fx.bigSeq.Missed {
		t.Errorf("baseline==latest new missed = known:%v %d baseline=%d, want true/0/%d",
			r.NewMissedKnown, r.NewMissed, r.BaselineMissed, fx.bigSeq.Missed)
	}
}

// TestHealthQueryEarlierThanLatestCollectionErrorsWithoutFallback builds a
// history where a smaller-seq record genuinely exists before the requested
// query instant while the greatest-seq record does not yet exist at that
// instant. Both plain and baseline queries must error instead of rewinding to
// the older record and reporting it healthy.
func TestHealthQueryEarlierThanLatestCollectionErrorsWithoutFallback(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	q := testBase
	old := hb("clock", 4, q.Add(-3*time.Minute), "1.0", 100, 0)
	latest := hb("clock", 8, q.Add(-2*time.Minute), "1.0", 104, 0)
	mustSubmit(t, store, q, old, latest)

	// q-150s: seq 4 (q-180s) has already been collected, but the query is 30s
	// before the greatest-seq record's collection time (q-120s).
	at := q.Add(-150 * time.Second)
	if !latest.CollectedAt.After(at) || at.Before(old.CollectedAt) {
		t.Fatal("test setup: query must sit between the two collection times")
	}

	_, err = store.Health("clock", at, "1.0", 0)
	if err == nil {
		t.Fatal("plain query earlier than the latest record's collection time must error instead of falling back to seq 4")
	}
	if !strings.Contains(err.Error(), "earlier than collection") {
		t.Errorf("error should explain the query is earlier than collection: %v", err)
	}

	// A valid baseline (seq 4 exists) changes nothing: the latest-record time
	// rule is enforced before any result is returned, and the baseline record
	// must not be promoted to the answer.
	_, err = store.HealthSince("clock", at, "1.0", 0, old.Seq)
	if err == nil {
		t.Fatal("baseline query earlier than the latest record's collection time must error")
	}
	if !strings.Contains(err.Error(), "earlier than collection") {
		t.Errorf("baseline error should explain the time ordering: %v", err)
	}

	// The same instants queried at/after the latest collection succeed, still
	// anchored to the greatest seq — proving the rejection is about time, not
	// the data.
	r, err := store.Health("clock", q, "1.0", 0)
	if err != nil {
		t.Fatal(err)
	}
	if r.Seq != latest.Seq || r.Status != "offline" {
		t.Errorf("later query: %+v, want seq %d offline", r, latest.Seq)
	}
	r, err = store.HealthSince("clock", q, "1.0", 0, old.Seq)
	if err != nil {
		t.Fatal(err)
	}
	if r.Seq != latest.Seq || r.Status != "offline" || !r.NewMissedKnown {
		t.Errorf("later baseline query: %+v, want seq %d offline with a known new-miss count", r, latest.Seq)
	}

	// Querying the exact greatest-seq collection instant is allowed (the
	// boundary is strict "before"), including with a fraction.
	fractionalLatest := hb("clock-frac", 8,
		mustParseRFC3339(t, "2026-10-01T12:00:00.25Z"), "1.0", 104, 0)
	mustSubmit(t, store, mustParseRFC3339(t, "2026-10-01T12:02:00Z"), fractionalLatest)
	exact := mustParseRFC3339(t, "2026-10-01T12:00:00.25Z")
	r, err = store.Health("clock-frac", exact, "1.0", 0)
	if err != nil {
		t.Fatalf("query at the exact (fractional) collection instant must be allowed: %v", err)
	}
	if r.Status != "online" {
		t.Errorf("at exact collection instant status=%q, want online", r.Status)
	}
}

// TestHealthQueriesNeverMutateStoredHeartbeats snapshots the saved files and
// runs every kind of read — successful online/offline/notelemetry, baseline
// known and rollback-unknown, and the failing early-query and bad-baseline
// paths — then requires the stored heartbeats to be byte-for-byte unchanged,
// including fractional collection instants. A failed query must not create a
// node file either.
func TestHealthQueriesNeverMutateStoredHeartbeats(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	q := testBase

	// Disordered offline node, a fresh online node, and a counter-reset node
	// for the rollback baseline path.
	mustSubmit(t, store, q.Add(-90*time.Second),
		hb("recon", 10, q.Add(-2*time.Minute), "1.25.0", 1000, 5))
	mustSubmit(t, store, q,
		hb("recon", 5, q.Add(-10*time.Second), "1.26.0", 900, 2),
		hb("fresh", 1, mustParseRFC3339(t, "2026-10-01T11:59:59.5Z"), "1.0", 7, 0),
		hb("reset", 1, q.Add(-3*time.Second), "1.0", 1, 12),
		hb("reset", 2, q.Add(-2*time.Second), "1.0", 2, 5),
		hb("reset", 3, q.Add(-time.Second), "1.0", 3, 14),
	)

	snapshot := map[string]string{}
	for _, node := range []string{"recon", "fresh", "reset"} {
		b, err := os.ReadFile(store.nodePath(node))
		if err != nil {
			t.Fatal(err)
		}
		snapshot[node] = string(b)
	}

	if _, err := store.Health("recon", q, "1.26.0", 4); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Health("fresh", q, "1.0", 0); err != nil {
		t.Fatal(err)
	}
	if r, err := store.Health("ghost", q, "1.0", 0); err != nil || r.Status != "notelemetry" {
		t.Fatalf("unknown node: r=%+v err=%v, want notelemetry/nil", r, err)
	}
	if _, err := store.HealthSince("recon", q, "1.26.0", 2, 5); err != nil {
		t.Fatal(err)
	}
	if r, err := store.HealthSince("reset", q, "1.0", 2, 1); err != nil {
		t.Fatal(err)
	} else if r.NewMissedKnown || !hasFinding(r.Findings, "累计漏签数回退") {
		t.Fatalf("rollback query: %+v, want unknown new missed with rollback finding", r)
	}
	if _, err := store.Health("recon", q.Add(-3*time.Minute), "1.26.0", 4); err == nil {
		t.Error("early plain query should fail")
	}
	if _, err := store.HealthSince("recon", q.Add(-3*time.Minute), "1.26.0", 2, 5); err == nil {
		t.Error("early baseline query should fail")
	}
	if _, err := store.HealthSince("recon", q, "1.26.0", 2, 99); err == nil {
		t.Error("baseline above latest should fail")
	}

	// Querying an unknown node must not have created its file.
	if _, err := os.Stat(store.nodePath("ghost")); !os.IsNotExist(err) {
		t.Errorf("querying an unknown node created a file: stat err=%v", err)
	}

	for node, before := range snapshot {
		after, err := os.ReadFile(store.nodePath(node))
		if err != nil {
			t.Fatal(err)
		}
		if string(after) != before {
			t.Errorf("saved heartbeats for %q changed during queries/failures", node)
		}
	}

	// Reopen and confirm content semantics, especially the fractional time.
	reopened, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	hist, err := reopened.History("recon")
	if err != nil || len(hist) != 2 || hist[1].Seq != 10 {
		t.Fatalf("recon history after queries: %+v err=%v", hist, err)
	}
	freshHist, err := reopened.History("fresh")
	if err != nil || len(freshHist) != 1 || freshHist[0].CollectedAt.Nanosecond() != 500_000_000 {
		t.Fatalf("fresh history fraction lost: %+v err=%v", freshHist, err)
	}
}
