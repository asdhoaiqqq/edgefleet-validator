package edgefleet

import (
	"strings"
	"testing"
	"time"
)

// These regression tests pin the relationship between backfilled (断连补传)
// heartbeats and the current online judgement. The same node may keep records
// whose seqs have gaps, whose arrival order differs from collection order,
// and whose telemetry fields (version, height, cumulative missed) differ.
//
// The rules protected here:
//
//   - "latest telemetry" is the record with the greatest seq, never the one
//     with the most recent collection time, the last-received record, or a
//     stitch-up of fields from two records;
//   - a smaller-seq record backfilled or re-submitted at a later receive time
//     cannot refresh the latest telemetry's collection time;
//   - the online window is an exact 60s boundary at sub-second precision:
//     exactly 60s is still online, one nanosecond more is offline;
//   - the same two instants written in another timezone must give the same
//     status and the same offline finding;
//   - a query time earlier than the greatest-seq record is an error for both
//     the plain and the baseline query, with no fallback to a smaller-seq
//     record, and no query ever rewrites saved heartbeats.

// assertLatestFields checks a health result against the telemetry of the
// record that must have been selected (the greatest-seq one).
func assertLatestFields(t *testing.T, r HealthResult, want Heartbeat) {
	t.Helper()
	if r.Seq != want.Seq {
		t.Errorf("seq=%d, want %d (must select greatest seq, not freshest collection time)", r.Seq, want.Seq)
	}
	if !r.CollectedAt.Equal(want.CollectedAt) {
		t.Errorf("collected_at=%v, want %v (instant from greatest-seq record)", r.CollectedAt, want.CollectedAt)
	}
	if r.Version != want.Version {
		t.Errorf("version=%q, want %q (from greatest-seq record, not spliced)", r.Version, want.Version)
	}
	if r.Height != want.Height {
		t.Errorf("height=%d, want %d (from greatest-seq record, not spliced)", r.Height, want.Height)
	}
	if r.Missed != want.Missed {
		t.Errorf("missed=%d, want %d (cumulative count from greatest-seq record, not spliced)", r.Missed, want.Missed)
	}
}

// newerIsLarger builds the canonical gap/backfill scenario:
//
//   - seq 8 (old telemetry): collected 2 minutes ago, version 1.24.0,
//     height 900, missed 7 — the record with the greater seq but older
//     collection time;
//   - seq 5 (recent low-seq): collected 10 seconds ago, version 1.26.0,
//     height 999, missed 2 — fresher wall-clock but a smaller seq.
//
// The query instant is testBase; both records predate it. Selection must
// ignore collection-time recency and arrival order.
func olderGreaterSeqRecord(receive time.Time) Heartbeat {
	return hb("n1", 8, receive.Add(-2*time.Minute), "1.24.0", 900, 7)
}
func fresherSmallerSeqRecord(receive time.Time) Heartbeat {
	return hb("n1", 5, receive.Add(-10*time.Second), "1.26.0", 999, 2)
}

func TestHealthSelectsGreatestSeqNotFreshestCollected(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase
	old8 := olderGreaterSeqRecord(receive)
	fresh5 := fresherSmallerSeqRecord(receive)

	// Submit greatest-seq first; the fresher small-seq arrives second.
	mustSubmit(t, store, receive, old8)
	mustSubmit(t, store, receive, fresh5)

	r, err := store.Health("n1", receive, "1.26.0", 10)
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != "offline" {
		t.Errorf("status=%s, want offline (old seq-8 telemetry is 2min stale)", r.Status)
	}
	if !hasFinding(r.Findings, "offline") {
		t.Errorf("findings %v must contain offline", r.Findings)
	}
	assertLatestFields(t, r, old8)

	// Version skew is judged against the selected (seq 8) version 1.24.0,
	// not the fresher record's 1.26.0 — the version the node is actually on
	// according to the latest telemetry, and no splicing.
	if !hasFinding(r.Findings, "version skew: 1.24.0 != 1.26.0") {
		t.Errorf("findings %v must flag skew against selected version 1.24.0", r.Findings)
	}
	// The fresher record's cumulative missed (2) must not be mixed in: the
	// selected record's 7, judged cumulatively against tolerance 10, does not
	// alarm; mixing the fresh count would change the finding set.
	if hasFinding(r.Findings, "missed duties above tolerance") {
		t.Errorf("selected missed=7 with tolerance 10 must not alarm: %v", r.Findings)
	}
}

func TestHealthSelectsGreatestSeqWhenFresherRecordArrivesFirst(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase
	old8 := olderGreaterSeqRecord(receive)
	fresh5 := fresherSmallerSeqRecord(receive)

	// Reverse arrival order: fresher small-seq first, greater-seq (stale)
	// later. The judgement at receive must be identical to the other order.
	mustSubmit(t, store, receive, fresh5)
	mustSubmit(t, store, receive, old8)

	r, err := store.Health("n1", receive, "1.26.0", 10)
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != "offline" {
		t.Errorf("status=%s, want offline regardless of arrival order", r.Status)
	}
	assertLatestFields(t, r, old8)
	if !hasFinding(r.Findings, "version skew: 1.24.0 != 1.26.0") {
		t.Errorf("findings %v must flag skew against 1.24.0", r.Findings)
	}
}

// A genuine late backfill: only the small, fresh record is known at the first
// query (node looks online), then the stale greater-seq record is backfilled
// after reconnect. A later query must flip to offline on the greater seq.
func TestHealthBackfilledStaleGreaterSeqFlipsToOffline(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	// Use a fixed receive for the first phase; the backfill happens at a
	// later receive instant, modelling reconnection after an outage.
	firstReceive := testBase
	fresh5 := hb("n1", 5, firstReceive.Add(-10*time.Second), "1.26.0", 999, 2)
	mustSubmit(t, store, firstReceive, fresh5)

	r, err := store.Health("n1", firstReceive, "1.26.0", 10)
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != "online" || r.Seq != 5 {
		t.Fatalf("before backfill: status=%s seq=%d, want online/5", r.Status, r.Seq)
	}

	// Reconnect: a batch collected during the outage arrives late. seq 8 was
	// collected 2 minutes ago (before seq 5's collection) but has the greater
	// seq, so it becomes the latest telemetry.
	reconnect := firstReceive.Add(3 * time.Minute)
	old8 := hb("n1", 8, firstReceive.Add(-2*time.Minute), "1.24.0", 900, 7)
	mustSubmit(t, store, reconnect, old8)

	r, err = store.Health("n1", firstReceive, "1.26.0", 10)
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != "offline" {
		t.Errorf("after backfill status=%s, want offline on greater-seq stale record", r.Status)
	}
	assertLatestFields(t, r, old8)
}

// Re-submitting the smaller-seq record at an even later time must not refresh
// the latest telemetry: it is either a duplicate (unchanged content) or a
// conflict (changed content), neither of which may move the selected record
// or extend the online window.
func TestLateResubmitSmallerSeqCannotRefreshLatest(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	firstReceive := testBase
	old8 := hb("n1", 8, firstReceive.Add(-2*time.Minute), "1.24.0", 900, 7)
	fresh5 := hb("n1", 5, firstReceive.Add(-10*time.Second), "1.26.0", 999, 2)
	mustSubmit(t, store, firstReceive, old8, fresh5)

	query := firstReceive.Add(5 * time.Minute)
	r, err := store.Health("n1", query, "1.26.0", 10)
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != "offline" || r.Seq != 8 {
		t.Fatalf("baseline: status=%s seq=%d, want offline/8", r.Status, r.Seq)
	}

	// 1) Exact duplicate of seq 5 re-submitted much later. It is counted as a
	// duplicate, adds nothing, and cannot extend freshness.
	laterReceive := firstReceive.Add(10 * time.Minute)
	if _, dup, err := store.Submit([]Heartbeat{fresh5}, laterReceive); err != nil || dup != 1 {
		t.Fatalf("late duplicate: dup=%d err=%v, want 1/nil", dup, err)
	}
	r, err = store.Health("n1", query, "1.26.0", 10)
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != "offline" || r.Seq != 8 {
		t.Errorf("after late duplicate: status=%s seq=%d, want offline/8", r.Status, r.Seq)
	}
	assertLatestFields(t, r, old8)
	// The collection instant of the selected record is untouched.
	if !r.CollectedAt.Equal(old8.CollectedAt) {
		t.Errorf("selected time moved to %v after duplicate, want %v", r.CollectedAt, old8.CollectedAt)
	}

	// 2) A batch attempting to refresh via seq 5 carrying a newer collection
	// time is a conflict and changes nothing.
	conflict5 := hb("n1", 5, laterReceive.Add(-time.Second), "1.26.0", 999, 2)
	if _, _, err := store.Submit([]Heartbeat{conflict5}, laterReceive); err == nil {
		t.Error("seq-5 record with a moved collection time must conflict, not refresh")
	} else if !strings.Contains(err.Error(), "conflict") {
		t.Errorf("expected conflict error, got %v", err)
	}
	r, err = store.Health("n1", query, "1.26.0", 10)
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != "offline" || r.Seq != 8 {
		t.Errorf("after conflict: status=%s seq=%d, want offline/8", r.Status, r.Seq)
	}
	assertLatestFields(t, r, old8)
}

// HealthSince shares the selection rule: a baseline changes only the quantity
// the missed-duty alarm uses; the first-line cumulative count and online state
// still come from the greatest-seq record, and a fresher small-seq record
// must not make the node online.
func TestHealthSinceSelectsGreatestSeqBaselineDoesNotRuleStatus(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase
	// Cumulative missed is monotonic across the saved seqs (4 -> 5 -> 8:
	// 1 -> 2 -> 7), so no rollback is reported; the baseline changes only the
	// alarm quantity. The fresher seq 5 must still be ignored for selection.
	base4 := hb("n1", 4, receive.Add(-3*time.Minute), "1.24.0", 800, 1)
	old8 := hb("n1", 8, receive.Add(-2*time.Minute), "1.24.0", 900, 7)
	fresh5 := fresherSmallerSeqRecord(receive)
	mustSubmit(t, store, receive, base4, old8, fresh5)

	// Baseline cumulative 1, latest 7 -> 6 new; tolerance 3 -> alarm on the
	// baseline-relative quantity only. Online/offline and the first line use
	// seq 8.
	r, err := store.HealthSince("n1", receive, "1.26.0", 3, 4)
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != "offline" {
		t.Errorf("status=%s, want offline; baseline must not decide status", r.Status)
	}
	if r.Seq != 8 {
		t.Errorf("seq=%d, want 8 (greatest seq despite fresh seq 5)", r.Seq)
	}
	if r.Missed != 7 {
		t.Errorf("first-line cumulative missed=%d, want 7 from seq 8", r.Missed)
	}
	if r.BaselineSeq != 4 || r.BaselineMissed != 1 {
		t.Errorf("baseline = %d/%d, want 4/1", r.BaselineSeq, r.BaselineMissed)
	}
	if !r.NewMissedKnown || r.NewMissed != 6 {
		t.Errorf("new missed = known:%v %d, want true/6", r.NewMissedKnown, r.NewMissed)
	}
	assertLatestFields(t, r, old8)
	if !hasFinding(r.Findings, "offline") {
		t.Errorf("findings %v missing offline", r.Findings)
	}
	if !hasFinding(r.Findings, "missed duties above tolerance") {
		t.Errorf("findings %v missing baseline-relative alarm (4 > 3)", r.Findings)
	}
}

func TestHealthOnlineSubsecondBoundary(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	// A fractional collection instant guards against truncating the age to
	// whole seconds (e.g. a terminal display that renders seconds as ints): a
	// naive 1ns-over-60s comparison that floors to seconds would misjudge it.
	collected := time.Date(2026, 10, 1, 11, 59, 0, 500000000, time.UTC) // .500000000
	if _, _, err := store.Submit([]Heartbeat{hb("n1", 1, collected, "1.0", 100, 0)}, collected); err != nil {
		t.Fatal(err)
	}

	// Exactly 60s after collection (age == onlineWindow): still online.
	r, err := store.Health("n1", collected.Add(60*time.Second), "1.0", 0)
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != "online" {
		t.Errorf("age exactly 60s: status=%s, want online", r.Status)
	}

	// 60s + one nanosecond: offline. Floors/ceils to whole seconds would keep
	// it online; the rule is nanosecond-exact.
	r, err = store.Health("n1", collected.Add(60*time.Second+time.Nanosecond), "1.0", 0)
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != "offline" {
		t.Errorf("age 60s + 1ns: status=%s, want offline", r.Status)
	}
	if !hasFinding(r.Findings, "offline") {
		t.Errorf("findings %v must contain offline", r.Findings)
	}
	// The offline judgement is a successful query carrying the right fields.
	if r.Seq != 1 || r.Version != "1.0" || r.Height != 100 || r.Missed != 0 {
		t.Errorf("offline result fields wrong: %+v", r)
	}
}

// The exact boundary from both fractional sides: another fractional layout
// (1ns under 60s) must stay online, and an integer-second case (no fraction)
// must behave the same so fraction handling never loosens the window.
func TestHealthOnlineSubsecondBoundaryBothSides(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	collected := time.Date(2026, 10, 1, 11, 59, 0, 1, time.UTC) // .000000001
	mustSubmit(t, store, collected, hb("n1", 1, collected, "1.0", 100, 0))

	check := func(at time.Time, want string) {
		t.Helper()
		r, err := store.Health("n1", at, "1.0", 0)
		if err != nil {
			t.Fatalf("query at %v failed: %v", at, err)
		}
		if r.Status != want {
			t.Errorf("age %v: status=%s, want %s", at.Sub(collected), r.Status, want)
		}
	}
	check(collected.Add(60*time.Second-time.Nanosecond), "online")
	check(collected.Add(60*time.Second), "online")
	check(collected.Add(60*time.Second+time.Nanosecond), "offline")
}

// The same collection/query instants expressed in different timezones must
// yield the same status; the fractional part survives the representation
// change. This is a library-level check that comparison is on instants.
func TestHealthOnlineBoundaryTimezoneInvariant(t *testing.T) {
	plus8 := time.FixedZone("+08", 8*3600)
	minus5 := time.FixedZone("-05", -5*3600)

	// 11:59:00.5Z == 19:59:00.5+08:00. Query at the 60s+1ns boundary.
	collectedUTC := time.Date(2026, 10, 1, 11, 59, 0, 500000000, time.UTC)
	queryBoundary := collectedUTC.Add(60*time.Second + time.Nanosecond)

	for _, tc := range []struct {
		name      string
		collected time.Time
		query     time.Time
		zone      *time.Location
	}{
		{"utc", collectedUTC, queryBoundary, time.UTC},
		{"plus8", collectedUTC.In(plus8), queryBoundary.In(plus8), plus8},
		{"minus5", collectedUTC.In(minus5), queryBoundary.In(minus5), minus5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			st, err := OpenStore(dir)
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err := st.Submit([]Heartbeat{hb("n1", 1, tc.collected, "1.0", 100, 0)}, tc.collected); err != nil {
				t.Fatal(err)
			}
			r, err := st.Health("n1", tc.query, "1.0", 0)
			if err != nil {
				t.Fatal(err)
			}
			if r.Status != "offline" {
				t.Errorf("status=%s, want offline at 60s+1ns in %s", r.Status, tc.name)
			}
			if !hasFinding(r.Findings, "offline") {
				t.Errorf("findings %v must include offline in %s", r.Findings, tc.name)
			}
			// Nanosecond precision retained after submit/query.
			if r.CollectedAt.Nanosecond() != 500000000 {
				t.Errorf("collected nanos=%d, want 500000000 after zone re-spelling", r.CollectedAt.Nanosecond())
			}
			if !r.CollectedAt.Equal(collectedUTC) {
				t.Errorf("collected instant=%v, want %v across zones", r.CollectedAt, collectedUTC)
			}
		})
	}
}

// A query time earlier than the greatest-seq record's collection time is an
// error for the plain query, even though a smaller-seq record exists and was
// collected even earlier — there must be no fallback to it.
func TestHealthQueryBeforeLatestErrorsWithoutFallback(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase
	small := hb("n1", 5, receive.Add(-10*time.Second), "1.26.0", 999, 2)
	latest := hb("n1", 8, receive.Add(-2*time.Second), "1.26.0", 900, 7)
	mustSubmit(t, store, receive, small, latest)

	// Query at an instant between the smaller record's collection and the
	// latest record's collection: an error, not seq-5 telemetry.
	at := latest.CollectedAt.Add(-time.Second)
	_, err = store.Health("n1", at, "1.26.0", 10)
	if err == nil {
		t.Fatal("query earlier than latest seq must error instead of falling back to seq 5")
	}
	if !strings.Contains(err.Error(), "earlier than collection time") {
		t.Errorf("error must explain query-before-collection, got %v", err)
	}

	// The same instant with a valid baseline fails identically.
	_, err = store.HealthSince("n1", at, "1.26.0", 10, 5)
	if err == nil {
		t.Fatal("baseline query earlier than latest seq must error, not use the baseline/fresh record")
	}
	if !strings.Contains(err.Error(), "earlier than collection time") {
		t.Errorf("baseline error must explain query-before-collection, got %v", err)
	}

	// A query exactly one nanosecond before the latest collection is also an
	// error (strict Before), pinning the nanosecond edge for failure too.
	_, err = store.Health("n1", latest.CollectedAt.Add(-time.Nanosecond), "1.26.0", 10)
	if err == nil {
		t.Error("query 1ns before collection must error")
	}

	// At the exact collection instant the query succeeds (boundary).
	r, err := store.Health("n1", latest.CollectedAt, "1.26.0", 10)
	if err != nil {
		t.Fatalf("query at exact collection instant must succeed, got %v", err)
	}
	if r.Status != "online" || r.Seq != 8 {
		t.Errorf("exact-instant query: status=%s seq=%d, want online/8", r.Status, r.Seq)
	}
}

// Neither a failing query nor a baseline query may rewrite saved heartbeats.
func TestQueriesDoNotMutateHistory(t *testing.T) {
	dir := t.TempDir()
	receive := testBase
	store1, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	small := fresherSmallerSeqRecord(receive)
	old8 := olderGreaterSeqRecord(receive)
	mustSubmit(t, store1, receive, small, old8)

	before, err := store1.History("n1")
	if err != nil {
		t.Fatal(err)
	}

	// Successful queries, both kinds.
	if _, err := store1.Health("n1", receive, "1.26.0", 10); err != nil {
		t.Fatal(err)
	}
	if _, err := store1.HealthSince("n1", receive, "1.26.0", 10, 5); err != nil {
		t.Fatal(err)
	}
	// Failing queries: before-latest (plain and baseline).
	at := old8.CollectedAt.Add(-time.Second)
	if _, err := store1.Health("n1", at, "1.26.0", 10); err == nil {
		t.Error("expected before-latest error")
	}
	if _, err := store1.HealthSince("n1", at, "1.26.0", 10, 5); err == nil {
		t.Error("expected before-latest baseline error")
	}

	store2, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	after, err := store2.History("n1")
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(before) {
		t.Fatalf("history length changed %d -> %d after queries", len(before), len(after))
	}
	for i := range before {
		if !before[i].Equal(after[i]) {
			t.Errorf("record %d changed after queries:\nbefore=%+v\nafter =%+v", i+1, before[i], after[i])
		}
	}
}
