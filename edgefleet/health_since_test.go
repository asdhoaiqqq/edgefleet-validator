package edgefleet

import (
	"strings"
	"testing"
	"time"
)

// mustSubmit writes the given records, failing the test on error.
func mustSubmit(t *testing.T, store *Store, receive time.Time, records ...Heartbeat) {
	t.Helper()
	if _, _, err := store.Submit(records, receive); err != nil {
		t.Fatalf("submit failed: %v", err)
	}
}

// monotonicHistory builds (seq, missed) pairs sharing all other fields; times
// are one second apart and all fresh relative to receive.
func missedSeries(receive time.Time, pairs ...[2]int64) []Heartbeat {
	records := make([]Heartbeat, 0, len(pairs))
	for i, p := range pairs {
		records = append(records, hb("n1", p[0], receive.Add(-time.Duration(len(pairs)-i)*time.Second), "1.0", int64(100+i), p[1]))
	}
	return records
}

func hasFinding(list []string, want string) bool {
	for _, f := range list {
		if f == want {
			return true
		}
	}
	return false
}

func TestHealthSinceNewMissedBoundary(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase
	mustSubmit(t, store, receive, missedSeries(receive, [2]int64{1, 12}, [2]int64{2, 13}, [2]int64{3, 14})...)

	// Baseline cumulative 12, latest 14, tolerance 2: two new missed duties.
	// Equality with the tolerance must NOT alarm, even though the cumulative
	// total (14) is already above the tolerance.
	r, err := store.HealthSince("n1", receive, "1.0", 2, 1)
	if err != nil {
		t.Fatal(err)
	}
	if !r.NewMissedKnown {
		t.Fatalf("new missed should be known: %+v", r)
	}
	if r.BaselineSeq != 1 || r.BaselineMissed != 12 {
		t.Errorf("baseline fields: seq=%d missed=%d, want 1/12", r.BaselineSeq, r.BaselineMissed)
	}
	if r.NewMissed != 2 {
		t.Errorf("new missed = %d, want 2", r.NewMissed)
	}
	if r.Missed != 14 {
		t.Errorf("cumulative missed = %d, want 14", r.Missed)
	}
	if hasFinding(r.Findings, "missed duties above tolerance") {
		t.Errorf("new=2 with tolerance 2 must not alarm: %v", r.Findings)
	}

	// The cumulative query without a baseline still alarms on 14 > 2.
	cum, err := store.Health("n1", receive, "1.0", 2)
	if err != nil {
		t.Fatal(err)
	}
	if !hasFinding(cum.Findings, "missed duties above tolerance") {
		t.Errorf("cumulative query should still alarm: %v", cum.Findings)
	}

	// One more new missed duty: 12 -> 15 strictly exceeds 2 and alarms.
	mustSubmit(t, store, receive, hb("n1", 4, receive.Add(-time.Second), "1.0", 104, 15))
	r, err = store.HealthSince("n1", receive, "1.0", 2, 1)
	if err != nil {
		t.Fatal(err)
	}
	if r.NewMissed != 3 {
		t.Errorf("new missed = %d, want 3", r.NewMissed)
	}
	if !hasFinding(r.Findings, "missed duties above tolerance") {
		t.Errorf("new=3 with tolerance 2 must alarm: %v", r.Findings)
	}
}

func TestHealthSinceBaselineIsLatest(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase
	mustSubmit(t, store, receive, missedSeries(receive, [2]int64{1, 5}, [2]int64{2, 9})...)

	r, err := store.HealthSince("n1", receive, "1.0", 0, 2)
	if err != nil {
		t.Fatal(err)
	}
	if !r.NewMissedKnown || r.NewMissed != 0 {
		t.Errorf("baseline == latest: known=%v new=%d, want true/0", r.NewMissedKnown, r.NewMissed)
	}
	if r.BaselineMissed != 9 || r.Missed != 9 {
		t.Errorf("baseline/latest counts = %d/%d, want 9/9", r.BaselineMissed, r.Missed)
	}
	if hasFinding(r.Findings, "missed duties above tolerance") {
		t.Errorf("zero new missed duties must not alarm: %v", r.Findings)
	}
}

func TestHealthSinceRollbackInIntervalUnknown(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase
	// Counter: 12 -> 5 (reset) -> 14. The endpoints alone (12 -> 14) would
	// suggest 2 new duties, but the saved history proves a decrease.
	mustSubmit(t, store, receive, missedSeries(receive, [2]int64{1, 12}, [2]int64{2, 5}, [2]int64{3, 14})...)

	r, err := store.HealthSince("n1", receive, "1.0", 2, 1)
	if err != nil {
		t.Fatal(err)
	}
	if r.NewMissedKnown {
		t.Errorf("rollback must make new missed unknown: %+v", r)
	}
	if !hasFinding(r.Findings, "累计漏签数回退") {
		t.Errorf("findings %v missing rollback warning", r.Findings)
	}
	if hasFinding(r.Findings, "missed duties above tolerance") {
		t.Errorf("unknown new count must not produce a tolerance alarm: %v", r.Findings)
	}
	if r.Missed != 14 || r.BaselineMissed != 12 {
		t.Errorf("counts = cumulative %d baseline %d, want 14/12", r.Missed, r.BaselineMissed)
	}
	if r.Status != "online" {
		t.Errorf("status=%s, want online", r.Status)
	}
}

func TestHealthSinceRollbackBeforeBaselineIgnored(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase
	// Decrease 100 -> 90 happens before the baseline seq 2; 90 -> 100 is
	// monotonic inside the query interval.
	mustSubmit(t, store, receive, missedSeries(receive, [2]int64{1, 100}, [2]int64{2, 90}, [2]int64{3, 100})...)

	r, err := store.HealthSince("n1", receive, "1.0", 50, 2)
	if err != nil {
		t.Fatal(err)
	}
	if !r.NewMissedKnown {
		t.Errorf("pre-baseline rollback must not matter: %+v", r)
	}
	if r.NewMissed != 10 {
		t.Errorf("new missed = %d, want 10", r.NewMissed)
	}
	if hasFinding(r.Findings, "累计漏签数回退") {
		t.Errorf("no rollback finding expected: %v", r.Findings)
	}
}

func TestHealthSinceSeqGapsCompareAdjacentSaved(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase
	// Seqs may have gaps; seq 4 is still the next saved record after seq 1
	// and a decrease between them is detectable.
	mustSubmit(t, store, receive,
		hb("n1", 1, receive.Add(-3*time.Second), "1.0", 100, 12),
		hb("n1", 4, receive.Add(-time.Second), "1.0", 103, 11),
	)
	r, err := store.HealthSince("n1", receive, "1.0", 5, 1)
	if err != nil {
		t.Fatal(err)
	}
	if r.NewMissedKnown {
		t.Errorf("decrease across a gap must be detected: %+v", r)
	}
	if !hasFinding(r.Findings, "累计漏签数回退") {
		t.Errorf("findings %v missing rollback", r.Findings)
	}
}

func TestHealthSinceLateInsertRevealsRollback(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase
	mustSubmit(t, store, receive,
		hb("n1", 1, receive.Add(-3*time.Second), "1.0", 100, 12),
		hb("n1", 3, receive.Add(-time.Second), "1.0", 102, 14),
	)

	// Endpoints 12 -> 14 with a gap: no saved evidence of a reset.
	r, err := store.HealthSince("n1", receive, "1.0", 2, 1)
	if err != nil {
		t.Fatal(err)
	}
	if !r.NewMissedKnown || r.NewMissed != 2 {
		t.Fatalf("before late insert: known=%v new=%d, want true/2", r.NewMissedKnown, r.NewMissed)
	}

	// A late, replayed record fills the gap and reveals the reset.
	mustSubmit(t, store, receive, hb("n1", 2, receive.Add(-2*time.Second), "1.0", 101, 5))
	r, err = store.HealthSince("n1", receive, "1.0", 2, 1)
	if err != nil {
		t.Fatal(err)
	}
	if r.NewMissedKnown {
		t.Errorf("newly revealed rollback must be reflected in later queries: %+v", r)
	}
	if !hasFinding(r.Findings, "累计漏签数回退") {
		t.Errorf("findings %v missing rollback", r.Findings)
	}

	// Re-submitting the same record changes nothing.
	mustSubmit(t, store, receive, hb("n1", 2, receive.Add(-2*time.Second), "1.0", 101, 5))
	r, err = store.HealthSince("n1", receive, "1.0", 2, 1)
	if err != nil {
		t.Fatal(err)
	}
	if r.NewMissedKnown || r.NewMissed != 0 {
		t.Errorf("resubmitted record must not alter judgement: known=%v new=%d", r.NewMissedKnown, r.NewMissed)
	}
	hist, err := store.History("n1")
	if err != nil {
		t.Fatal(err)
	}
	if len(hist) != 3 {
		t.Errorf("history len=%d, want 3 (duplicate not stored)", len(hist))
	}
}

func TestHealthSinceInvalidBaselines(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase
	mustSubmit(t, store, receive, missedSeries(receive, [2]int64{1, 0}, [2]int64{3, 4})...)

	for _, seq := range []int64{0, -1, -100} {
		if _, err := store.HealthSince("n1", receive, "1.0", 0, seq); err == nil {
			t.Errorf("baseline %d must be rejected", seq)
		}
	}
	// Existing gap: seq 2 was never saved.
	_, err = store.HealthSince("n1", receive, "1.0", 0, 2)
	if err == nil || !strings.Contains(err.Error(), "seq 2") {
		t.Errorf("missing baseline seq must error mentioning the seq, got %v", err)
	}
	// Baseline newer than the latest record.
	_, err = store.HealthSince("n1", receive, "1.0", 0, 4)
	if err == nil || !strings.Contains(err.Error(), "=4") || !strings.Contains(err.Error(), "latest") {
		t.Errorf("baseline above latest must error, got %v", err)
	}
	// No telemetry at all: the baseline cannot be substituted or treated as 0.
	_, err = store.HealthSince("ghost", receive, "1.0", 0, 1)
	if err == nil || !strings.Contains(err.Error(), "no heartbeats") {
		t.Errorf("baseline on node without telemetry must error, got %v", err)
	}
}

func TestHealthSinceKeepsOnlineVersionAndTimeRules(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase
	collected := receive.Add(-2 * time.Minute)
	mustSubmit(t, store, receive,
		hb("n1", 1, collected.Add(-time.Minute), "0.9", 100, 0),
		hb("n1", 2, collected, "0.9", 101, 1),
	)

	// Stale telemetry -> offline; version skew still reported; new missed=1
	// with tolerance 1 does not add a missed-duty alarm.
	r, err := store.HealthSince("n1", receive, "1.0", 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != "offline" {
		t.Errorf("status=%s, want offline", r.Status)
	}
	joined := strings.Join(r.Findings, ";")
	if !strings.Contains(joined, "offline") {
		t.Errorf("findings %v missing offline", r.Findings)
	}
	if !strings.Contains(joined, "version skew") {
		t.Errorf("findings %v missing version skew", r.Findings)
	}
	if strings.Contains(joined, "above tolerance") {
		t.Errorf("new=1/tolerance=1 must not alarm: %v", r.Findings)
	}

	// Query time before the latest collection time is still rejected.
	_, err = store.HealthSince("n1", collected.Add(-time.Second), "1.0", 1, 1)
	if err == nil {
		t.Errorf("query before collection time must be rejected with a baseline")
	}
}

func TestHealthSinceLargeIntegersExact(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase
	const base = int64(9223372036854775800)
	mustSubmit(t, store, receive,
		hb("n1", 1, receive.Add(-2*time.Second), "1.0", 100, base),
		hb("n1", 2, receive.Add(-time.Second), "1.0", 101, 9223372036854775807),
	)
	r, err := store.HealthSince("n1", receive, "1.0", 100, 1)
	if err != nil {
		t.Fatal(err)
	}
	if !r.NewMissedKnown || r.NewMissed != 7 {
		t.Errorf("new missed = known:%v %d, want true/7", r.NewMissedKnown, r.NewMissed)
	}
}

func TestHealthSinceDoesNotMutateStateOrCumulativeQuery(t *testing.T) {
	dir := t.TempDir()
	receive := testBase
	store1, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	mustSubmit(t, store1, receive, missedSeries(receive, [2]int64{1, 12}, [2]int64{3, 14})...)

	if _, err := store1.HealthSince("n1", receive, "1.0", 2, 1); err != nil {
		t.Fatal(err)
	}

	// Reopen the same directory: the baseline is just a saved record, still
	// usable, and history is untouched.
	store2, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	hist, err := store2.History("n1")
	if err != nil {
		t.Fatal(err)
	}
	if len(hist) != 2 || hist[0].Seq != 1 || hist[1].Seq != 3 {
		t.Errorf("history changed after baseline query: %+v", hist)
	}
	r, err := store2.HealthSince("n1", receive, "1.0", 2, 1)
	if err != nil {
		t.Fatal(err)
	}
	if !r.NewMissedKnown || r.NewMissed != 2 {
		t.Errorf("after reopen: known=%v new=%d, want true/2", r.NewMissedKnown, r.NewMissed)
	}
	cum, err := store2.Health("n1", receive, "1.0", 2)
	if err != nil {
		t.Fatal(err)
	}
	if cum.BaselineSeq != 0 || cum.NewMissedKnown {
		t.Errorf("plain Health must leave baseline fields zero: %+v", cum)
	}
	if !hasFinding(cum.Findings, "missed duties above tolerance") {
		t.Errorf("plain Health must keep judging cumulative counts: %v", cum.Findings)
	}
}
