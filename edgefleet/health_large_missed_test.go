package edgefleet

// Regression coverage for cumulative missed-duty counts beyond the 32-bit
// signed/unsigned integer ranges. A node reports missed as a non-negative
// int64 (up to 9223372036854775807), and a plain health query must judge that
// full saved counter on both 32-bit and 64-bit systems: a counter of
// 2147483648 or 4294967296 with tolerance 0 must alarm even though narrowing
// it to a 32-bit int would reinterpret it as -2147483648 or 0, and the result
// keeps showing the original value rather than zero or "unknown". A baseline
// query still compares only the post-baseline increase, and a discovered
// counter rollback still makes the increase unknown.

import (
	"strconv"
	"strings"
	"testing"
	"time"
)

// freshLargeMissed stores one fresh online node carrying a single heartbeat
// with the given cumulative missed count and returns its health under a plain
// (no baseline) query.
func freshLargeMissed(t *testing.T, node string, missed int64, tolerated int) HealthResult {
	t.Helper()
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase
	mustSubmit(t, store, receive, hb(node, 1, receive.Add(-time.Second), "1.0", 1, missed))
	r, err := store.Health(node, receive, "1.0", tolerated)
	if err != nil {
		t.Fatalf("health query for missed=%d must not fail: %v", missed, err)
	}
	return r
}

func TestHealthLargeCumulativeMissedAlarmsOnAnyWidth(t *testing.T) {
	// Each value breaks a different 32-bit interpretation: 2^31 looks
	// negative as a signed int32, 2^32 looks zero as an unsigned int32, and
	// MaxInt64 is the legal upper bound and must not be refused or zeroed.
	for _, missed := range []int64{2147483648, 4294967296, 9223372036854775807} {
		r := freshLargeMissed(t, "wide", missed, 0)
		if r.Missed != missed {
			t.Errorf("missed=%d: result missed=%d, want the original full value", missed, r.Missed)
		}
		if !hasFinding(r.Findings, missedAboveToleranceFinding) {
			t.Errorf("missed=%d with tolerance 0 must alarm, findings=%v", missed, r.Findings)
		}
		// A huge missed count never decides liveness: the telemetry is fresh.
		if r.Status != "online" {
			t.Errorf("missed=%d: status=%q, want online", missed, r.Status)
		}
		if len(r.Findings) != 1 || r.Findings[0] != missedAboveToleranceFinding {
			t.Errorf("missed=%d: findings=%v, want only the tolerance alarm", missed, r.Findings)
		}
		if r.NewMissedKnown || r.BaselineSeq != 0 {
			t.Errorf("plain query must leave baseline fields unset: %+v", r)
		}
	}
}

func TestHealthLargeMissedEqualityBoundary(t *testing.T) {
	// The largest value a 32-bit tolerance can name: equality must not alarm
	// on either width, while one more missed duty must.
	r := freshLargeMissed(t, "edge-eq", 2147483647, 2147483647)
	if r.Missed != 2147483647 {
		t.Fatalf("result missed=%d, want 2147483647", r.Missed)
	}
	if len(r.Findings) != 0 {
		t.Errorf("count equal to tolerance must not alarm: %v", r.Findings)
	}

	r = freshLargeMissed(t, "edge-over", 2147483647, 2147483646)
	if !hasFinding(r.Findings, missedAboveToleranceFinding) {
		t.Errorf("count one over tolerance must alarm: %v", r.Findings)
	}

	// 2^31 with a tolerance of 0 is strictly over — the exact case the
	// 32-bit narrowing missed.
	r = freshLargeMissed(t, "edge-2to31", 2147483648, 0)
	if !hasFinding(r.Findings, missedAboveToleranceFinding) {
		t.Errorf("2147483648 > 0 must alarm on every width: %v", r.Findings)
	}
}

func TestHealthLargeToleranceStillAcceptedOn64Bit(t *testing.T) {
	// Tolerances above int32 range are only representable on 64-bit systems;
	// there they must keep working, with equality still silent. The values
	// travel through int64 variables so the file compiles on 32-bit, where
	// the case is simply inapplicable.
	if strconv.IntSize < 64 {
		t.Skip("tolerances above 2147483647 are not representable in int on this platform")
	}
	var big int64 = 3000000000

	equal := freshLargeMissed(t, "edge-big-eq", big, int(big))
	if len(equal.Findings) != 0 {
		t.Errorf("count %d equal to tolerance %d must not alarm: %v", big, big, equal.Findings)
	}
	over := freshLargeMissed(t, "edge-big-over", big+1, int(big))
	if !hasFinding(over.Findings, missedAboveToleranceFinding) {
		t.Errorf("count %d over tolerance %d must alarm: %v", big+1, big, over.Findings)
	}
}

func TestHealthSinceLargeBaselineComparesOnlyNewMissed(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase
	// Baseline cumulative 2^32, latest 2^32+1: on a 32-bit view both values
	// wrap, but the saved int64 difference is exactly 1.
	mustSubmit(t, store, receive,
		hb("n1", 1, receive.Add(-2*time.Second), "1.0", 100, 4294967296),
		hb("n1", 2, receive.Add(-time.Second), "1.0", 101, 4294967297),
	)

	plain, err := store.Health("n1", receive, "1.0", 1)
	if err != nil {
		t.Fatal(err)
	}
	if plain.Missed != 4294967297 || !hasFinding(plain.Findings, missedAboveToleranceFinding) {
		t.Errorf("plain query: %+v, want cumulative 4294967297 alarmed", plain)
	}

	r, err := store.HealthSince("n1", receive, "1.0", 1, 1)
	if err != nil {
		t.Fatalf("baseline query over 32-bit-wide counts must not fail: %v", err)
	}
	if !r.NewMissedKnown {
		t.Fatalf("new missed should be known: %+v", r)
	}
	if r.BaselineMissed != 4294967296 || r.Missed != 4294967297 {
		t.Errorf("counts = baseline %d latest %d, want 4294967296/4294967297", r.BaselineMissed, r.Missed)
	}
	if r.NewMissed != 1 {
		t.Errorf("new missed = %d, want 1", r.NewMissed)
	}
	if hasFinding(r.Findings, missedAboveToleranceFinding) {
		t.Errorf("new=1 with tolerance 1 must not alarm: %v", r.Findings)
	}
	if r.Status != "online" {
		t.Errorf("status=%q, want online", r.Status)
	}

	// Baseline at the latest record: zero new missed duties, no alarm.
	r, err = store.HealthSince("n1", receive, "1.0", 0, 2)
	if err != nil {
		t.Fatal(err)
	}
	if !r.NewMissedKnown || r.NewMissed != 0 || hasFinding(r.Findings, missedAboveToleranceFinding) {
		t.Errorf("baseline==latest: %+v, want known new=0 without alarm", r)
	}
}

func TestHealthSinceLargeCounterRollbackStaysUnknown(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase
	// A counter above 2^32 that resets to a small value must be detected
	// exactly as with small counters: the new count is unknown.
	mustSubmit(t, store, receive,
		hb("n1", 1, receive.Add(-3*time.Second), "1.0", 100, 4294967296),
		hb("n1", 2, receive.Add(-2*time.Second), "1.0", 101, 5),
		hb("n1", 3, receive.Add(-time.Second), "1.0", 102, 4294967297),
	)

	r, err := store.HealthSince("n1", receive, "1.0", 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if r.NewMissedKnown {
		t.Errorf("rollback past a 32-bit-wide value must make new missed unknown: %+v", r)
	}
	if !hasFinding(r.Findings, rollbackFinding) {
		t.Errorf("findings %v missing rollback", r.Findings)
	}
	if hasFinding(r.Findings, missedAboveToleranceFinding) {
		t.Errorf("unknown new count must not produce a tolerance alarm: %v", r.Findings)
	}
	if r.BaselineMissed != 4294967296 || r.Missed != 4294967297 {
		t.Errorf("full cumulative values must still be shown: baseline=%d latest=%d", r.BaselineMissed, r.Missed)
	}
	if r.Status != "online" {
		t.Errorf("rollback and a large counter never affect liveness: status=%q", r.Status)
	}

	// The plain cumulative query ignores rollback semantics and judges the
	// latest value alone.
	plain, err := store.Health("n1", receive, "1.0", 0)
	if err != nil {
		t.Fatal(err)
	}
	if !hasFinding(plain.Findings, missedAboveToleranceFinding) {
		t.Errorf("plain query must alarm on the latest full cumulative count: %v", plain.Findings)
	}
}

func TestHealthLargeMissedKeepsOnlineAndVersionIndependent(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase

	// Fresh telemetry with an enormous counter and version skew stays online;
	// only skew plus the missed alarm are reported.
	mustSubmit(t, store, receive, hb("fresh", 1, receive.Add(-time.Second), "0.9", 1, 4294967296))
	r, err := store.Health("fresh", receive, "1.0", 0)
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != "online" || hasFinding(r.Findings, "offline") {
		t.Errorf("fresh node with huge missed must stay online: %+v", r)
	}
	joined := strings.Join(r.Findings, ";")
	if !strings.Contains(joined, "version skew") || !strings.Contains(joined, "above tolerance") {
		t.Errorf("findings %v must keep version skew and missed alarm", r.Findings)
	}

	// Stale telemetry with the same counter is offline for age alone; the
	// counter neither causes nor suppresses the offline finding.
	mustSubmit(t, store, receive,
		hb("stale", 1, receive.Add(-2*time.Minute), "0.9", 1, 4294967296))
	r, err = store.Health("stale", receive, "1.0", 0)
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != "offline" {
		t.Errorf("stale node must be offline regardless of missed: %+v", r)
	}
	if !hasFinding(r.Findings, "offline") || !hasFinding(r.Findings, missedAboveToleranceFinding) {
		t.Errorf("findings %v must contain offline and the missed alarm", r.Findings)
	}
}

func TestHealthLargeMissedQueryDoesNotMutateHistory(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase
	mustSubmit(t, store, receive,
		hb("n1", 1, receive.Add(-2*time.Second), "1.0", 100, 4294967296),
		hb("n1", 2, receive.Add(-time.Second), "1.0", 101, 9223372036854775807),
	)
	if _, err := store.Health("n1", receive, "1.0", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := store.HealthSince("n1", receive, "1.0", 0, 1); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	hist, err := reopened.History("n1")
	if err != nil {
		t.Fatal(err)
	}
	if len(hist) != 2 || hist[0].Missed != 4294967296 || hist[1].Missed != 9223372036854775807 {
		t.Errorf("history changed after queries: %+v", hist)
	}
}

func TestSubmitStillRejectsNegativeMissedWithLargeRangesAllowed(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase
	bad := hb("bad", 1, receive.Add(-time.Second), "1.0", 1, -1)
	if _, _, err := store.Submit([]Heartbeat{bad}, receive); err == nil ||
		!strings.Contains(err.Error(), "missed") {
		t.Errorf("negative missed must still be rejected, got err=%v", err)
	}
}
