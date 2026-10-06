package edgefleet

// Regression tests for the missed-duty alarm over cumulative counts beyond
// the 32-bit int range. The platform saves missed as a non-negative int64,
// but the plain health query once narrowed it to int before comparing, so on
// 32-bit systems a count like 4294967296 was judged as 0 (or a negative
// number) and the excess was never reported. The judgement must use the full
// counter the node reported, identically on 32-bit and 64-bit systems.

import (
	"math"
	"testing"
	"time"
)

// submitOneMissed stores a single fresh heartbeat with the given cumulative
// missed count for the node.
func submitOneMissed(t *testing.T, store *Store, receive time.Time, node string, missed int64) {
	t.Helper()
	mustSubmit(t, store, receive, hb(node, 1, receive.Add(-time.Second), "1.0", 100, missed))
}

func TestHealthLargeCumulativeMissed(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase

	// Counts beyond the 32-bit range, up to the largest legal int64.
	submitOneMissed(t, store, receive, "big31", 2147483648)
	submitOneMissed(t, store, receive, "big32", 4294967296)
	submitOneMissed(t, store, receive, "bigmax", 9223372036854775807)

	for node, want := range map[string]int64{
		"big31":  2147483648,
		"big32":  4294967296,
		"bigmax": 9223372036854775807,
	} {
		r, err := store.Health(node, receive, "1.0", 0)
		if err != nil {
			t.Fatalf("%s: health query rejected a legal count: %v", node, err)
		}
		if r.Missed != want {
			t.Errorf("%s: missed = %d, want the full reported %d", node, r.Missed, want)
		}
		if !hasFinding(r.Findings, "missed duties above tolerance") {
			t.Errorf("%s: missed %d with tolerance 0 must alarm: %v", node, want, r.Findings)
		}
		// A large missed count never flips a fresh node offline.
		if r.Status != "online" {
			t.Errorf("%s: status = %q, want online (missed excess is not liveness)", node, r.Status)
		}
	}
}

func TestHealthLargeMissedToleranceBoundary(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase

	// The largest tolerance the platform's int can express (2^63-1 on 64-bit,
	// 2^31-1 on 32-bit): a count equal to it must not alarm. maxTol is a
	// variable so the computations below stay runtime int64 ones — as
	// constant expressions they would overflow on 64-bit platforms.
	maxTol := math.MaxInt
	submitOneMissed(t, store, receive, "equal", int64(maxTol))

	r, err := store.Health("equal", receive, "1.0", maxTol)
	if err != nil {
		t.Fatal(err)
	}
	if hasFinding(r.Findings, "missed duties above tolerance") {
		t.Errorf("missed == tolerance must not alarm: %v", r.Findings)
	}
	if r.Missed != int64(maxTol) {
		t.Errorf("missed = %d, want %d", r.Missed, int64(maxTol))
	}

	// One more than the tolerance alarms. On 64-bit maxTol is already the
	// largest int64, so the over-count only exists on 32-bit platforms —
	// where it is exactly the 2^31 case that used to be misjudged.
	if over := int64(maxTol) + 1; over > 0 {
		submitOneMissed(t, store, receive, "over", over)
		r, err = store.Health("over", receive, "1.0", maxTol)
		if err != nil {
			t.Fatal(err)
		}
		if !hasFinding(r.Findings, "missed duties above tolerance") {
			t.Errorf("missed = maxInt+1 with tolerance maxInt must alarm: %v", r.Findings)
		}
		if r.Missed != over {
			t.Errorf("missed = %d, want %d", r.Missed, over)
		}
	}
}

func TestHealthLargeTolerance64Bit(t *testing.T) {
	if math.MaxInt != math.MaxInt64 {
		t.Skip("a tolerance beyond the 32-bit range requires a 64-bit int")
	}
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase

	// A tolerance a 32-bit int cannot hold stays valid on 64-bit systems:
	// equality with the cumulative count must not alarm, one more must.
	// wideTol is a variable so the int conversion compiles on 32-bit too
	// (this test never runs there).
	var wideTol int64 = 4294967296
	submitOneMissed(t, store, receive, "equal", wideTol)
	submitOneMissed(t, store, receive, "over", wideTol+1)

	r, err := store.Health("equal", receive, "1.0", int(wideTol))
	if err != nil {
		t.Fatal(err)
	}
	if hasFinding(r.Findings, "missed duties above tolerance") {
		t.Errorf("missed == 4294967296 with tolerance 4294967296 must not alarm: %v", r.Findings)
	}

	r, err = store.Health("over", receive, "1.0", int(wideTol))
	if err != nil {
		t.Fatal(err)
	}
	if !hasFinding(r.Findings, "missed duties above tolerance") {
		t.Errorf("missed = 4294967297 with tolerance 4294967296 must alarm: %v", r.Findings)
	}
}

func TestHealthSinceLargeCumulativeBaseline(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase
	mustSubmit(t, store, receive,
		hb("n1", 1, receive.Add(-2*time.Second), "1.0", 100, 4294967296),
		hb("n1", 2, receive.Add(-time.Second), "1.0", 101, 4294967297),
	)

	// The baseline query counts only the increase past the baseline: one new
	// missed duty with tolerance 1 is equality, not an excess.
	r, err := store.HealthSince("n1", receive, "1.0", 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if !r.NewMissedKnown || r.NewMissed != 1 {
		t.Errorf("new missed = known:%v %d, want true/1", r.NewMissedKnown, r.NewMissed)
	}
	if r.BaselineMissed != 4294967296 || r.Missed != 4294967297 {
		t.Errorf("counts = baseline %d cumulative %d, want 4294967296/4294967297", r.BaselineMissed, r.Missed)
	}
	if hasFinding(r.Findings, "missed duties above tolerance") {
		t.Errorf("new=1 with tolerance 1 must not alarm: %v", r.Findings)
	}

	// The plain query over the same telemetry judges the full cumulative
	// count: 4294967297 > 1 alarms.
	cum, err := store.Health("n1", receive, "1.0", 1)
	if err != nil {
		t.Fatal(err)
	}
	if !hasFinding(cum.Findings, "missed duties above tolerance") {
		t.Errorf("cumulative 4294967297 with tolerance 1 must alarm: %v", cum.Findings)
	}
	if cum.Missed != 4294967297 {
		t.Errorf("cumulative missed = %d, want 4294967297", cum.Missed)
	}
}
