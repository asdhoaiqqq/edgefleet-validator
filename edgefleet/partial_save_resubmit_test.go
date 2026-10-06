package edgefleet

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// findRecord returns the record with the given seq, if present.
func findRecord(hist []Heartbeat, seq int64) (Heartbeat, bool) {
	for _, r := range hist {
		if r.Seq == seq {
			return r, true
		}
	}
	return Heartbeat{}, false
}

// TestSubmitPartialSaveResubmit covers the recovery flow promised for a
// persistence failure that strikes halfway through a batch: one node's new
// record is already saved when another node's write fails, the submit
// reports failure with zero counts (the saved part must not be reported as
// a successful batch), and resubmitting the identical batch once the fault
// is fixed completes exactly the missing records.
//
// Both nodes already have seq 1 saved. The batch holds four records: seq 2
// for each node, node B's seq 2 a second time with identical content (an
// in-batch duplicate), and node A's already-saved seq 1. The failure is
// injected by making node B's storage directory unwritable, so node B's
// write fails while node A's can succeed. Node write order is not a public
// promise: if node B happens to be attempted first, nothing new is saved at
// all — a different scenario this test must not mistake for a partial save —
// so the attempt is repeated until History itself shows exactly one new
// record on disk. The error return alone is never trusted as proof.
func TestSubmitPartialSaveResubmit(t *testing.T) {
	receive := testBase
	nodeA := "val-retry-a"                    // short id: nodes/<hex>.json
	nodeB := strings.Repeat("val-slot-b", 13) // long id: slots/<sha256>.json

	a1 := hb(nodeA, 1, receive.Add(-4*time.Minute), "1.0.0", 100, 1)
	a2 := hb(nodeA, 2, receive.Add(-3*time.Minute), "1.1.0", 200, 2)
	b1 := hb(nodeB, 1, receive.Add(-2*time.Minute), "2.0.0", 300, 3)
	b2 := hb(nodeB, 2, receive.Add(-time.Minute), "2.1.0", 400, 4)

	// The batch that is submitted, fails halfway, and is later resubmitted
	// verbatim with the original receive time.
	batch := []Heartbeat{a2, b2, b2, a1}

	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	// Seed: both nodes already have their seq 1 record.
	if n, d, err := store.Submit([]Heartbeat{a1, b1}, receive); err != nil || n != 2 || d != 0 {
		t.Fatalf("seed submit = new=%d duplicate=%d err=%v, want new=2 duplicate=0", n, d, err)
	}

	slotsDir := filepath.Join(dir, "slots")
	defer os.Chmod(slotsDir, 0o755) // keep t.TempDir cleanup possible

	var partialA, partialB bool
	const maxAttempts = 100
	for attempt := 1; ; attempt++ {
		// Node B's slot directory rejects new files, so its write fails
		// while node A's can succeed.
		if err := os.Chmod(slotsDir, 0o500); err != nil {
			t.Fatal(err)
		}
		n, d, serr := store.Submit(batch, receive)
		os.Chmod(slotsDir, 0o755)

		if serr == nil {
			// Either the injection is ineffective here (e.g. the test runs
			// as root, where directory permissions do not block writes), or
			// Submit silently dropped a failed write — tell them apart by
			// what actually landed.
			hist, herr := store.History(nodeB)
			if herr != nil {
				t.Fatal(herr)
			}
			if _, ok := findRecord(hist, 2); !ok {
				t.Fatalf("submit reported success but node B's new record was never saved")
			}
			t.Skipf("write-failure injection ineffective in this environment (e.g. running as root)")
		}
		if n != 0 || d != 0 {
			t.Fatalf("failed submit = new=%d duplicate=%d, want both 0: a partially saved batch must not be reported as saved", n, d)
		}
		if !strings.Contains(serr.Error(), "failed to persist heartbeat batch") {
			t.Fatalf("failed submit error = %v, want the persistence-failure wording", serr)
		}

		// Prove through real queries what the failure left behind: both
		// nodes' old seq 1 records intact, and the new seq 2 record saved
		// for at most one of them, with content identical to the submission.
		histA, err := store.History(nodeA)
		if err != nil {
			t.Fatal(err)
		}
		histB, err := store.History(nodeB)
		if err != nil {
			t.Fatal(err)
		}
		checkOld := func(node string, hist []Heartbeat, old Heartbeat) {
			t.Helper()
			got, ok := findRecord(hist, 1)
			if !ok || !got.Equal(old) {
				t.Fatalf("node %q: previously saved record lost or changed by failed submit: history %+v, want seq 1 = %+v", node, hist, old)
			}
			if len(hist) > 2 {
				t.Fatalf("node %q: unexpected records after failed submit: %+v", node, hist)
			}
		}
		checkOld(nodeA, histA, a1)
		checkOld(nodeB, histB, b1)

		checkNew := func(node string, hist []Heartbeat, submitted Heartbeat) bool {
			t.Helper()
			got, ok := findRecord(hist, 2)
			if ok && !got.Equal(submitted) {
				t.Fatalf("node %q: saved seq 2 record %+v differs from the submission %+v", node, got, submitted)
			}
			return ok
		}
		savedA := checkNew(nodeA, histA, a2)
		savedB := checkNew(nodeB, histB, b2)

		if savedA && savedB {
			t.Fatalf("submit reported a persistence failure but both new records are queryable")
		}
		if !savedA && !savedB {
			// Node B was attempted first: nothing new was saved, so this is
			// not the partial-save scenario. The stored state is unchanged;
			// try again.
			if attempt == maxAttempts {
				t.Fatalf("no partial save observed in %d attempts", maxAttempts)
			}
			continue
		}
		partialA, partialB = savedA, savedB
		break
	}
	t.Logf("partial save observed: node A seq 2 saved=%v, node B seq 2 saved=%v", partialA, partialB)

	// Fault fixed (the directory is writable again): resubmit the identical
	// batch with the original receive time. The record already saved counts
	// as a duplicate, the missing record is new exactly once, the second
	// copy of node B's seq 2 is still an in-batch duplicate, and node A's
	// old seq 1 is a duplicate of the saved history — new=1 duplicate=3
	// regardless of which node happened to be saved before the failure.
	n, d, err := store.Submit(batch, receive)
	if err != nil {
		t.Fatalf("resubmit after recovery: %v", err)
	}
	if n != 1 || d != 3 {
		t.Fatalf("resubmit = new=%d duplicate=%d, want new=1 duplicate=3", n, d)
	}

	// Final state, read through a freshly opened store so only genuinely
	// persisted data can satisfy it: each node holds exactly its own seq 1
	// and seq 2, ascending, old content and new content both intact.
	reopened, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		node string
		want []Heartbeat
	}{
		{nodeA, []Heartbeat{a1, a2}},
		{nodeB, []Heartbeat{b1, b2}},
	} {
		hist, err := reopened.History(tc.node)
		if err != nil {
			t.Fatal(err)
		}
		if len(hist) != 2 {
			t.Fatalf("node %q: final history has %d records, want exactly 2: %+v", tc.node, len(hist), hist)
		}
		for i, want := range tc.want {
			if !hist[i].Equal(want) {
				t.Errorf("node %q: record %d = %+v, want %+v", tc.node, i+1, hist[i], want)
			}
		}
		if hist[0].Seq >= hist[1].Seq {
			t.Errorf("node %q: history not ascending by seq: %+v", tc.node, hist)
		}
	}
}
