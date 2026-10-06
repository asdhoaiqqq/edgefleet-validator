package edgefleet

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

var testBase = time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)

func hb(node string, seq int64, collected time.Time, version string, height, missed int64) Heartbeat {
	return Heartbeat{
		NodeID:      node,
		Seq:         seq,
		CollectedAt: collected,
		Version:     version,
		Height:      height,
		Missed:      missed,
	}
}

// loadNodeFileFor reads a node's file through the same location mapping and
// verification the store uses, for tests that forge raw on-disk files.
func loadNodeFileFor(store *Store, nodeID string) (*nodeFile, error) {
	return loadNodeFile(store.nodeLocationFor(nodeID))
}

func TestParseHeartbeatsValid(t *testing.T) {
	input := `[
	  {"node":"val-1","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0.0","height":100,"missed":0},
	  {"node":"val-2","seq":2,"collected_at":"2026-10-01T20:00:00+08:00","version":"1.0.0","height":101,"missed":1}
	]`
	records, err := ParseHeartbeats([]byte(input), testBase)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(records))
	}
	if records[0].NodeID != "val-1" || records[0].Seq != 1 || records[0].Height != 100 || records[0].Missed != 0 {
		t.Errorf("record 0 mismatch: %+v", records[0])
	}
	// +08:00 20:00 is 12:00 UTC, equal to testBase -> valid (not later than receive).
	if !records[1].CollectedAt.Equal(testBase) {
		t.Errorf("record 1 time = %v, want %v", records[1].CollectedAt, testBase)
	}
}

func TestParseHeartbeatsInvalid(t *testing.T) {
	cases := []struct {
		name  string
		input string
	}{
		{"not array", `{"node":"val-1"}`},
		{"empty array", `[]`},
		{"missing node", `[{"seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":1,"missed":0}]`},
		{"missing seq", `[{"node":"val-1","collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":1,"missed":0}]`},
		{"missing collected_at", `[{"node":"val-1","seq":1,"version":"1.0","height":1,"missed":0}]`},
		{"missing version", `[{"node":"val-1","seq":1,"collected_at":"2026-10-01T11:59:00Z","height":1,"missed":0}]`},
		{"missing height", `[{"node":"val-1","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","missed":0}]`},
		{"missing missed", `[{"node":"val-1","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":1}]`},
		{"unknown field", `[{"node":"val-1","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":1,"missed":0,"extra":5}]`},
		{"empty node", `[{"node":"","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":1,"missed":0}]`},
		{"seq zero", `[{"node":"val-1","seq":0,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":1,"missed":0}]`},
		{"seq negative", `[{"node":"val-1","seq":-1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":1,"missed":0}]`},
		{"seq float", `[{"node":"val-1","seq":1.5,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":1,"missed":0}]`},
		{"collected_at no timezone", `[{"node":"val-1","seq":1,"collected_at":"2026-10-01T11:59:00","version":"1.0","height":1,"missed":0}]`},
		{"collected_at garbage", `[{"node":"val-1","seq":1,"collected_at":"yesterday","version":"1.0","height":1,"missed":0}]`},
		{"collected_at after receive", `[{"node":"val-1","seq":1,"collected_at":"2026-10-01T12:00:01Z","version":"1.0","height":1,"missed":0}]`},
		{"empty version", `[{"node":"val-1","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"","height":1,"missed":0}]`},
		{"height negative", `[{"node":"val-1","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":-1,"missed":0}]`},
		{"missed negative", `[{"node":"val-1","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":1,"missed":-1}]`},
		{"record not object", `[42]`},
		{"node null", `[{"node":null,"seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":1,"missed":0}]`},
		{"seq null", `[{"node":"val-1","seq":null,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":1,"missed":0}]`},
		{"collected_at null", `[{"node":"val-1","seq":1,"collected_at":null,"version":"1.0","height":1,"missed":0}]`},
		{"version null", `[{"node":"val-1","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":null,"height":1,"missed":0}]`},
		{"height null", `[{"node":"val-1","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":null,"missed":0}]`},
		{"missed null", `[{"node":"val-1","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":1,"missed":null}]`},
		{"duplicate missed same value", `[{"node":"val-1","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":1,"missed":0,"missed":0}]`},
		{"duplicate missed diff value", `[{"node":"val-1","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":1,"missed":0,"missed":1}]`},
		{"duplicate height", `[{"node":"val-1","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":1,"height":2,"missed":0}]`},
		{"duplicate node", `[{"node":"a","node":"b","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":1,"missed":0}]`},
		{"duplicate seq", `[{"node":"val-1","seq":1,"seq":2,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":1,"missed":0}]`},
		{"duplicate collected_at", `[{"node":"val-1","seq":1,"collected_at":"2026-10-01T11:59:00Z","collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":1,"missed":0}]`},
		{"duplicate version", `[{"node":"val-1","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","version":"2.0","height":1,"missed":0}]`},
		{"duplicate via unicode escape", `[{"node":"val-1","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":1,"missed":0,"missed":1}]`},
		{"duplicate via unicode escape reversed", `[{"node":"val-1","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":1,"missed":0,"missed":1}]`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseHeartbeats([]byte(tc.input), testBase)
			if err == nil {
				t.Errorf("expected error for %s, got nil", tc.name)
			}
		})
	}
}

func TestValidateHeartbeat(t *testing.T) {
	receive := testBase
	valid := hb("n1", 1, receive.Add(-time.Minute), "1.0", 10, 0)
	if err := ValidateHeartbeat(valid, receive); err != nil {
		t.Errorf("valid record rejected: %v", err)
	}
	// Boundary: collected exactly at receive time is allowed.
	exact := hb("n1", 1, receive, "1.0", 10, 0)
	if err := ValidateHeartbeat(exact, receive); err != nil {
		t.Errorf("collected_at == receive_time should be allowed: %v", err)
	}
	// Boundary: height 0 and missed 0 are allowed.
	zero := hb("n1", 1, receive.Add(-time.Minute), "1.0", 0, 0)
	if err := ValidateHeartbeat(zero, receive); err != nil {
		t.Errorf("zero height/missed should be allowed: %v", err)
	}
	// Future collection time rejected.
	future := hb("n1", 1, receive.Add(time.Second), "1.0", 10, 0)
	if err := ValidateHeartbeat(future, receive); err == nil {
		t.Errorf("future collected_at should be rejected")
	}
}

func TestSubmitNewAndDuplicate(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase
	batch := []Heartbeat{
		hb("n1", 1, receive.Add(-2*time.Minute), "1.0", 100, 0),
		hb("n1", 2, receive.Add(-time.Minute), "1.0", 101, 0),
		hb("n2", 5, receive.Add(-time.Minute), "2.0", 50, 3),
	}
	newC, dupC, err := store.Submit(batch, receive)
	if err != nil {
		t.Fatalf("submit failed: %v", err)
	}
	if newC != 3 || dupC != 0 {
		t.Errorf("first submit: new=%d dup=%d, want 3/0", newC, dupC)
	}

	// Re-submit the same records: all duplicates, nothing new.
	newC, dupC, err = store.Submit(batch, receive)
	if err != nil {
		t.Fatalf("resubmit failed: %v", err)
	}
	if newC != 0 || dupC != 3 {
		t.Errorf("resubmit: new=%d dup=%d, want 0/3", newC, dupC)
	}

	// Same instant expressed in a different timezone is still a duplicate.
	rebased := []Heartbeat{hb("n1", 1, receive.Add(-2*time.Minute).In(time.FixedZone("+08", 8*3600)), "1.0", 100, 0)}
	newC, dupC, err = store.Submit(rebased, receive)
	if err != nil {
		t.Fatalf("rebased submit failed: %v", err)
	}
	if newC != 0 || dupC != 1 {
		t.Errorf("rebased: new=%d dup=%d, want 0/1", newC, dupC)
	}
}

func TestSubmitConflict(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase
	first := hb("n1", 1, receive.Add(-time.Minute), "1.0", 100, 0)
	if _, _, err := store.Submit([]Heartbeat{first}, receive); err != nil {
		t.Fatal(err)
	}

	// Same node+seq, different version -> conflict, no overwrite.
	conflict := hb("n1", 1, receive.Add(-time.Minute), "2.0", 100, 0)
	_, _, err = store.Submit([]Heartbeat{conflict}, receive)
	if err == nil {
		t.Errorf("expected conflict error, got nil")
	}

	// Original record unchanged.
	hist, err := store.History("n1")
	if err != nil {
		t.Fatal(err)
	}
	if len(hist) != 1 || hist[0].Version != "1.0" {
		t.Errorf("stored record changed after conflict: %+v", hist)
	}
}

func TestBatchAtomicity(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase

	// One invalid record in a batch of valid ones: nothing is saved.
	batch := []Heartbeat{
		hb("n1", 1, receive.Add(-time.Minute), "1.0", 100, 0),
		hb("n2", 2, receive.Add(-time.Minute), "1.0", -1, 0), // invalid
	}
	_, _, err = store.Submit(batch, receive)
	if err == nil {
		t.Errorf("expected error for invalid batch")
	}
	for _, node := range []string{"n1", "n2"} {
		hist, err := store.History(node)
		if err != nil {
			t.Fatal(err)
		}
		if len(hist) != 0 {
			t.Errorf("node %s got records despite invalid batch: %+v", node, hist)
		}
	}

	// One conflict in a batch: nothing is saved.
	first := hb("n1", 1, receive.Add(-time.Minute), "1.0", 100, 0)
	if _, _, err := store.Submit([]Heartbeat{first}, receive); err != nil {
		t.Fatal(err)
	}
	conflictBatch := []Heartbeat{
		hb("n3", 1, receive.Add(-time.Minute), "1.0", 100, 0),
		hb("n1", 1, receive.Add(-time.Minute), "9.9", 100, 0), // conflicts
	}
	_, _, err = store.Submit(conflictBatch, receive)
	if err == nil {
		t.Errorf("expected conflict error")
	}
	hist3, _ := store.History("n3")
	if len(hist3) != 0 {
		t.Errorf("n3 got record despite conflicting batch: %+v", hist3)
	}
}

func TestBatchInternalDuplicateAndConflict(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase

	// Same node+seq, equal fields twice: duplicate within batch.
	dupBatch := []Heartbeat{
		hb("n1", 1, receive.Add(-time.Minute), "1.0", 100, 0),
		hb("n1", 1, receive.Add(-time.Minute), "1.0", 100, 0),
	}
	newC, dupC, err := store.Submit(dupBatch, receive)
	if err != nil {
		t.Fatalf("duplicate batch failed: %v", err)
	}
	if newC != 1 || dupC != 1 {
		t.Errorf("internal dup: new=%d dup=%d, want 1/1", newC, dupC)
	}

	// Same node+seq, different fields: conflict within batch.
	conflictBatch := []Heartbeat{
		hb("n2", 1, receive.Add(-time.Minute), "1.0", 100, 0),
		hb("n2", 1, receive.Add(-time.Minute), "2.0", 100, 0),
	}
	_, _, err = store.Submit(conflictBatch, receive)
	if err == nil {
		t.Errorf("expected internal conflict error")
	}
	hist, _ := store.History("n2")
	if len(hist) != 0 {
		t.Errorf("n2 got records despite internal conflict: %+v", hist)
	}
}

func TestSeqOrderingAndLateRecords(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase
	// Submit out of order; seqs may jump.
	batch := []Heartbeat{
		hb("n1", 5, receive.Add(-time.Minute), "1.0", 105, 0),
		hb("n1", 1, receive.Add(-5*time.Minute), "1.0", 101, 0),
		hb("n1", 3, receive.Add(-3*time.Minute), "1.0", 103, 0),
	}
	if _, _, err := store.Submit(batch, receive); err != nil {
		t.Fatal(err)
	}
	hist, err := store.History("n1")
	if err != nil {
		t.Fatal(err)
	}
	if len(hist) != 3 {
		t.Fatalf("expected 3 records, got %d", len(hist))
	}
	for i, wantSeq := range []int64{1, 3, 5} {
		if hist[i].Seq != wantSeq {
			t.Errorf("position %d: seq=%d, want %d", i, hist[i].Seq, wantSeq)
		}
	}

	// Late record with a smaller seq still gets saved.
	late := hb("n1", 2, receive.Add(-4*time.Minute), "1.0", 102, 0)
	if _, _, err := store.Submit([]Heartbeat{late}, receive); err != nil {
		t.Fatalf("late record rejected: %v", err)
	}
	hist, _ = store.History("n1")
	if len(hist) != 4 || hist[1].Seq != 2 {
		t.Errorf("late record not inserted in order: %+v", hist)
	}

	// Current telemetry is always the max seq.
	result, err := store.Health("n1", receive, "1.0", 0)
	if err != nil {
		t.Fatal(err)
	}
	if result.Seq != 5 || result.Height != 105 {
		t.Errorf("current telemetry = seq %d height %d, want seq 5 height 105", result.Seq, result.Height)
	}
}

func TestDifferentNodesShareSeq(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase
	batch := []Heartbeat{
		hb("n1", 1, receive.Add(-time.Minute), "1.0", 100, 0),
		hb("n2", 1, receive.Add(-time.Minute), "1.0", 200, 0),
	}
	newC, _, err := store.Submit(batch, receive)
	if err != nil {
		t.Fatal(err)
	}
	if newC != 2 {
		t.Errorf("different nodes same seq: new=%d, want 2", newC)
	}
}

func TestRestartConsistency(t *testing.T) {
	dir := t.TempDir()
	receive := testBase
	batch := []Heartbeat{
		hb("n1", 1, receive.Add(-2*time.Minute), "1.0", 100, 0),
		hb("n1", 2, receive.Add(-time.Minute), "1.0", 101, 0),
	}
	store1, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := store1.Submit(batch, receive); err != nil {
		t.Fatal(err)
	}

	// Simulate a platform restart: a new Store instance on the same dir.
	store2, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	hist, err := store2.History("n1")
	if err != nil {
		t.Fatal(err)
	}
	if len(hist) != 2 {
		t.Fatalf("after restart: %d records, want 2", len(hist))
	}
	// Duplicate recognition survives restart.
	newC, dupC, err := store2.Submit(batch, receive)
	if err != nil {
		t.Fatal(err)
	}
	if newC != 0 || dupC != 2 {
		t.Errorf("after restart resubmit: new=%d dup=%d, want 0/2", newC, dupC)
	}
	// Current telemetry selection survives restart.
	result, err := store2.Health("n1", receive, "1.0", 0)
	if err != nil {
		t.Fatal(err)
	}
	if result.Seq != 2 {
		t.Errorf("after restart current seq = %d, want 2", result.Seq)
	}
}

func TestHealthOnlineBoundary(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	collected := testBase
	if _, _, err := store.Submit([]Heartbeat{hb("n1", 1, collected, "1.0", 100, 0)}, collected); err != nil {
		t.Fatal(err)
	}

	// Exactly 60s after collection: still online.
	r, err := store.Health("n1", collected.Add(60*time.Second), "1.0", 0)
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != "online" {
		t.Errorf("at exactly 60s: status=%s, want online", r.Status)
	}

	// 61s after collection: offline.
	r, err = store.Health("n1", collected.Add(61*time.Second), "1.0", 0)
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != "offline" {
		t.Errorf("at 61s: status=%s, want offline", r.Status)
	}
	if r.Seq != 1 || r.Version != "1.0" || r.Height != 100 || r.Missed != 0 {
		t.Errorf("offline result fields wrong: %+v", r)
	}
}

func TestHealthQueryTimeBeforeRecord(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	collected := testBase
	if _, _, err := store.Submit([]Heartbeat{hb("n1", 1, collected, "1.0", 100, 0)}, collected); err != nil {
		t.Fatal(err)
	}
	_, err = store.Health("n1", collected.Add(-time.Second), "1.0", 0)
	if err == nil {
		t.Errorf("expected error when query time is before collection time")
	}
}

func TestHealthNoTelemetry(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	r, err := store.Health("ghost", testBase, "1.0", 0)
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != "notelemetry" {
		t.Errorf("status=%q, want notelemetry", r.Status)
	}
	if len(r.Findings) != 1 || r.Findings[0] != "无遥测" {
		t.Errorf("findings=%v, want [无遥测]", r.Findings)
	}
}

func TestHealthVersionSkewAndMissed(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	collected := testBase
	if _, _, err := store.Submit([]Heartbeat{hb("n1", 1, collected, "0.9", 100, 5)}, collected); err != nil {
		t.Fatal(err)
	}
	r, err := store.Health("n1", collected, "1.0", 3)
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != "online" {
		t.Errorf("status=%s, want online (fresh telemetry)", r.Status)
	}
	joined := strings.Join(r.Findings, ";")
	if !strings.Contains(joined, "version skew") {
		t.Errorf("findings %v missing version skew", r.Findings)
	}
	if !strings.Contains(joined, "missed duties above tolerance") {
		t.Errorf("findings %v missing missed-duties", r.Findings)
	}
}

func TestLateReplayNotHealthy(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase
	// Record collected 2 hours ago, only just received.
	collected := receive.Add(-2 * time.Hour)
	if _, _, err := store.Submit([]Heartbeat{hb("n1", 1, collected, "1.0", 100, 0)}, receive); err != nil {
		t.Fatal(err)
	}
	r, err := store.Health("n1", receive, "1.0", 0)
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != "offline" {
		t.Errorf("late replay status=%s, want offline (stale telemetry must not be judged healthy)", r.Status)
	}
}

func TestHistoryEmpty(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	hist, err := store.History("nope")
	if err != nil {
		t.Fatal(err)
	}
	if len(hist) != 0 {
		t.Errorf("expected empty history, got %+v", hist)
	}
}

func TestCorruptionRefused(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase
	if _, _, err := store.Submit([]Heartbeat{hb("n1", 1, receive.Add(-time.Minute), "1.0", 100, 0)}, receive); err != nil {
		t.Fatal(err)
	}
	path := store.nodePath("n1")

	cases := []struct {
		name    string
		content string
	}{
		{"garbage", `not json at all`},
		{"wrong format", `{"format":"other","checksum":"x","records":[]}`},
		{"bad checksum", `{"format":"edgefleet-heartbeats-v1","checksum":"deadbeef","records":[{"node":"n1","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":100,"missed":0}]}`},
		{"unsorted", `{"format":"edgefleet-heartbeats-v1","checksum":"x","records":[{"node":"n1","seq":2,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":100,"missed":0},{"node":"n1","seq":1,"collected_at":"2026-10-01T11:58:00Z","version":"1.0","height":100,"missed":0}]}`},
		{"duplicate seq", `{"format":"edgefleet-heartbeats-v1","checksum":"x","records":[{"node":"n1","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":100,"missed":0},{"node":"n1","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":100,"missed":0}]}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := os.WriteFile(path, []byte(tc.content), 0o644); err != nil {
				t.Fatal(err)
			}
			// Submit must refuse.
			_, _, err := store.Submit([]Heartbeat{hb("n1", 2, receive.Add(-time.Minute), "1.0", 101, 0)}, receive)
			if err == nil || !IsCorrupt(err) {
				t.Errorf("submit: expected corruption error, got %v", err)
			}
			// Health must refuse.
			_, err = store.Health("n1", receive, "1.0", 0)
			if err == nil || !IsCorrupt(err) {
				t.Errorf("health: expected corruption error, got %v", err)
			}
			// History must refuse.
			_, err = store.History("n1")
			if err == nil || !IsCorrupt(err) {
				t.Errorf("history: expected corruption error, got %v", err)
			}
		})
	}
}

func TestCorruptionDoesNotAffectOtherNodes(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase
	if _, _, err := store.Submit([]Heartbeat{
		hb("good", 1, receive.Add(-time.Minute), "1.0", 100, 0),
		hb("bad", 1, receive.Add(-time.Minute), "1.0", 100, 0),
	}, receive); err != nil {
		t.Fatal(err)
	}
	// Corrupt only the "bad" node file.
	if err := os.WriteFile(store.nodePath("bad"), []byte(`garbage`), 0o644); err != nil {
		t.Fatal(err)
	}
	// "good" node is still queryable.
	r, err := store.Health("good", receive, "1.0", 0)
	if err != nil {
		t.Fatalf("good node affected by bad node corruption: %v", err)
	}
	if r.Status != "online" {
		t.Errorf("good node status=%s, want online", r.Status)
	}
	// Submitting to "good" still works.
	if _, _, err := store.Submit([]Heartbeat{hb("good", 2, receive.Add(-time.Minute), "1.0", 101, 0)}, receive); err != nil {
		t.Errorf("submit to good node failed: %v", err)
	}
}

func TestConcurrentSubmitSameRecord(t *testing.T) {
	dir := t.TempDir()
	receive := testBase
	record := hb("n1", 1, receive.Add(-time.Minute), "1.0", 100, 0)

	var mu sync.Mutex
	totalNew, totalDup := 0, 0
	var errs []error
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			store, err := OpenStore(dir)
			if err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
				return
			}
			n, d, err := store.Submit([]Heartbeat{record}, receive)
			mu.Lock()
			totalNew += n
			totalDup += d
			if err != nil {
				errs = append(errs, err)
			}
			mu.Unlock()
		}()
	}
	wg.Wait()
	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}
	if totalNew != 1 || totalDup != 19 {
		t.Errorf("new=%d dup=%d, want 1/19", totalNew, totalDup)
	}
}

func TestConcurrentSubmitConflict(t *testing.T) {
	dir := t.TempDir()
	receive := testBase
	r1 := hb("n1", 1, receive.Add(-time.Minute), "1.0", 100, 0)
	r2 := hb("n1", 1, receive.Add(-time.Minute), "2.0", 100, 0)

	var mu sync.Mutex
	successes := 0
	conflicts := 0
	var errs []error
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			store, err := OpenStore(dir)
			if err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
				return
			}
			r := r1
			if i%2 == 1 {
				r = r2
			}
			_, _, err = store.Submit([]Heartbeat{r}, receive)
			mu.Lock()
			if err == nil {
				successes++
			} else if strings.Contains(err.Error(), "conflict") {
				conflicts++
			} else {
				errs = append(errs, err)
			}
			mu.Unlock()
		}(i)
	}
	wg.Wait()
	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}
	// All 10 submissions of the winning variant succeed (1 new + 9
	// duplicates); all 10 of the losing variant hit the conflict.
	if successes != 10 {
		t.Errorf("successes=%d, want 10", successes)
	}
	if conflicts != 10 {
		t.Errorf("conflicts=%d, want 10", conflicts)
	}
	// The stored record is one of the two, intact.
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	hist, err := store.History("n1")
	if err != nil {
		t.Fatal(err)
	}
	if len(hist) != 1 || (hist[0].Version != "1.0" && hist[0].Version != "2.0") {
		t.Errorf("stored record invalid: %+v", hist)
	}
}

func TestConcurrentQuerySeesConsistentState(t *testing.T) {
	dir := t.TempDir()
	receive := testBase

	var submittedMu sync.Mutex
	submitted := map[int64]bool{}
	var wg sync.WaitGroup

	// Writers: each batch adds 5 new seqs.
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			store, err := OpenStore(dir)
			if err != nil {
				t.Error(err)
				return
			}
			batch := make([]Heartbeat, 5)
			for j := 0; j < 5; j++ {
				seq := int64(w*5 + j + 1)
				batch[j] = hb("n1", seq, receive.Add(-time.Duration(100-seq)*time.Second), "1.0", seq, 0)
			}
			if _, _, err := store.Submit(batch, receive); err != nil {
				t.Error(err)
				return
			}
			submittedMu.Lock()
			for _, r := range batch {
				submitted[r.Seq] = true
			}
			submittedMu.Unlock()
		}(w)
	}

	// Readers: history must always be sorted, duplicate-free, and only
	// contain seqs that have been submitted.
	for r := 0; r < 8; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			store, err := OpenStore(dir)
			if err != nil {
				t.Error(err)
				return
			}
			for i := 0; i < 50; i++ {
				hist, err := store.History("n1")
				if err != nil {
					t.Error(err)
					return
				}
				var prev int64
				seen := map[int64]bool{}
				for _, r := range hist {
					if r.Seq <= prev && prev != 0 {
						t.Errorf("history not sorted: %+v", hist)
						return
					}
					if seen[r.Seq] {
						t.Errorf("duplicate seq in history: %d", r.Seq)
						return
					}
					seen[r.Seq] = true
					prev = r.Seq
					submittedMu.Lock()
					ok := submitted[r.Seq]
					submittedMu.Unlock()
					if !ok {
						t.Errorf("history contains seq %d that was never submitted", r.Seq)
						return
					}
				}
			}
		}()
	}
	wg.Wait()

	// Final state: all 20 records present.
	store, _ := OpenStore(dir)
	hist, err := store.History("n1")
	if err != nil {
		t.Fatal(err)
	}
	if len(hist) != 20 {
		t.Errorf("final history count=%d, want 20", len(hist))
	}
}

func TestPersistenceFailureKeepsPreviousData(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase
	if _, _, err := store.Submit([]Heartbeat{hb("n1", 1, receive.Add(-time.Minute), "1.0", 100, 0)}, receive); err != nil {
		t.Fatal(err)
	}

	// Make the nodes directory unwritable to force a persistence failure.
	nodesDir := filepath.Join(dir, "nodes")
	if err := os.Chmod(nodesDir, 0o500); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(nodesDir, 0o755)

	_, _, err = store.Submit([]Heartbeat{hb("n1", 2, receive.Add(-time.Minute), "1.0", 101, 0)}, receive)
	if err == nil {
		t.Errorf("expected persistence failure")
	}

	// Previous data remains queryable.
	r, err := store.Health("n1", receive, "1.0", 0)
	if err != nil {
		t.Fatalf("previous data not queryable after failure: %v", err)
	}
	if r.Seq != 1 {
		t.Errorf("after failure seq=%d, want 1 (previous data intact)", r.Seq)
	}
}

func TestHeartbeatJSONRoundTrip(t *testing.T) {
	original := hb("n1", 42, testBase.Add(-time.Minute), "1.26.0", 12345, 3)
	data, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	var decoded Heartbeat
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if !decoded.Equal(original) {
		t.Errorf("round trip mismatch: %+v vs %+v", decoded, original)
	}
}

func TestParseHeartbeatsErrorMessageSpecific(t *testing.T) {
	_, err := ParseHeartbeats([]byte(`[{"node":"","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":1,"missed":0}]`), testBase)
	if err == nil || !strings.Contains(err.Error(), "node") {
		t.Errorf("error should mention node: %v", err)
	}
	_, err = ParseHeartbeats([]byte(`[{"node":"n1","seq":0,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":1,"missed":0}]`), testBase)
	if err == nil || !strings.Contains(err.Error(), "seq") {
		t.Errorf("error should mention seq: %v", err)
	}
	_, err = ParseHeartbeats([]byte(`[{"node":"n1","seq":1,"collected_at":"2026-10-01T11:59:00","version":"1.0","height":1,"missed":0}]`), testBase)
	if err == nil || !strings.Contains(err.Error(), "timezone") {
		t.Errorf("error should mention timezone: %v", err)
	}
}

func TestParseHeartbeatsNullErrorMessages(t *testing.T) {
	cases := []struct {
		input string
		field string
	}{
		{`[{"node":null,"seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":1,"missed":0}]`, "node"},
		{`[{"node":"n1","seq":null,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":1,"missed":0}]`, "seq"},
		{`[{"node":"n1","seq":1,"collected_at":null,"version":"1.0","height":1,"missed":0}]`, "collected_at"},
		{`[{"node":"n1","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":null,"height":1,"missed":0}]`, "version"},
		{`[{"node":"n1","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":null,"missed":0}]`, "height"},
		{`[{"node":"n1","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":1,"missed":null}]`, "missed"},
	}
	for _, tc := range cases {
		_, err := ParseHeartbeats([]byte(tc.input), testBase)
		if err == nil {
			t.Errorf("expected error for null %s, got nil", tc.field)
			continue
		}
		msg := err.Error()
		if !strings.Contains(msg, tc.field) {
			t.Errorf("error for null %s should mention field name, got: %v", tc.field, err)
		}
		if !strings.Contains(msg, "null") {
			t.Errorf("error for null %s should mention null, got: %v", tc.field, err)
		}
	}
}

func TestParseHeartbeatsDuplicateErrorMessages(t *testing.T) {
	cases := []struct {
		input string
		field string
	}{
		{`[{"node":"n1","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":1,"missed":0,"missed":0}]`, "missed"},
		{`[{"node":"n1","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":1,"missed":0,"missed":1}]`, "missed"},
		{`[{"node":"n1","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":1,"height":2,"missed":0}]`, "height"},
		{`[{"node":"a","node":"b","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":1,"missed":0}]`, "node"},
		{`[{"node":"n1","seq":1,"seq":2,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":1,"missed":0}]`, "seq"},
		{`[{"node":"n1","seq":1,"collected_at":"2026-10-01T11:59:00Z","collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":1,"missed":0}]`, "collected_at"},
		{`[{"node":"n1","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","version":"2.0","height":1,"missed":0}]`, "version"},
		// Unicode escape variants of the same field name.
		{`[{"node":"n1","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":1,"missed":0,"missed":1}]`, "missed"},
		{`[{"node":"n1","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":1,"missed":0,"missed":1}]`, "missed"},
	}
	for _, tc := range cases {
		_, err := ParseHeartbeats([]byte(tc.input), testBase)
		if err == nil {
			t.Errorf("expected error for duplicate %s, got nil", tc.field)
			continue
		}
		msg := err.Error()
		if !strings.Contains(msg, tc.field) {
			t.Errorf("error for duplicate %s should mention field name, got: %v", tc.field, err)
		}
		if !strings.Contains(msg, "duplicate") {
			t.Errorf("error for duplicate %s should mention duplicate, got: %v", tc.field, err)
		}
	}
}

func TestParseHeartbeatsRecordPosition(t *testing.T) {
	// Error record at index 0 (position 1).
	_, err := ParseHeartbeats([]byte(`[
		{"node":"n1","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":null,"missed":0},
		{"node":"n2","seq":2,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":1,"missed":0}
	]`), testBase)
	if err == nil || !strings.Contains(err.Error(), "record 1") {
		t.Errorf("error should mention record 1, got: %v", err)
	}

	// Error record at index 1 (position 2).
	_, err = ParseHeartbeats([]byte(`[
		{"node":"n1","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":1,"missed":0},
		{"node":"n2","seq":2,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":1,"missed":null}
	]`), testBase)
	if err == nil || !strings.Contains(err.Error(), "record 2") {
		t.Errorf("error should mention record 2, got: %v", err)
	}

	// Error record at index 2 (position 3).
	_, err = ParseHeartbeats([]byte(`[
		{"node":"n1","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":1,"missed":0},
		{"node":"n2","seq":2,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":1,"missed":0},
		{"node":"n3","seq":3,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":1,"missed":0,"missed":1}
	]`), testBase)
	if err == nil || !strings.Contains(err.Error(), "record 3") {
		t.Errorf("error should mention record 3, got: %v", err)
	}
}

func TestParseHeartbeatsUnicodeEscapeAccepted(t *testing.T) {
	// A single field written with a Unicode escape is still the same field
	// and must be accepted.
	input := `[{"node":"n1","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":12345,"missed":0}]`
	records, err := ParseHeartbeats([]byte(input), testBase)
	if err != nil {
		t.Fatalf("single escaped field should be accepted: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	if records[0].Height != 12345 || records[0].Missed != 0 {
		t.Errorf("escaped field value wrong: height=%d missed=%d", records[0].Height, records[0].Missed)
	}
}

func TestParseHeartbeatsLargeIntegerPreserved(t *testing.T) {
	// Large integers must retain their exact value through parsing.
	input := `[{"node":"n1","seq":9223372036854775807,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":9223372036854775806,"missed":9223372036854775805}]`
	records, err := ParseHeartbeats([]byte(input), testBase)
	if err != nil {
		t.Fatalf("large integer should be accepted: %v", err)
	}
	if records[0].Seq != 9223372036854775807 {
		t.Errorf("seq = %d, want 9223372036854775807", records[0].Seq)
	}
	if records[0].Height != 9223372036854775806 {
		t.Errorf("height = %d, want 9223372036854775806", records[0].Height)
	}
	if records[0].Missed != 9223372036854775805 {
		t.Errorf("missed = %d, want 9223372036854775805", records[0].Missed)
	}
}

func TestBatchRejectionOnInputError(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase

	// Pre-existing data for node n1.
	first := hb("n1", 1, receive.Add(-time.Minute), "1.0", 100, 0)
	if _, _, err := store.Submit([]Heartbeat{first}, receive); err != nil {
		t.Fatal(err)
	}

	// A batch with one null record and one valid record: nothing is saved,
	// not even the valid record.
	input := `[
		{"node":"n1","seq":2,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":null,"missed":0},
		{"node":"n2","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":1,"missed":0}
	]`
	records, err := ParseHeartbeats([]byte(input), receive)
	if err == nil {
		t.Fatalf("expected error for null record")
	}
	if records != nil {
		t.Errorf("ParseHeartbeats should return nil records on error, got %v", records)
	}

	// Pre-existing data is unchanged.
	hist, err := store.History("n1")
	if err != nil {
		t.Fatal(err)
	}
	if len(hist) != 1 || hist[0].Seq != 1 {
		t.Errorf("n1 history changed after rejected batch: %+v", hist)
	}

	// New node n2 has no telemetry.
	r, err := store.Health("n2", receive, "1.0", 0)
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != "notelemetry" {
		t.Errorf("n2 status=%s, want notelemetry", r.Status)
	}
}

func TestBatchRejectionOnDuplicateField(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase

	// A batch with one duplicate-field record: nothing is saved.
	input := `[
		{"node":"n1","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":1,"missed":0},
		{"node":"n2","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":1,"missed":0,"missed":1}
	]`
	_, err = ParseHeartbeats([]byte(input), receive)
	if err == nil {
		t.Fatalf("expected error for duplicate field")
	}

	// Neither node has any records.
	for _, node := range []string{"n1", "n2"} {
		hist, err := store.History(node)
		if err != nil {
			t.Fatal(err)
		}
		if len(hist) != 0 {
			t.Errorf("node %s got records despite rejected batch: %+v", node, hist)
		}
	}
}

func TestHealthResultFieldsPopulated(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	collected := testBase
	if _, _, err := store.Submit([]Heartbeat{hb("n1", 7, collected, "1.26.0", 12345, 2)}, collected); err != nil {
		t.Fatal(err)
	}
	r, err := store.Health("n1", collected, "1.26.0", 2)
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != "online" {
		t.Errorf("status=%s, want online", r.Status)
	}
	if r.Seq != 7 || r.Version != "1.26.0" || r.Height != 12345 || r.Missed != 2 {
		t.Errorf("fields wrong: %+v", r)
	}
	if !r.CollectedAt.Equal(collected) {
		t.Errorf("collected_at=%v, want %v", r.CollectedAt, collected)
	}
}

func TestSubmitReturnsErrorOnCorruptExisting(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase
	if _, _, err := store.Submit([]Heartbeat{hb("n1", 1, receive.Add(-time.Minute), "1.0", 100, 0)}, receive); err != nil {
		t.Fatal(err)
	}
	// Corrupt and try to submit a new record for the same node.
	if err := os.WriteFile(store.nodePath("n1"), []byte(`{bad`), 0o644); err != nil {
		t.Fatal(err)
	}
	_, _, err = store.Submit([]Heartbeat{hb("n1", 2, receive.Add(-time.Minute), "1.0", 101, 0)}, receive)
	if err == nil || !IsCorrupt(err) {
		t.Errorf("expected corruption error, got %v", err)
	}
}

func TestNodePathReversible(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	// Node ids with tricky characters still map to a stable path.
	for _, node := range []string{"val-eu-1", "node/with/slashes", "node with spaces", "节点"} {
		path := store.nodePath(node)
		if !strings.HasSuffix(path, ".json") {
			t.Errorf("path for %q not .json: %s", node, path)
		}
		if _, err := os.Stat(filepath.Dir(path)); err != nil {
			t.Errorf("parent dir missing for %q: %v", node, err)
		}
	}
}

func TestValidateHeartbeatErrorMessage(t *testing.T) {
	cases := []struct {
		hb   Heartbeat
		want string
	}{
		{hb("", 1, testBase, "1.0", 1, 0), "node"},
		{hb("n", 0, testBase, "1.0", 1, 0), "seq"},
		{hb("n", 1, testBase, "", 1, 0), "version"},
		{hb("n", 1, testBase, "1.0", -1, 0), "height"},
		{hb("n", 1, testBase, "1.0", 1, -1), "missed"},
	}
	for _, tc := range cases {
		err := ValidateHeartbeat(tc.hb, testBase)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("expected error containing %q, got %v", tc.want, err)
		}
	}
}

func TestSubmitEmptyBatch(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = store.Submit([]Heartbeat{}, testBase)
	if err == nil {
		t.Errorf("expected error for empty batch")
	}
}

func TestHealthAtExactlyQueryTime(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	collected := testBase
	if _, _, err := store.Submit([]Heartbeat{hb("n1", 1, collected, "1.0", 100, 0)}, collected); err != nil {
		t.Fatal(err)
	}
	// Query at the exact collection instant: age 0, online.
	r, err := store.Health("n1", collected, "1.0", 0)
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != "online" {
		t.Errorf("at exact collection time status=%s, want online", r.Status)
	}
}

func TestMain(m *testing.M) {
	// Ensure the lock file path's parent exists for all tests.
	if err := os.MkdirAll(filepath.Join(os.TempDir(), "edgefleet-test"), 0o755); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Exit(m.Run())
}
