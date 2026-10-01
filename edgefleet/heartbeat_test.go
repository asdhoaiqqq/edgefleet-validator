package edgefleet

import (
	"strings"
	"testing"
	"time"
)

func mustTime(t *testing.T, s string) time.Time {
	t.Helper()
	tt, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		t.Fatalf("bad time %q: %v", s, err)
	}
	return tt
}

func parseTimeOrPanic(s string) time.Time {
	tt, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		panic("bad time " + s + ": " + err.Error())
	}
	return tt
}

func hb(node string, seq int64, collected string, version string, height, missed int64) Heartbeat {
	return Heartbeat{
		NodeID:      node,
		Seq:         seq,
		CollectedAt: parseTimeOrPanic(collected),
		Version:     version,
		Height:      height,
		Missed:      missed,
	}
}

func TestUnmarshalHeartbeatRequiresExplicitFields(t *testing.T) {
	base := `{"node_id":"n1","seq":1,"collected_at":"2026-10-01T12:00:00+08:00","version":"1.26.0","height":10,"missed":0}`
	if _, err := UnmarshalHeartbeat([]byte(base)); err != nil {
		t.Fatalf("valid record rejected: %v", err)
	}
	cases := []struct {
		name string
		json string
		want string
	}{
		{"missing node", `{"seq":1,"collected_at":"2026-10-01T12:00:00+08:00","version":"v","height":1,"missed":0}`, "node_id"},
		{"missing seq", `{"node_id":"n1","collected_at":"2026-10-01T12:00:00+08:00","version":"v","height":1,"missed":0}`, "seq"},
		{"zero seq", `{"node_id":"n1","seq":0,"collected_at":"2026-10-01T12:00:00+08:00","version":"v","height":1,"missed":0}`, "seq"},
		{"negative seq", `{"node_id":"n1","seq":-2,"collected_at":"2026-10-01T12:00:00+08:00","version":"v","height":1,"missed":0}`, "seq"},
		{"missing collected_at", `{"node_id":"n1","seq":1,"version":"v","height":1,"missed":0}`, "collected_at"},
		{"offset-less time", `{"node_id":"n1","seq":1,"collected_at":"2026-10-01T12:00:00","version":"v","height":1,"missed":0}`, "collected_at"},
		{"missing version", `{"node_id":"n1","seq":1,"collected_at":"2026-10-01T12:00:00+08:00","version":"","height":1,"missed":0}`, "version"},
		{"missing height", `{"node_id":"n1","seq":1,"collected_at":"2026-10-01T12:00:00+08:00","version":"v","missed":0}`, "height"},
		{"negative height", `{"node_id":"n1","seq":1,"collected_at":"2026-10-01T12:00:00+08:00","version":"v","height":-1,"missed":0}`, "height"},
		{"explicit zero height ok structurally", `{"node_id":"n1","seq":1,"collected_at":"2026-10-01T12:00:00+08:00","version":"v","height":0,"missed":0}`, ""},
		{"missing missed", `{"node_id":"n1","seq":1,"collected_at":"2026-10-01T12:00:00+08:00","version":"v","height":1}`, "missed"},
		{"negative missed", `{"node_id":"n1","seq":1,"collected_at":"2026-10-01T12:00:00+08:00","version":"v","height":1,"missed":-3}`, "missed"},
		{"malformed json", `{not json`, "invalid JSON"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := UnmarshalHeartbeat([]byte(tc.json))
			if tc.want == "" {
				if err != nil {
					t.Fatalf("expected ok, got %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected error containing %q, got %v", tc.want, err)
			}
		})
	}
}

func TestValidateCollectedNotAfterReceived(t *testing.T) {
	h := hb("n1", 1, "2026-10-01T12:00:00+08:00", "v", 1, 0)
	if err := h.Validate(mustTime(t, "2026-10-01T12:00:00+08:00")); err != nil {
		t.Fatalf("equal instant should pass: %v", err)
	}
	if err := h.Validate(mustTime(t, "2026-10-01T11:59:59+08:00")); err == nil {
		t.Fatal("collected after received must fail")
	}
}

func TestHeartbeatEqualAcrossZones(t *testing.T) {
	a := hb("n1", 1, "2026-10-01T12:00:00+08:00", "v", 10, 2)
	b := hb("n1", 1, "2026-10-01T04:00:00Z", "v", 10, 2)
	c := hb("n1", 1, "2026-10-01T12:00:01+08:00", "v", 10, 2)
	if !heartbeatEqual(a, b) {
		t.Error("same instant in different zones must be equal")
	}
	if heartbeatEqual(a, c) {
		t.Error("different instants must not be equal")
	}
}

func TestQueryHealthNoTelemetry(t *testing.T) {
	st, err := QueryHealth(nil, mustTime(t, "2026-10-01T12:00:00+08:00"), "v", 0)
	if err != nil {
		t.Fatal(err)
	}
	if st.HasTelemetry || st.Healthy || st.Reason != "no-telemetry" {
		t.Fatalf("want explicit no-telemetry, got %+v", st)
	}
	if len(st.Findings) == 0 || st.Findings[0] != "无遥测" {
		t.Fatalf("want 无遥测 finding, got %v", st.Findings)
	}
}

func TestQueryHealthOnlineBoundary(t *testing.T) {
	latest := hb("n1", 1, "2026-10-01T12:00:00+08:00", "v", 1, 0)
	cases := []struct {
		name   string
		at     string
		online bool
		err    bool
	}{
		{"same instant", "2026-10-01T12:00:00+08:00", true, false},
		{"59s", "2026-10-01T12:00:59+08:00", true, false},
		{"exactly 60s still online", "2026-10-01T12:01:00+08:00", true, false},
		{"61s offline", "2026-10-01T12:01:01+08:00", false, false},
		{"query before collected", "2026-10-01T11:59:59+08:00", false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st, err := QueryHealth(&latest, mustTime(t, tc.at), "v", 0)
			if tc.err {
				if err == nil || st.Reason != "query-before-telemetry" {
					t.Fatalf("want query-before error, got st=%+v err=%v", st, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if st.Online != tc.online {
				t.Fatalf("online=%v want %v (findings=%v)", st.Online, tc.online, st.Findings)
			}
			if tc.online && !st.Healthy {
				t.Fatalf("matching record should be healthy, got %v", st.Findings)
			}
		})
	}
}

func TestQueryHealthVersionAndMissedRules(t *testing.T) {
	latest := hb("n1", 7, "2026-10-01T12:00:00+08:00", "1.25.0", 1, 2)
	st, err := QueryHealth(&latest, mustTime(t, "2026-10-01T12:00:00+08:00"), "1.26.0", 2)
	if err != nil {
		t.Fatal(err)
	}
	if st.Seq != 7 || st.Healthy {
		t.Fatalf("want unhealthy seq 7, got %+v", st)
	}
	if !contains(st.Findings, "version skew: 1.25.0 != 1.26.0") {
		t.Fatalf("missing version finding: %v", st.Findings)
	}
	if contains(st.Findings, "missed duties above tolerance") {
		t.Fatalf("missed == tolerance must pass: %v", st.Findings)
	}

	// missed just above tolerance flips unhealthy with the finding.
	latest.Missed = 3
	st, _ = QueryHealth(&latest, mustTime(t, "2026-10-01T12:00:00+08:00"), "1.26.0", 2)
	if !contains(st.Findings, "missed duties above tolerance") {
		t.Fatalf("missing tolerance finding: %v", st.Findings)
	}
}

func TestQueryHealthFreshBackfillDoesNotReviveStaleTelemetry(t *testing.T) {
	// Record sampled long ago but received only now: receive freshness must
	// not count; the telemetry itself is stale.
	latest := hb("n1", 1, "2026-10-01T12:00:00+08:00", "v", 1, 0)
	st, err := QueryHealth(&latest, mustTime(t, "2026-10-01T12:05:00+08:00"), "v", 0)
	if err != nil {
		t.Fatal(err)
	}
	if st.Online || st.Healthy || st.Reason != "offline" {
		t.Fatalf("stale record must be offline even if just received: %+v", st)
	}
}

func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}
