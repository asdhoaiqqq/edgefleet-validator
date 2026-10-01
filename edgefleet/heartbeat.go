// Package edgefleet implements validator and edge node fleet management.
package edgefleet

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// Heartbeat is one telemetry report from a node. Every field must be supplied
// explicitly: zero values are never interpreted as "not provided".
type Heartbeat struct {
	// NodeID identifies the reporting node; different nodes may reuse seq.
	NodeID string `json:"node_id"`
	// Seq is a positive, node-local counter. It may skip values and arrive
	// out of order; late records are still stored.
	Seq int64 `json:"seq"`
	// CollectedAt is when the node sampled the telemetry, with timezone.
	CollectedAt time.Time `json:"-"`
	// Version is the node software version; it must be non-empty.
	Version string `json:"version"`
	// Height is the observed block height; it may be zero but never negative.
	Height int64 `json:"height"`
	// Missed is the cumulative number of missed duties; non-negative.
	Missed int64 `json:"missed"`

	// ReceivedAt is assigned by the platform when the batch is accepted.
	ReceivedAt time.Time `json:"-"`
}

// heartbeatJSON is the on-wire and on-disk representation. Timestamps are
// RFC3339 texts carrying their offset, and numeric fields use pointers, so a
// present zero ("explicitly supplied") is distinguishable from an absent one.
type heartbeatJSON struct {
	NodeID      string `json:"node_id"`
	Seq         *int64 `json:"seq"`
	CollectedAt string `json:"collected_at"`
	Version     string `json:"version"`
	Height      *int64 `json:"height"`
	Missed      *int64 `json:"missed"`
	ReceivedAt  string `json:"received_at,omitempty"`
}

// MarshalHeartbeat encodes h with explicit RFC3339 timestamps.
func MarshalHeartbeat(h Heartbeat) ([]byte, error) {
	return json.Marshal(h.toJSON())
}

// UnmarshalHeartbeat decodes one heartbeat. An absent field fails rather than
// silently becoming its zero value.
func UnmarshalHeartbeat(data []byte) (Heartbeat, error) {
	var jh heartbeatJSON
	if err := json.Unmarshal(data, &jh); err != nil {
		return Heartbeat{}, fmt.Errorf("invalid JSON: %w", err)
	}
	return hbFromJSON(jh)
}

func (h Heartbeat) toJSON() heartbeatJSON {
	seq, height, missed := h.Seq, h.Height, h.Missed
	jh := heartbeatJSON{
		NodeID:      h.NodeID,
		Seq:         &seq,
		CollectedAt: formatTime(h.CollectedAt),
		Version:     h.Version,
		Height:      &height,
		Missed:      &missed,
	}
	if !h.ReceivedAt.IsZero() {
		jh.ReceivedAt = formatTime(h.ReceivedAt)
	}
	return jh
}

func hbFromJSON(jh heartbeatJSON) (Heartbeat, error) {
	h := Heartbeat{NodeID: jh.NodeID, Version: jh.Version}
	if strings.TrimSpace(jh.NodeID) == "" {
		return h, fmt.Errorf("node_id 不能为空")
	}
	if jh.Seq == nil {
		return h, fmt.Errorf("seq 必须明确提供")
	}
	if *jh.Seq <= 0 {
		return h, fmt.Errorf("seq 必须是大于 0 的整数, 得到 %d", *jh.Seq)
	}
	h.Seq = *jh.Seq
	if strings.TrimSpace(jh.CollectedAt) == "" {
		return h, fmt.Errorf("collected_at 必须明确提供")
	}
	t, err := time.Parse(time.RFC3339Nano, jh.CollectedAt)
	if err != nil {
		return h, fmt.Errorf("collected_at 必须是带时区的 RFC3339 时间 (如 2026-10-01T12:00:00+08:00): %w", err)
	}
	h.CollectedAt = t
	if strings.TrimSpace(jh.Version) == "" {
		return h, fmt.Errorf("version 不能为空")
	}
	if jh.Height == nil {
		return h, fmt.Errorf("height 必须明确提供")
	}
	if *jh.Height < 0 {
		return h, fmt.Errorf("height 不得为负, 得到 %d", *jh.Height)
	}
	h.Height = *jh.Height
	if jh.Missed == nil {
		return h, fmt.Errorf("missed 必须明确提供")
	}
	if *jh.Missed < 0 {
		return h, fmt.Errorf("missed 不得为负, 得到 %d", *jh.Missed)
	}
	h.Missed = *jh.Missed
	if jh.ReceivedAt != "" {
		rt, err := time.Parse(time.RFC3339Nano, jh.ReceivedAt)
		if err != nil {
			return h, fmt.Errorf("received_at 时间格式无效: %w", err)
		}
		h.ReceivedAt = rt
	}
	return h, nil
}

// Validate checks field-level rules against the receive time. Structural
// presence is enforced by UnmarshalHeartbeat; Validate covers the rules that
// depend on the receive clock.
func (h Heartbeat) Validate(receivedAt time.Time) error {
	switch {
	case strings.TrimSpace(h.NodeID) == "":
		return fmt.Errorf("node_id 不能为空")
	case h.Seq <= 0:
		return fmt.Errorf("seq 必须是大于 0 的整数, 得到 %d", h.Seq)
	case h.CollectedAt.IsZero():
		return fmt.Errorf("collected_at 必须明确提供")
	case strings.TrimSpace(h.Version) == "":
		return fmt.Errorf("version 不能为空")
	case h.Height < 0:
		return fmt.Errorf("height 不得为负, 得到 %d", h.Height)
	case h.Missed < 0:
		return fmt.Errorf("missed 不得为负, 得到 %d", h.Missed)
	case h.CollectedAt.After(receivedAt):
		return fmt.Errorf("collected_at %s 不得晚于接收时间 %s",
			formatTime(h.CollectedAt), formatTime(receivedAt))
	}
	return nil
}

// heartbeatEqual implements duplicate identity: same node, same seq and every
// field equal, with collected times compared as absolute instants (so the same
// moment written in different zones still counts as a duplicate).
func heartbeatEqual(a, b Heartbeat) bool {
	return a.NodeID == b.NodeID &&
		a.Seq == b.Seq &&
		a.CollectedAt.Equal(b.CollectedAt) &&
		a.Version == b.Version &&
		a.Height == b.Height &&
		a.Missed == b.Missed
}

// HealthStatus is the result of a telemetry-backed health query.
type HealthStatus struct {
	NodeID string `json:"node_id"`
	// HasTelemetry is false when the node has no heartbeat; callers must not
	// interpret zero-valued telemetry as healthy.
	HasTelemetry bool `json:"has_telemetry"`
	Online       bool `json:"online"`
	Healthy      bool `json:"healthy"`
	// Seq / CollectedAt / Version / Height / Missed describe the record that
	// was actually used (the node's max-seq heartbeat).
	Seq         int64     `json:"seq"`
	CollectedAt time.Time `json:"-"`
	Version     string    `json:"version,omitempty"`
	Height      int64     `json:"height"`
	Missed      int64     `json:"missed"`
	// Findings carries abnormal reasons, mirroring Evaluate wording.
	Findings []string `json:"findings,omitempty"`
	// Reason is a machine-readable short code:
	// "no-telemetry", "query-before-telemetry", "offline" or "".
	Reason string `json:"reason,omitempty"`
}

// HealthStatusJSON is the wire form with an explicit timestamp.
type HealthStatusJSON struct {
	NodeID       string   `json:"node_id"`
	HasTelemetry bool     `json:"has_telemetry"`
	Online       bool     `json:"online"`
	Healthy      bool     `json:"healthy"`
	Seq          int64    `json:"seq"`
	CollectedAt  string   `json:"collected_at,omitempty"`
	Version      string   `json:"version,omitempty"`
	Height       int64    `json:"height"`
	Missed       int64    `json:"missed"`
	Findings     []string `json:"findings,omitempty"`
	Reason       string   `json:"reason,omitempty"`
}

// JSON renders the status for CLI output.
func (s HealthStatus) JSON() HealthStatusJSON {
	j := HealthStatusJSON{
		NodeID:       s.NodeID,
		HasTelemetry: s.HasTelemetry,
		Online:       s.Online,
		Healthy:      s.Healthy,
		Seq:          s.Seq,
		Version:      s.Version,
		Height:       s.Height,
		Missed:       s.Missed,
		Findings:     s.Findings,
		Reason:       s.Reason,
	}
	if s.HasTelemetry {
		j.CollectedAt = formatTime(s.CollectedAt)
	}
	return j
}

const onlineWindow = 60 * time.Second

// QueryHealth derives health from the node's current telemetry (its max-seq
// record). expectedVersion and toleratedMisses reuse the Evaluate rules.
//
// The latest record is chosen by seq, not by arrival time, so a freshly
// back-filled stale heartbeat cannot make old telemetry look healthy.
func QueryHealth(latest *Heartbeat, queryAt time.Time, expectedVersion string, toleratedMisses int64) (HealthStatus, error) {
	if latest == nil {
		return HealthStatus{
			HasTelemetry: false,
			Healthy:      false,
			Reason:       "no-telemetry",
			Findings:     []string{"无遥测"},
		}, nil
	}
	st := HealthStatus{
		NodeID:       latest.NodeID,
		HasTelemetry: true,
		Healthy:      true,
		Seq:          latest.Seq,
		CollectedAt:  latest.CollectedAt,
		Version:      latest.Version,
		Height:       latest.Height,
		Missed:       latest.Missed,
	}
	if queryAt.Before(latest.CollectedAt) {
		st.Healthy = false
		st.Reason = "query-before-telemetry"
		st.Findings = []string{"查询时间早于最新采集时间"}
		return st, fmt.Errorf("查询时间 %s 早于该节点最新采集时间 %s",
			formatTime(queryAt), formatTime(latest.CollectedAt))
	}
	if age := queryAt.Sub(latest.CollectedAt); age <= onlineWindow {
		st.Online = true
	} else {
		st.Healthy = false
		st.Reason = "offline"
		st.Findings = append(st.Findings, "offline")
	}
	if latest.Version != expectedVersion {
		st.Healthy = false
		st.Findings = append(st.Findings, "version skew: "+latest.Version+" != "+expectedVersion)
	}
	if latest.Missed > toleratedMisses {
		st.Healthy = false
		st.Findings = append(st.Findings, "missed duties above tolerance")
	}
	return st, nil
}

// formatTime renders t with its zone for human and machine output.
func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format(time.RFC3339Nano)
}
