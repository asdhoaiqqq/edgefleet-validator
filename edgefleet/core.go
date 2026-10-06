// Package edgefleet implements validator and edge node fleet management.
package edgefleet

import "sort"

// Node is one validator or edge device under management.
type Node struct {
	ID      string
	Region  string
	Version string
	Height  int64
	Missed  int
	Online  bool
}

// Health summarises a node against the fleet expectations.
type Health struct {
	Node     string
	Healthy  bool
	Findings []string
}

// missedAboveToleranceFinding is the single alarm text emitted when a
// missed-duty count strictly exceeds the tolerance. The counted quantity
// differs per query path — the latest cumulative counter for Evaluate and
// plain health queries, the baseline-relative increase for HealthSince — but
// the alarm wording is shared.
const missedAboveToleranceFinding = "missed duties above tolerance"

// livenessVersionFindings holds the single online and version-skew judgement
// shared by Evaluate and both store health query paths: online reflects only
// whether the latest telemetry is fresh (a version skew or missed-duty excess
// never makes a node offline), and the version is compared as the exact text
// the node reported, with no format imposed and no trimming or case folding.
// Findings come back in the canonical order: offline first, then version skew.
// The skew text shows both versions through the shared DisplayText rule, so a
// version containing newlines, tabs or control characters cannot split the
// finding or corrupt the terminal; the comparison itself still uses the
// original text.
func livenessVersionFindings(online bool, version, expectedVersion string) []string {
	var findings []string
	if !online {
		findings = append(findings, "offline")
	}
	if version != expectedVersion {
		findings = append(findings, "version skew: "+DisplayText(version)+" != "+DisplayText(expectedVersion))
	}
	return findings
}

// missedExceedsTolerance is the single missed-duty alarm rule shared by
// Evaluate and both store health query paths: the count alarms only when it
// strictly exceeds the tolerance; equality does not. The comparison is done
// in int64 so a cumulative counter beyond the int range (e.g. above 2^31-1
// on 32-bit platforms, where Node.Missed cannot even hold it) is judged by
// the full value the node reported, never a truncated or sign-flipped one.
func missedExceedsTolerance(missed int64, toleratedMisses int) bool {
	return missed > int64(toleratedMisses)
}

// Evaluate checks liveness, version skew and missed duties.
func Evaluate(node Node, expectedVersion string, toleratedMisses int) Health {
	health := Health{Node: node.ID, Healthy: true}
	health.Findings = livenessVersionFindings(node.Online, node.Version, expectedVersion)
	if missedExceedsTolerance(int64(node.Missed), toleratedMisses) {
		health.Findings = append(health.Findings, missedAboveToleranceFinding)
	}
	health.Healthy = len(health.Findings) == 0
	return health
}

// Plan returns the rollout order: lagging nodes first, stable by node id.
func Plan(nodes []Node, target string) []string {
	var lagging, current []string
	for _, node := range nodes {
		if node.Version == target {
			current = append(current, node.ID)
		} else {
			lagging = append(lagging, node.ID)
		}
	}
	sort.Strings(lagging)
	sort.Strings(current)
	return append(lagging, current...)
}
