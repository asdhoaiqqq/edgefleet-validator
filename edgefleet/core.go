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

// missedToleranceFinding is the single alarm wording emitted whenever a
// missed-duty count — the cumulative counter in a plain judgement, or the
// newly missed count in a baseline judgement — strictly exceeds the
// tolerated number of misses.
const missedToleranceFinding = "missed duties above tolerance"

// presenceFindings holds the single online/version rule set shared by
// Evaluate and both store health queries (plain and baseline): the online
// flag says only whether the latest telemetry is fresh, and the version is
// compared as the exact text the node reported, with no trimming, case
// folding or format rules. The findings are ordered offline first, then
// version skew, so the missed-duty finding of either query path always
// appends after them.
func presenceFindings(online bool, version, expectedVersion string) []string {
	var findings []string
	if !online {
		findings = append(findings, "offline")
	}
	if version != expectedVersion {
		findings = append(findings, "version skew: "+version+" != "+expectedVersion)
	}
	return findings
}

// Evaluate checks liveness, version skew and missed duties.
func Evaluate(node Node, expectedVersion string, toleratedMisses int) Health {
	health := Health{Node: node.ID, Healthy: true}
	health.Findings = presenceFindings(node.Online, node.Version, expectedVersion)
	if node.Missed > toleratedMisses {
		health.Findings = append(health.Findings, missedToleranceFinding)
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
