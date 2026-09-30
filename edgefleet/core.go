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

// Evaluate checks liveness, version skew and missed duties.
func Evaluate(node Node, expectedVersion string, toleratedMisses int) Health {
	health := Health{Node: node.ID, Healthy: true}
	if !node.Online {
		health.Healthy = false
		health.Findings = append(health.Findings, "offline")
	}
	if node.Version != expectedVersion {
		health.Healthy = false
		health.Findings = append(health.Findings, "version skew: "+node.Version+" != "+expectedVersion)
	}
	if node.Missed > toleratedMisses {
		health.Healthy = false
		health.Findings = append(health.Findings, "missed duties above tolerance")
	}
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
