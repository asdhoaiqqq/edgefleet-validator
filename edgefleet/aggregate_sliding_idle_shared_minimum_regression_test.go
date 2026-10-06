package edgefleet

import (
	"strings"
	"testing"
)

// Shared fixture for the shared-minimum idle regression: window 1000, slide
// 600, three partitions, one key. Partition 0's event at 700 lands in
// [0,1000) and [600,1600); partition 1's event at 1000 lands only in
// [600,1600); partition 2's event at 1300 lands in [600,1600) and
// [1200,2200). After the watermarks 1600/600/600 the effective watermark is
// 600, so nothing closes: partitions 1 and 2 both hold the minimum, and only
// the last of them declaring idle may let the effective watermark pass it.
func slidingIdleSharedMinFixtureLines() []string {
	return []string{
		`{"type":"event","key":"k","time":700,"value":2,"partition":0}`,
		`{"type":"event","key":"k","time":1000,"value":3,"partition":1}`,
		`{"type":"event","key":"k","time":1300,"value":5,"partition":2}`,
		`{"type":"watermark","time":1600,"partition":0}`,
		`{"type":"watermark","time":600,"partition":1}`,
		`{"type":"watermark","time":600,"partition":2}`, // effective = 600: nothing closes
	}
}

// slidingIdleSharedMinOutput is the batch emitted once both minimum-holding
// partitions are idle and partition 0's 1600 becomes effective: [0,1000)
// keeps only the event at 700, while [600,1600) merges all four events
// (2+3+5+4 = 14), including the two idle partitions' pre-idle contributions
// and the event the still-active slow partition sent after the first idle
// declaration. The results carry no partition field. [1200,2200) holds the
// event at 1300 but stays open at effective watermark 1600 and is not
// flushed at end of input.
var slidingIdleSharedMinOutput = []string{
	`{"key":"k","start":0,"end":1000,"count":1,"sum":2}`,
	`{"key":"k","start":600,"end":1600,"count":4,"sum":14}`,
}

// Two active partitions hold the same minimum watermark 600. When partition 1
// declares idle, partition 2 still pins the effective watermark at 600, so
// nothing closes; partition 2's later event at 1000 is then judged against
// that effective 600 -- not against partition 0's higher 1600 -- and joins
// [600,1600) normally instead of being dropped as late or lost because
// another partition is idle. Only when partition 2 also declares idle does
// partition 0's 1600 become effective and close [0,1000) and [600,1600).
func TestAggregateSlidingIdleSharedMinimumLastIdleCloses(t *testing.T) {
	// Up to and including the still-active slow partition's post-idle event
	// nothing may be emitted: the effective watermark never left 600.
	prefix := append(slidingIdleSharedMinFixtureLines(),
		`{"type":"idle","partition":1}`, // effective still 600: p2 holds the minimum too
		`{"type":"event","key":"k","time":1000,"value":4,"partition":2}`, // counted in [600,1600), not late
	)
	stdout, stderr, err := runPartitionedSliding(t, strings.Join(prefix, "\n"), 1000, 600, 3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stdout != "" {
		t.Fatalf("no window may close while partition 2 still holds the minimum, got %q", stdout)
	}
	if stderr != "" {
		t.Fatalf("the post-idle event at 1000 is not late against effective watermark 600, got %q", stderr)
	}

	lines := append(prefix,
		`{"type":"idle","partition":2}`, // last minimum holder idles: effective = 1600
	)
	stdout, stderr, err = runPartitionedSliding(t, strings.Join(lines, "\n"), 1000, 600, 3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stderr != "" {
		t.Fatalf("unexpected stderr: %q", stderr)
	}
	want := strings.Join(slidingIdleSharedMinOutput, "\n") + "\n"
	if stdout != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, want)
	}
}

// The rule is symmetric: with partitions 1 and 2 both holding 600, either one
// may be the first to declare idle. Here partition 2 idles first and the
// still-active partition 1 sends the post-idle event at 1000; the outcome is
// identical -- no output before the last minimum holder idles, then the same
// two merged windows.
func TestAggregateSlidingIdleSharedMinimumSymmetric(t *testing.T) {
	lines := append(slidingIdleSharedMinFixtureLines(),
		`{"type":"idle","partition":2}`, // effective still 600: p1 holds the minimum too
		`{"type":"event","key":"k","time":1000,"value":4,"partition":1}`, // counted in [600,1600), not late
		`{"type":"idle","partition":1}`,                                  // last minimum holder idles: effective = 1600
	)
	stdout, stderr, err := runPartitionedSliding(t, strings.Join(lines, "\n"), 1000, 600, 3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stderr != "" {
		t.Fatalf("unexpected stderr: %q", stderr)
	}
	want := strings.Join(slidingIdleSharedMinOutput, "\n") + "\n"
	if stdout != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, want)
	}
}

// Repeating an idle declaration for an already idle partition is a no-op: it
// neither advances the effective watermark nor emits anything, so the other
// still-active minimum holder keeps blocking window closure, and once the
// windows do close a further repeated declaration cannot re-emit or
// duplicate the results.
func TestAggregateSlidingIdleSharedMinimumRepeatedIdleNoop(t *testing.T) {
	// Repeated idles of partition 1 while partition 2 is still active: the
	// effective watermark stays 600 and nothing is emitted.
	prefix := append(slidingIdleSharedMinFixtureLines(),
		`{"type":"idle","partition":1}`,
		`{"type":"idle","partition":1}`, // no-op: p2 still pins the effective watermark at 600
		`{"type":"idle","partition":1}`, // no-op again
	)
	stdout, stderr, err := runPartitionedSliding(t, strings.Join(prefix, "\n"), 1000, 600, 3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stdout != "" {
		t.Fatalf("repeated idle declarations must not close anything while partition 2 is active, got %q", stdout)
	}
	if stderr != "" {
		t.Fatalf("unexpected stderr: %q", stderr)
	}

	lines := append(prefix,
		`{"type":"event","key":"k","time":1000,"value":4,"partition":2}`,
		`{"type":"idle","partition":2}`, // last minimum holder idles: effective = 1600
		`{"type":"idle","partition":2}`, // no-op after closure: no re-emit, no duplicate
		`{"type":"idle","partition":1}`, // still a no-op
	)
	stdout, stderr, err = runPartitionedSliding(t, strings.Join(lines, "\n"), 1000, 600, 3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stderr != "" {
		t.Fatalf("unexpected stderr: %q", stderr)
	}
	want := strings.Join(slidingIdleSharedMinOutput, "\n") + "\n"
	if stdout != want {
		t.Fatalf("stdout mismatch (each window exactly once, no duplicates):\n got: %q\nwant: %q", stdout, want)
	}
}
