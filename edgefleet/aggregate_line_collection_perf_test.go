package edgefleet

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"
)

// This file guards the cost of collecting physical lines, not their results.
// The result contract for long records split across arbitrary reads lives in
// aggregate_long_record_test.go and aggregate_chunked_input_test.go. What is
// pinned here is the complexity: when a legal record with a very long key
// arrives one or a few bytes per Read, line collection must cost about one
// newline scan per input byte -- linear in the cumulative input byte count
// and read count -- rather than one scan per Read over the whole accumulated
// tail, which is quadratic in the line length.

// singleByteCuts lists a cut before every byte except the first, the schedule
// that made the old rescanner quadratic.
func singleByteCuts(n int) []int {
	cuts := make([]int, 0, n-1)
	for at := 1; at < n; at++ {
		cuts = append(cuts, at)
	}
	return cuts
}

// bestSingleByteTime measures repeated one-byte-per-Read passes in growing
// batches until a batch spans at least minBatch, returning the best
// (minimum-noise) average per pass. Comparing two streams of the same byte
// length under the same schedule makes the ratio independent of the
// machine's absolute speed.
func bestSingleByteTime(t *testing.T, input string, minBatch time.Duration) time.Duration {
	t.Helper()
	cuts := singleByteCuts(len(input))
	iters := 1
	best := time.Duration(1<<63 - 1)
	for attempt := 0; attempt < 6; attempt++ {
		begin := time.Now()
		for i := 0; i < iters; i++ {
			var out, late bytes.Buffer
			if err := RunAggregate(newChunkReader(input, cuts...), 1000, &out, &late); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		}
		batch := time.Since(begin)
		if avg := batch / time.Duration(iters); avg < best {
			best = avg
		}
		if batch >= minBatch {
			return best
		}
		// Run long enough to rise above scheduler noise, then try again.
		iters *= 4
	}
	return best
}

// TestAggregateLineCollectionCostFollowsBytesNotLineLength is the complexity
// guard. Two inputs have essentially the SAME total byte count and arrive
// through the SAME number of one-byte Reads; they differ only in whether the
// bytes form one very long physical record or thousands of short ones. A
// linear collector scans every byte about once either way (plus read-call
// overhead, identical per byte), so the two timings are comparable. The old
// collector rescanned the whole pending tail after every Read, i.e. about
// L*L/2 comparisons for one L-byte line versus only N*m/2 for N bytes split
// into m-byte lines, making the long-line input hundreds of times slower
// despite the identical byte and read counts.
func TestAggregateLineCollectionCostFollowsBytesNotLineLength(t *testing.T) {
	if testing.Short() {
		t.Skip("timing guard skipped in -short mode")
	}
	const target = 200 * 1024

	// One long event record (long multibyte key) closed by a watermark.
	longKey := longKey(longKeySuffixA, target)
	oneLongLine := strings.Join([]string{
		fmt.Sprintf(`{"type":"event","key":%q,"time":100,"value":7}`, longKey),
		`{"type":"watermark","time":1000}`,
		``,
	}, "\n")

	// The same number of bytes as many short newline-terminated records, all
	// merging into one short-key window, closed by a watermark.
	shortRecord := `{"type":"event","key":"短","time":100,"value":1}` + "\n"
	var b strings.Builder
	for b.Len()+len(shortRecord) < target {
		b.WriteString(shortRecord)
	}
	b.WriteString(`{"type":"watermark","time":1000}` + "\n")
	manyShortLines := b.String()

	if len(oneLongLine) < target {
		t.Fatalf("test setup: long input is only %d bytes, want >= %d", len(oneLongLine), target)
	}
	// The two streams must be genuinely the same order of size; otherwise the
	// ratio would measure byte count rather than line layout.
	if got := len(oneLongLine); got > target*2 || got < target {
		t.Fatalf("test setup: long input %d bytes vs short input %d bytes", got, len(manyShortLines))
	}

	// The timed long-line delivery must also stay correct: the whole key
	// survives one-byte arrival and the watermark closes its window.
	var out, late bytes.Buffer
	if err := RunAggregate(newChunkReader(oneLongLine, singleByteCuts(len(oneLongLine))...), 1000, &out, &late); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	wantWindow := fmt.Sprintf(`{"key":%q,"start":0,"end":1000,"count":1,"sum":7}`+"\n", longKey)
	if out.String() != wantWindow {
		t.Fatalf("single-byte long-line output mismatch:\n got: %q\nwant: %q", out.String(), wantWindow)
	}
	if late.String() != "" {
		t.Fatalf("unexpected late notices: %q", late.String())
	}

	longAvg := bestSingleByteTime(t, oneLongLine, 30*time.Millisecond)
	shortAvg := bestSingleByteTime(t, manyShortLines, 30*time.Millisecond)

	// Linear collection: comparable cost for equal bytes and equal reads.
	// The short stream parses thousands of records (more callbacks), the long
	// stream copies/grows one large buffer (more copies), so either may win;
	// allow a generous factor either way. The quadratic rescanner made the
	// long stream hundreds of times slower.
	const factor = 6.0
	if ratio := float64(longAvg) / float64(shortAvg); ratio > factor {
		t.Fatalf("line collection cost grows with line length at fixed byte/read count: "+
			"one %d-byte line took %v/pass, %d bytes of short lines took %v/pass "+
			"(long/short ratio %.2f, want <= %.1f)",
			len(oneLongLine), longAvg, len(manyShortLines), shortAvg, ratio, factor)
	}
	// Absolute backstop: ~200 KiB delivered one byte at a time must finish
	// promptly with a linear collector, even on a slow, contended machine.
	if longAvg > 2*time.Second {
		t.Fatalf("single-byte processing of %d bytes took %v/pass, want under 2s", len(oneLongLine), longAvg)
	}
}

// TestAggregateLineCollectionAlternatingChunksLinear interleaves long and
// short records and alternates the read size between one byte and a few
// kilobytes, so the cursor that resumes the newline search must repeatedly
// skip already-searched long tails and then find newlines inside freshly
// appended bytes. Output must equal the one-shot reference; this guards
// correctness of the resume cursor under mixed chunking, the timing ratio is
// covered by the test above.
func TestAggregateLineCollectionAlternatingChunksLinear(t *testing.T) {
	keyA := longKey(longKeySuffixA, 64*1024)
	keyB := longKey(longKeySuffixB, 64*1024)
	lines := []string{
		``,
		fmt.Sprintf(`{"type":"event","key":%q,"time":100,"value":2}`, "短"),
		fmt.Sprintf(`{"type":"event","key":%q,"time":100,"value":10}`, keyA),
		`{"type":"watermark","time":500}`,
		fmt.Sprintf(`{"type":"event","key":%q,"time":100,"value":7}`, keyB),
		``,
		fmt.Sprintf(`{"type":"event","key":%q,"time":200,"value":4}`, "短"),
		`{"type":"watermark","time":1000}`,
	}
	input := strings.Join(lines, "\n") + "\n"

	var refOut, refLate bytes.Buffer
	if err := RunAggregate(strings.NewReader(input), 1000, &refOut, &refLate); err != nil {
		t.Fatalf("one-shot reference failed: %v", err)
	}

	// Alternating one-byte and 4 KiB cuts.
	var cuts []int
	pos := 0
	small := true
	for pos < len(input) {
		step := 1
		if !small {
			step = 4096
		}
		small = !small
		pos += step
		if pos < len(input) {
			cuts = append(cuts, pos)
		}
	}

	var out, late bytes.Buffer
	if err := RunAggregate(newChunkReader(input, cuts...), 1000, &out, &late); err != nil {
		t.Fatalf("alternating-chunk run failed: %v", err)
	}
	if out.String() != refOut.String() {
		t.Fatalf("alternating chunks changed the window output:\n got: %q\nwant: %q", out.String(), refOut.String())
	}
	if late.String() != refLate.String() {
		t.Fatalf("alternating chunks changed the late output:\n got: %q\nwant: %q", late.String(), refLate.String())
	}
}
