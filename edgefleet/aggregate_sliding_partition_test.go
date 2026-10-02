package edgefleet

import (
	"bytes"
	"strings"
	"testing"
)

func runPartitionedSliding(t *testing.T, input string, window, slide, partitions int64) (string, string, error) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	err := RunAggregatePartitionedSliding(strings.NewReader(input), window, slide, partitions, &stdout, &stderr)
	return stdout.String(), stderr.String(), err
}

// Sliding windows combine with partitions: the same key from different
// partitions merges within each overlapping window, and only the effective
// (minimum) watermark closes them.
func TestAggregatePartitionedSlidingMergesPerWindow(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":700,"value":2,"partition":0}`,
		`{"type":"event","key":"k","time":1000,"value":3,"partition":1}`,
		`{"type":"watermark","time":5000,"partition":0}`, // ahead: effective still unknown
		`{"type":"watermark","time":1000,"partition":1}`, // min = 1000: closes only [0,1000)
		`{"type":"watermark","time":5000,"partition":0}`, // p0 repeats; min stays 1000
		`{"type":"watermark","time":1600,"partition":1}`, // min = 1600: closes [600,1600)
	}, "\n")
	stdout, stderr, err := runPartitionedSliding(t, input, 1000, 600, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stderr != "" {
		t.Fatalf("unexpected stderr: %q", stderr)
	}
	want := strings.Join([]string{
		`{"key":"k","start":0,"end":1000,"count":1,"sum":2}`,
		`{"key":"k","start":600,"end":1600,"count":2,"sum":5}`,
		``,
	}, "\n")
	if stdout != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, want)
	}
}

// The late-event check uses the effective watermark: an event below the min
// is skipped entirely even while an overlapping window covering its time is
// still open.
func TestAggregatePartitionedSlidingLateUsesEffectiveWatermark(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"watermark","time":5000,"partition":0}`,
		`{"type":"watermark","time":1000,"partition":1}`,                // effective = 1000
		`{"type":"event","key":"k","time":999,"value":1,"partition":0}`, // late despite open [600,1600)
		`{"type":"event","key":"k","time":1000,"value":4,"partition":0}`,
		`{"type":"watermark","time":1600,"partition":1}`,
	}, "\n")
	stdout, stderr, err := runPartitionedSliding(t, input, 1000, 600, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := `{"key":"k","start":600,"end":1600,"count":1,"sum":4}` + "\n"
	if stdout != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, want)
	}
	wantErr := "line 3: late event time=999 below current watermark 1000, skipped\n"
	if stderr != wantErr {
		t.Fatalf("stderr = %q, want %q", stderr, wantErr)
	}
}

// Before the effective watermark exists nothing closes and nothing is late;
// an idle declaration can itself close overlapping windows.
func TestAggregatePartitionedSlidingIdleCloses(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":700,"value":2,"partition":0}`,
		`{"type":"event","key":"k","time":1000,"value":3,"partition":0}`,
		`{"type":"watermark","time":1600,"partition":0}`,
		`{"type":"idle","partition":1}`, // effective = 1600: [0,1000) and [600,1600) close
	}, "\n")
	stdout, _, err := runPartitionedSliding(t, input, 1000, 600, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := strings.Join([]string{
		`{"key":"k","start":0,"end":1000,"count":1,"sum":2}`,
		`{"key":"k","start":600,"end":1600,"count":2,"sum":5}`,
		``,
	}, "\n")
	if stdout != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, want)
	}
}

// Existing partition validation still applies with sliding windows.
func TestAggregatePartitionedSlidingInvalidPartitionAndArgs(t *testing.T) {
	bad := `{"type":"event","key":"k","time":700,"value":1,"partition":9}` + "\n"
	_, _, err := runPartitionedSliding(t, bad, 1000, 600, 2)
	if err == nil {
		t.Fatal("expected out-of-range partition error")
	}
	inputErr, ok := err.(*InputError)
	if !ok {
		t.Fatalf("expected *InputError, got %T: %v", err, err)
	}
	if !strings.Contains(inputErr.Reason, "[0,2)") {
		t.Fatalf("reason = %q, want partition range", inputErr.Reason)
	}

	for _, sl := range []int64{0, -1, 1001} {
		if err := RunAggregatePartitionedSliding(strings.NewReader(""), 1000, sl, 2, &bytes.Buffer{}, &bytes.Buffer{}); err == nil {
			t.Fatalf("expected error for slide %d", sl)
		}
	}
}
