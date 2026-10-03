package edgefleet

import (
	"strings"
	"testing"
)

// Sliding windows with an idle-then-resumed partition, end to end: idling
// partition 1 only removes it from the effective-watermark minimum, so the
// still-open overlapping window [600,1600) keeps both partitions' earlier
// contributions. Resuming at the current effective watermark succeeds and
// re-emits nothing; the resumed partition's new events land in the open
// window, which closes only once both partitions reach its end.
func TestAggregatePartitionedSlidingIdleResumeFullCycle(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":700,"value":2,"partition":0}`,
		`{"type":"event","key":"k","time":1000,"value":3,"partition":1}`,
		`{"type":"watermark","time":1000,"partition":0}`,                 // p1 unreported: no effective watermark
		`{"type":"watermark","time":600,"partition":1}`,                  // effective = 600: nothing closes
		`{"type":"idle","partition":1}`,                                  // effective = 1000: closes [0,1000) only
		`{"type":"watermark","time":1000,"partition":1}`,                 // resume at the effective boundary: no re-emit
		`{"type":"event","key":"k","time":1000,"value":4,"partition":1}`, // joins still-open [600,1600)
		`{"type":"watermark","time":1600,"partition":0}`,                 // min(1600,1000)=1000: [600,1600) stays open
		`{"type":"watermark","time":1600,"partition":1}`,                 // effective = 1600: closes [600,1600)
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
		`{"key":"k","start":600,"end":1600,"count":3,"sum":9}`,
		``,
	}, "\n")
	if stdout != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, want)
	}
}

// A failed resume is fatal with the physical line number (blank lines count)
// and a reason naming the violated bound; output produced before the error is
// retained and records after the erroring line never produce results.
func TestAggregatePartitionedSlidingResumeRejection(t *testing.T) {
	prefix := []string{
		``, // blank line still counts toward physical line numbers
		`{"type":"event","key":"k","time":700,"value":2,"partition":0}`,
		`{"type":"watermark","time":1000,"partition":0}`,
		`{"type":"watermark","time":600,"partition":1}`,
		`{"type":"idle","partition":1}`, // effective = 1000: closes [0,1000)
		``,
	}
	// Records that would close [600,1600) if they were ever processed.
	tail := []string{
		`{"type":"watermark","time":1600,"partition":0}`,
		`{"type":"watermark","time":1600,"partition":1}`,
	}
	wantOut := `{"key":"k","start":0,"end":1000,"count":1,"sum":2}` + "\n"

	cases := []struct {
		name      string
		resume    string
		wantInMsg []string
	}{
		{
			"below own previous watermark",
			`{"type":"watermark","time":500,"partition":1}`, // < own previous 600
			[]string{"partition 1", "previous watermark", "600"},
		},
		{
			"above own previous but below effective",
			`{"type":"watermark","time":900,"partition":1}`, // >= 600 own, < 1000 effective
			[]string{"partition 1", "effective watermark", "1000"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			lines := append(append(append([]string{}, prefix...), tc.resume), tail...)
			input := strings.Join(lines, "\n") + "\n"
			stdout, _, err := runPartitionedSliding(t, input, 1000, 600, 2)
			if err == nil {
				t.Fatal("expected resume rejection, got nil")
			}
			inputErr, ok := err.(*InputError)
			if !ok {
				t.Fatalf("expected *InputError, got %T: %v", err, err)
			}
			if want := len(prefix) + 1; inputErr.Line != want {
				t.Errorf("line = %d, want %d (blank lines count)", inputErr.Line, want)
			}
			for _, sub := range tc.wantInMsg {
				if !strings.Contains(inputErr.Reason, sub) {
					t.Errorf("reason = %q, want substring %q", inputErr.Reason, sub)
				}
			}
			if stdout != wantOut {
				t.Fatalf("prior output must be retained and post-error records produce nothing:\n got: %q\nwant: %q", stdout, wantOut)
			}
		})
	}
}

// After a successful resume the effective watermark still governs lateness:
// an event below it is reported late and skipped even though it falls inside
// the still-open overlapping window, while an event exactly at the watermark
// is counted normally.
func TestAggregatePartitionedSlidingLateEventAfterResume(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":700,"value":2,"partition":0}`,
		`{"type":"watermark","time":1000,"partition":0}`,
		`{"type":"watermark","time":600,"partition":1}`,
		`{"type":"idle","partition":1}`,                                  // effective = 1000: closes [0,1000)
		`{"type":"watermark","time":1000,"partition":1}`,                 // resume at the boundary
		`{"type":"event","key":"k","time":999,"value":5,"partition":1}`,  // late despite open [600,1600)
		`{"type":"event","key":"k","time":1000,"value":4,"partition":1}`, // equal to watermark: counted
		`{"type":"watermark","time":1600,"partition":0}`,
		`{"type":"watermark","time":1600,"partition":1}`, // closes [600,1600)
	}, "\n")
	stdout, stderr, err := runPartitionedSliding(t, input, 1000, 600, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := strings.Join([]string{
		`{"key":"k","start":0,"end":1000,"count":1,"sum":2}`,
		`{"key":"k","start":600,"end":1600,"count":2,"sum":6}`,
		``,
	}, "\n")
	if stdout != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, want)
	}
	wantErr := "line 6: late event time=999 below current watermark 1000, skipped\n"
	if stderr != wantErr {
		t.Fatalf("stderr = %q, want %q", stderr, wantErr)
	}
}
