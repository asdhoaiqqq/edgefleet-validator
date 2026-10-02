package edgefleet

import (
	"bytes"
	"math"
	"strings"
	"testing"
)

func runAgg(t *testing.T, input string, windowMs int64) (string, string, error) {
	t.Helper()
	var out, errOut bytes.Buffer
	err := RunAggregate(strings.NewReader(input), &out, &errOut, windowMs)
	return out.String(), errOut.String(), err
}

// The example from the task description.
func TestAggregateSpecExample(t *testing.T) {
	input := `{"type":"event","key":"sensor-a","time":1200,"value":5}
{"type":"watermark","time":2000}
`
	out, _, err := runAgg(t, input, 1000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := `{"key":"sensor-a","start":1000,"end":2000,"count":1,"sum":5}` + "\n"
	if out != want {
		t.Errorf("got %q, want %q", out, want)
	}
}

func TestAggregateOutOfOrderAndLate(t *testing.T) {
	input := `{"type":"event","key":"a","time":5,"value":1}
{"type":"event","key":"b","time":15,"value":2}
{"type":"event","key":"a","time":15,"value":3}
{"type":"watermark","time":10}
{"type":"event","key":"a","time":9,"value":100}
{"type":"event","key":"a","time":10,"value":7}
{"type":"watermark","time":20}
`
	out, errOut, err := runAgg(t, input, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := strings.Join([]string{
		`{"key":"a","start":0,"end":10,"count":1,"sum":1}`,
		`{"key":"a","start":10,"end":20,"count":2,"sum":10}`,
		`{"key":"b","start":10,"end":20,"count":1,"sum":2}`,
	}, "\n") + "\n"
	if out != want {
		t.Errorf("stdout:\ngot  %q\nwant %q", out, want)
	}
	if !strings.Contains(errOut, "line 5") || !strings.Contains(errOut, "event time 9") || !strings.Contains(errOut, "watermark 10") {
		t.Errorf("late-event stderr missing details: %q", errOut)
	}
}

func TestAggregateWatermarkJumpClosesInOrder(t *testing.T) {
	input := `{"type":"event","key":"z","time":5,"value":1}
{"type":"event","key":"a","time":15,"value":2}
{"type":"event","key":"m","time":25,"value":3}
{"type":"watermark","time":30}
`
	out, _, err := runAgg(t, input, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := strings.Join([]string{
		`{"key":"z","start":0,"end":10,"count":1,"sum":1}`,
		`{"key":"a","start":10,"end":20,"count":1,"sum":2}`,
		`{"key":"m","start":20,"end":30,"count":1,"sum":3}`,
	}, "\n") + "\n"
	if out != want {
		t.Errorf("got %q, want %q", out, want)
	}
}

func TestAggregateWatermarkRepeatNoOutput(t *testing.T) {
	input := `{"type":"event","key":"a","time":5,"value":1}
{"type":"watermark","time":10}
{"type":"watermark","time":10}
{"type":"watermark","time":10}
`
	out, _, err := runAgg(t, input, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := `{"key":"a","start":0,"end":10,"count":1,"sum":1}` + "\n"
	if out != want {
		t.Errorf("got %q, want %q", out, want)
	}
}

func TestAggregateWatermarkRegression(t *testing.T) {
	input := `{"type":"watermark","time":20}
{"type":"watermark","time":10}
`
	_, _, err := runAgg(t, input, 10)
	if err == nil {
		t.Fatal("expected error for watermark regression")
	}
	if !strings.Contains(err.Error(), "line 2") || !strings.Contains(err.Error(), "regression") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestAggregateNoWatermarkNoOutput(t *testing.T) {
	input := `{"type":"event","key":"a","time":5,"value":1}
{"type":"event","key":"a","time":15,"value":2}
`
	out, _, err := runAgg(t, input, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out != "" {
		t.Errorf("expected no output, got %q", out)
	}
}

func TestAggregateEndOfInputDoesNotClose(t *testing.T) {
	input := `{"type":"event","key":"a","time":5,"value":1}
`
	out, _, err := runAgg(t, input, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out != "" {
		t.Errorf("expected no output at end of input, got %q", out)
	}
}

func TestAggregateBlankLines(t *testing.T) {
	input := "\n  \n{\"type\":\"event\",\"key\":\"a\",\"time\":5,\"value\":1}\n\n{\"type\":\"watermark\",\"time\":10}\n"
	out, _, err := runAgg(t, input, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := `{"key":"a","start":0,"end":10,"count":1,"sum":1}` + "\n"
	if out != want {
		t.Errorf("got %q, want %q", out, want)
	}
}

func TestAggregateCRLF(t *testing.T) {
	input := "{\"type\":\"event\",\"key\":\"a\",\"time\":5,\"value\":1}\r\n{\"type\":\"watermark\",\"time\":10}\r\n"
	out, _, err := runAgg(t, input, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := `{"key":"a","start":0,"end":10,"count":1,"sum":1}` + "\n"
	if out != want {
		t.Errorf("got %q, want %q", out, want)
	}
}

func TestAggregateKeyOrderingUTF8(t *testing.T) {
	input := `{"type":"event","key":"z","time":5,"value":1}
{"type":"event","key":"A","time":5,"value":1}
{"type":"event","key":"a","time":5,"value":1}
{"type":"watermark","time":10}
`
	out, _, err := runAgg(t, input, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := strings.Join([]string{
		`{"key":"A","start":0,"end":10,"count":1,"sum":1}`,
		`{"key":"a","start":0,"end":10,"count":1,"sum":1}`,
		`{"key":"z","start":0,"end":10,"count":1,"sum":1}`,
	}, "\n") + "\n"
	if out != want {
		t.Errorf("got %q, want %q", out, want)
	}
}

func TestAggregateSumAccumulatesSigned(t *testing.T) {
	input := `{"type":"event","key":"a","time":5,"value":-5}
{"type":"event","key":"a","time":6,"value":10}
{"type":"event","key":"a","time":7,"value":-2}
{"type":"watermark","time":10}
`
	out, _, err := runAgg(t, input, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := `{"key":"a","start":0,"end":10,"count":3,"sum":3}` + "\n"
	if out != want {
		t.Errorf("got %q, want %q", out, want)
	}
}

func TestAggregateDeterministic(t *testing.T) {
	input := `{"type":"event","key":"a","time":15,"value":3}
{"type":"event","key":"a","time":5,"value":1}
{"type":"event","key":"b","time":25,"value":2}
{"type":"watermark","time":30}
{"type":"event","key":"a","time":9,"value":100}
{"type":"event","key":"a","time":10,"value":7}
{"type":"watermark","time":30}
`
	out1, _, err := runAgg(t, input, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	out2, _, err := runAgg(t, input, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out1 != out2 {
		t.Errorf("non-deterministic:\n%q\nvs\n%q", out1, out2)
	}
}

func TestAggregateErrorPreservesPriorOutput(t *testing.T) {
	input := `{"type":"event","key":"a","time":5,"value":1}
{"type":"watermark","time":10}
{"type":"event","key":"a","time":15,"value":2}
{"type":"watermark","time":5}
`
	out, _, err := runAgg(t, input, 10)
	if err == nil {
		t.Fatal("expected error")
	}
	want := `{"key":"a","start":0,"end":10,"count":1,"sum":1}` + "\n"
	if out != want {
		t.Errorf("prior output not preserved: got %q, want %q", out, want)
	}
}

func TestAggregateErrors(t *testing.T) {
	cases := []struct {
		name  string
		input string
	}{
		{"malformed json", `{not json` + "\n"},
		{"trailing data", "{\"type\":\"watermark\",\"time\":1} garbage\n"},
		{"missing type", "{\"key\":\"a\",\"time\":1,\"value\":1}\n"},
		{"type not string", "{\"type\":1,\"key\":\"a\",\"time\":1,\"value\":1}\n"},
		{"unknown type", "{\"type\":\"foo\",\"time\":1}\n"},
		{"missing key", "{\"type\":\"event\",\"time\":1,\"value\":1}\n"},
		{"empty key", "{\"type\":\"event\",\"key\":\"\",\"time\":1,\"value\":1}\n"},
		{"key not string", "{\"type\":\"event\",\"key\":5,\"time\":1,\"value\":1}\n"},
		{"missing time", "{\"type\":\"event\",\"key\":\"a\",\"value\":1}\n"},
		{"time not int", "{\"type\":\"event\",\"key\":\"a\",\"time\":1.5,\"value\":1}\n"},
		{"time negative", "{\"type\":\"event\",\"key\":\"a\",\"time\":-1,\"value\":1}\n"},
		{"time overflow", "{\"type\":\"event\",\"key\":\"a\",\"time\":99999999999999999999999,\"value\":1}\n"},
		{"missing value", "{\"type\":\"event\",\"key\":\"a\",\"time\":1}\n"},
		{"value not int", "{\"type\":\"event\",\"key\":\"a\",\"time\":1,\"value\":\"x\"}\n"},
		{"value overflow", "{\"type\":\"event\",\"key\":\"a\",\"time\":1,\"value\":99999999999999999999999}\n"},
		{"watermark time negative", "{\"type\":\"watermark\",\"time\":-1}\n"},
		{"watermark missing time", "{\"type\":\"watermark\"}\n"},
		{"null record", "null\n"},
		{"array record", "[1,2,3]\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := runAgg(t, tc.input, 10)
			if err == nil {
				t.Errorf("expected error, got nil")
			}
		})
	}
}

func TestAggregateSumOverflow(t *testing.T) {
	input := `{"type":"event","key":"a","time":5,"value":9223372036854775807}
{"type":"event","key":"a","time":6,"value":1}
{"type":"watermark","time":10}
`
	_, _, err := runAgg(t, input, 10)
	if err == nil {
		t.Fatal("expected sum overflow error")
	}
	if !strings.Contains(err.Error(), "line 2") || !strings.Contains(err.Error(), "sum overflow") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestAggregateSumUnderflow(t *testing.T) {
	input := `{"type":"event","key":"a","time":5,"value":-9223372036854775808}
{"type":"event","key":"a","time":6,"value":-1}
{"type":"watermark","time":10}
`
	_, _, err := runAgg(t, input, 10)
	if err == nil {
		t.Fatal("expected sum underflow error")
	}
	if !strings.Contains(err.Error(), "line 2") || !strings.Contains(err.Error(), "sum overflow") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestAggregateWindowEndOverflow(t *testing.T) {
	// windowMs = MaxInt64, event at MaxInt64: start = 0, end = MaxInt64 (ok).
	// Use a window length that overflows when the event's window end is computed.
	input := `{"type":"event","key":"a","time":9223372036854775807,"value":1}
`
	_, _, err := runAgg(t, input, 2)
	if err == nil {
		t.Fatal("expected window end overflow error")
	}
	if !strings.Contains(err.Error(), "line 1") || !strings.Contains(err.Error(), "window end overflow") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestAggregateMaxWindowMs(t *testing.T) {
	input := `{"type":"event","key":"a","time":0,"value":1}
{"type":"watermark","time":9223372036854775807}
`
	out, _, err := runAgg(t, input, math.MaxInt64)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := `{"key":"a","start":0,"end":9223372036854775807,"count":1,"sum":1}` + "\n"
	if out != want {
		t.Errorf("got %q, want %q", out, want)
	}
}

func TestAggregateEventAtWatermarkBoundaryValid(t *testing.T) {
	// Event exactly at watermark time belongs to the next window.
	input := `{"type":"event","key":"a","time":10,"value":1}
{"type":"watermark","time":10}
{"type":"watermark","time":20}
`
	out, _, err := runAgg(t, input, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := `{"key":"a","start":10,"end":20,"count":1,"sum":1}` + "\n"
	if out != want {
		t.Errorf("got %q, want %q", out, want)
	}
}

func TestAggregateLateEventDoesNotCloseWindow(t *testing.T) {
	// A late event must not resurrect a closed window; it is simply skipped.
	input := `{"type":"event","key":"a","time":5,"value":1}
{"type":"watermark","time":10}
{"type":"event","key":"a","time":5,"value":100}
{"type":"watermark","time":20}
`
	out, _, err := runAgg(t, input, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := strings.Join([]string{
		`{"key":"a","start":0,"end":10,"count":1,"sum":1}`,
	}, "\n") + "\n"
	if out != want {
		t.Errorf("got %q, want %q", out, want)
	}
}
