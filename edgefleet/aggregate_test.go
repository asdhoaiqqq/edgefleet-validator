package edgefleet

import (
	"bytes"
	"encoding/json"
	"math"
	"reflect"
	"strings"
	"testing"
)

func runAggregate(t *testing.T, input string, window int64) (string, string, error) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	err := RunAggregate(strings.NewReader(input), window, &stdout, &stderr)
	return stdout.String(), stderr.String(), err
}

func runAggregatePartitions(t *testing.T, input string, window, partitions int64) (string, string, error) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	err := RunAggregatePartitions(strings.NewReader(input), window, partitions, &stdout, &stderr)
	return stdout.String(), stderr.String(), err
}

func TestAggregateBasicClosureAndOrder(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"sensor-b","time":800,"value":-2}`,
		`{"type":"event","key":"sensor-a","time":1200,"value":5}`,
		``, // blank line must be ignored
		`{"type":"event","key":"sensor-a","time":1500,"value":7}`,
		`{"type":"watermark","time":1000}`,
		`{"type":"watermark","time":1999}`, // does not close [1000,2000)
		`{"type":"watermark","time":2000}`,
	}, "\n")
	stdout, stderr, err := runAggregate(t, input, 1000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stderr != "" {
		t.Fatalf("unexpected stderr: %q", stderr)
	}
	want := strings.Join([]string{
		`{"key":"sensor-b","start":0,"end":1000,"count":1,"sum":-2}`,
		`{"key":"sensor-a","start":1000,"end":2000,"count":2,"sum":12}`,
		``,
	}, "\n")
	if stdout != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, want)
	}
}

func TestAggregateClosureOrderByEndThenUTF8Key(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"中","time":500,"value":1}`,
		`{"type":"event","key":"é","time":100,"value":1}`,
		`{"type":"event","key":"A","time":900,"value":1}`,
		`{"type":"event","key":"A","time":1200,"value":1}`,
		`{"type":"event","key":"é","time":1100,"value":1}`,
		`{"type":"watermark","time":2000}`,
	}, "\n")
	stdout, _, err := runAggregate(t, input, 1000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	lines := strings.Split(strings.TrimSuffix(stdout, "\n"), "\n")
	wantKeys := []string{"A", "é", "中", "A", "é"} // end=1000 byte-order, then end=2000
	wantEnds := []int64{1000, 1000, 1000, 2000, 2000}
	var gotKeys []string
	var gotEnds []int64
	for _, line := range lines {
		var result AggregateResult
		if err := json.Unmarshal([]byte(line), &result); err != nil {
			t.Fatalf("invalid output line %q: %v", line, err)
		}
		gotKeys = append(gotKeys, result.Key)
		gotEnds = append(gotEnds, result.End)
	}
	if !reflect.DeepEqual(gotKeys, wantKeys) || !reflect.DeepEqual(gotEnds, wantEnds) {
		t.Fatalf("closure order = %v ends %v, want %v ends %v\nfull output:\n%s", gotKeys, gotEnds, wantKeys, wantEnds, stdout)
	}
}

func TestAggregateNoWatermarkNoOutput(t *testing.T) {
	input := `{"type":"event","key":"k","time":1,"value":1}` + "\n"
	stdout, _, err := runAggregate(t, input, 1000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stdout != "" {
		t.Fatalf("open windows must not flush at EOF, got %q", stdout)
	}
}

func TestAggregateRepeatedAndJumpingWatermark(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":100,"value":2}`,
		`{"type":"watermark","time":1000}`,
		`{"type":"watermark","time":1000}`,                 // repeat: no duplicate output
		`{"type":"event","key":"k","time":1000,"value":3}`, // boundary: next window
		`{"type":"event","key":"k","time":2500,"value":4}`,
		`{"type":"watermark","time":5000}`, // jump closes 1000 and 2000 windows
	}, "\n")
	stdout, _, err := runAggregate(t, input, 1000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := strings.Join([]string{
		`{"key":"k","start":0,"end":1000,"count":1,"sum":2}`,
		`{"key":"k","start":1000,"end":2000,"count":1,"sum":3}`,
		`{"key":"k","start":2000,"end":3000,"count":1,"sum":4}`,
		``,
	}, "\n")
	if stdout != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, want)
	}
}

func TestAggregateLateAndBoundaryEvents(t *testing.T) {
	input := strings.Join([]string{
		``, // line 1 blank
		`{"type":"watermark","time":1000}`,
		``, // line 3 blank
		`{"type":"event","key":"k","time":999,"value":1}`,  // late, line 4
		`{"type":"event","key":"k","time":1000,"value":5}`, // equal to watermark: valid
	}, "\n")
	stdout, stderr, err := runAggregate(t, input, 1000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stdout != "" {
		t.Fatalf("window [1000,2000) must stay open, got %q", stdout)
	}
	wantErr := "line 4: late event time=999 below current watermark 1000, skipped\n"
	if stderr != wantErr {
		t.Fatalf("stderr = %q, want %q", stderr, wantErr)
	}
}

func TestAggregatePriorOutputRetainedOnFatalError(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":0,"value":1}`,
		`{"type":"watermark","time":1000}`,
		`not json`,
	}, "\n")
	stdout, _, err := runAggregate(t, input, 1000)
	if err == nil {
		t.Fatal("expected non-nil error for malformed JSON")
	}
	if got := stdout; got != `{"key":"k","start":0,"end":1000,"count":1,"sum":1}`+"\n" {
		t.Fatalf("earlier output must be retained, got %q", got)
	}
}

func TestAggregateDeterministic(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"zeta","time":200,"value":1}`,
		`{"type":"event","key":"alpha","time":100,"value":-1}`,
		`{"type":"event","key":"alpha","time":1200,"value":2}`,
		`{"type":"event","key":"zeta","time":1100,"value":3}`,
		`{"type":"event","key":"mid","time":900,"value":0}`,
		`{"type":"watermark","time":3000}`,
	}, "\n")
	first, _, err := runAggregate(t, input, 1000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	second, _, err := runAggregate(t, input, 1000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if first != second {
		t.Fatalf("repeated runs must be identical:\n%s\nvs\n%s", first, second)
	}
}

func TestAggregateInvalidWindow(t *testing.T) {
	for _, w := range []int64{0, -1, math.MinInt64} {
		if err := RunAggregate(strings.NewReader(""), w, &bytes.Buffer{}, &bytes.Buffer{}); err == nil {
			t.Fatalf("expected error for window %d", w)
		}
	}
}

func TestAggregateFatalRecordErrors(t *testing.T) {
	cases := []struct {
		name      string
		input     string
		wantLine  int
		wantInMsg string
	}{
		{"malformed json", "{not json\n", 1, "invalid JSON"},
		{"missing type", `{"key":"k","time":1,"value":1}` + "\n", 1, `"type"`},
		{"non-string type", `{"type":7,"key":"k","time":1,"value":1}` + "\n", 1, `"type"`},
		{"unknown type", `{"type":"heartbeat"}` + "\n", 1, "unknown record type"},
		{"missing key", `{"type":"event","time":1,"value":1}` + "\n", 1, `"key"`},
		{"empty key", `{"type":"event","key":"","time":1,"value":1}` + "\n", 1, "non-empty"},
		{"numeric key", `{"type":"event","key":5,"time":1,"value":1}` + "\n", 1, `"key"`},
		{"missing time", `{"type":"event","key":"k","value":1}` + "\n", 1, `"time"`},
		{"negative time", `{"type":"event","key":"k","time":-1,"value":1}` + "\n", 1, "non-negative"},
		{"fractional time", `{"type":"event","key":"k","time":1.5,"value":1}` + "\n", 1, `"time"`},
		{"string time", `{"type":"event","key":"k","time":"1","value":1}` + "\n", 1, `"time"`},
		{"null time", `{"type":"event","key":"k","time":null,"value":1}` + "\n", 1, `"time"`},
		{"time too large", `{"type":"event","key":"k","time":9223372036854775808,"value":1}` + "\n", 1, "range"},
		{"missing value", `{"type":"event","key":"k","time":1}` + "\n", 1, `"value"`},
		{"boolean value", `{"type":"event","key":"k","time":1,"value":true}` + "\n", 1, `"value"`},
		{"value too large", `{"type":"event","key":"k","time":1,"value":9223372036854775808}` + "\n", 1, "range"},
		{"watermark missing time", `{"type":"watermark"}` + "\n", 1, `"time"`},
		{"watermark negative", `{"type":"watermark","time":-5}` + "\n", 1, "non-negative"},
		{"watermark regression", `{"type":"watermark","time":10}` + "\n" + `{"type":"watermark","time":9}` + "\n", 2, "backwards"},
		{
			"window end overflow",
			`{"type":"event","key":"k","time":9223372036854775807,"value":1}` + "\n",
			1, "window end overflow",
		},
		{
			"cumulative sum overflow",
			`{"type":"event","key":"k","time":1,"value":9223372036854775807}` + "\n" +
				`{"type":"event","key":"k","time":2,"value":1}` + "\n",
			2, "sum overflow",
		},
		{
			"cumulative sum underflow",
			`{"type":"event","key":"k","time":1,"value":-9223372036854775808}` + "\n" +
				`{"type":"event","key":"k","time":2,"value":-1}` + "\n",
			2, "sum overflow",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := runAggregate(t, tc.input, 3)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			inputErr, ok := err.(*InputError)
			if !ok {
				t.Fatalf("expected *InputError, got %T: %v", err, err)
			}
			if inputErr.Line != tc.wantLine {
				t.Errorf("line = %d, want %d", inputErr.Line, tc.wantLine)
			}
			if !strings.Contains(inputErr.Reason, tc.wantInMsg) {
				t.Errorf("reason = %q, want substring %q", inputErr.Reason, tc.wantInMsg)
			}
		})
	}
}

func TestAggregateErroredRecordProducesNoWindow(t *testing.T) {
	// First event valid, second invalid within same window: window must not be emitted.
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":1,"value":1}`,
		`{"type":"event","key":"k","time":2,"value":"oops"}`,
		`{"type":"watermark","time":1000}`,
	}, "\n")
	stdout, _, err := runAggregate(t, input, 1000)
	if err == nil {
		t.Fatal("expected error")
	}
	if stdout != "" {
		t.Fatalf("errored record must not contribute to window results, got %q", stdout)
	}
}

func TestAggregateExtraFieldsAccepted(t *testing.T) {
	input := `{"type":"event","key":"k","time":1,"value":1,"source":"lab"}` + "\n" +
		`{"type":"watermark","time":1000,"note":"jump"}` + "\n"
	stdout, _, err := runAggregate(t, input, 1000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := `{"key":"k","start":0,"end":1000,"count":1,"sum":1}` + "\n"
	if stdout != want {
		t.Fatalf("got %q, want %q", stdout, want)
	}
}

func TestAggregateWatermarkZero(t *testing.T) {
	input := `{"type":"watermark","time":0}` + "\n" +
		`{"type":"event","key":"k","time":0,"value":1}` + "\n"
	stdout, _, err := runAggregate(t, input, 1000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stdout != "" {
		t.Fatalf("watermark 0 closes nothing; got %q", stdout)
	}
}

func TestAggregatePartitionsExampleFromSpec(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","partition":0,"key":"k","time":100,"value":2}`,
		`{"type":"watermark","partition":0,"time":2000}`, // p0 alone: no effective watermark yet
		`{"type":"event","partition":1,"key":"k","time":800,"value":3}`,
		`{"type":"watermark","partition":1,"time":1000}`, // min(2000,1000)=1000 closes [0,1000)
		`{"type":"watermark","partition":0,"time":2000}`, // repeat must not re-emit
	}, "\n")
	stdout, stderr, err := runAggregatePartitions(t, input, 1000, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stderr != "" {
		t.Fatalf("unexpected stderr: %q", stderr)
	}
	want := `{"key":"k","start":0,"end":1000,"count":2,"sum":5}` + "\n"
	if stdout != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, want)
	}
}

func TestAggregatePartitionsNoClosureOrLatenessUntilAllReported(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","partition":0,"key":"k","time":1,"value":1}`,
		`{"type":"watermark","partition":0,"time":9000}`,              // races ahead alone
		`{"type":"event","partition":1,"key":"k","time":2,"value":2}`, // not late despite p0=9000
	}, "\n")
	stdout, stderr, err := runAggregatePartitions(t, input, 1000, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stdout != "" {
		t.Fatalf("no window may close before every partition reports a watermark, got %q", stdout)
	}
	if stderr != "" {
		t.Fatalf("event must not be late without an effective watermark, got %q", stderr)
	}
}

func TestAggregatePartitionsEffectiveMinimumDrivesClosure(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","partition":0,"key":"k","time":0,"value":1}`,
		`{"type":"event","partition":1,"key":"k","time":1000,"value":2}`,
		`{"type":"watermark","partition":0,"time":5000}`,
		`{"type":"watermark","partition":1,"time":2000}`, // effective 2000: only [0,1000) closes
		`{"type":"watermark","partition":1,"time":5000}`, // effective 5000: [1000,2000) closes
	}, "\n")
	stdout, _, err := runAggregatePartitions(t, input, 1000, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := strings.Join([]string{
		`{"key":"k","start":0,"end":1000,"count":1,"sum":1}`,
		`{"key":"k","start":1000,"end":2000,"count":1,"sum":2}`,
		``,
	}, "\n")
	if stdout != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, want)
	}
}

func TestAggregatePartitionsLateUsesMinimum(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"watermark","partition":0,"time":5000}`,
		`{"type":"watermark","partition":1,"time":1000}`,                 // effective watermark 1000
		`{"type":"event","partition":0,"key":"k","time":999,"value":1}`,  // below min: late even on fast partition
		`{"type":"event","partition":0,"key":"k","time":1000,"value":2}`, // equal to min: valid
	}, "\n")
	stdout, stderr, err := runAggregatePartitions(t, input, 1000, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stdout != "" {
		t.Fatalf("[1000,2000) stays open, got %q", stdout)
	}
	wantErr := "line 3: late event time=999 below current watermark 1000, skipped\n"
	if stderr != wantErr {
		t.Fatalf("stderr = %q, want %q", stderr, wantErr)
	}
}

func TestAggregatePartitionsMergeAcrossPartitions(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","partition":0,"key":"k","time":100,"value":2}`,
		`{"type":"event","partition":1,"key":"k","time":200,"value":3}`,
		`{"type":"event","partition":1,"key":"k","time":300,"value":4}`,
		`{"type":"watermark","partition":0,"time":1000}`,
		`{"type":"watermark","partition":1,"time":1000}`,
	}, "\n")
	stdout, _, err := runAggregatePartitions(t, input, 1000, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := `{"key":"k","start":0,"end":1000,"count":3,"sum":9}` + "\n"
	if stdout != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, want)
	}
}

func TestAggregatePartitionsSinglePartitionRequiresZero(t *testing.T) {
	input := `{"type":"event","partition":0,"key":"k","time":1,"value":1}` + "\n" +
		`{"type":"watermark","partition":0,"time":1000}` + "\n"
	stdout, _, err := runAggregatePartitions(t, input, 1000, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := `{"key":"k","start":0,"end":1000,"count":1,"sum":1}` + "\n"
	if stdout != want {
		t.Fatalf("got %q, want %q", stdout, want)
	}
}

func TestAggregatePartitionsWatermarkRegression(t *testing.T) {
	cases := []struct {
		name     string
		input    string
		wantLine int
	}{
		{
			"regression lowers minimum",
			strings.Join([]string{
				`{"type":"watermark","partition":0,"time":1000}`,
				`{"type":"watermark","partition":1,"time":2000}`,
				`{"type":"watermark","partition":1,"time":1500}`,
			}, "\n"),
			3,
		},
		{
			"regression with unchanged minimum",
			strings.Join([]string{
				`{"type":"watermark","partition":0,"time":1000}`,
				`{"type":"watermark","partition":1,"time":5000}`,
				`{"type":"watermark","partition":1,"time":4000}`, // min stays 1000
			}, "\n"),
			3,
		},
		{
			"regression before all partitions reported",
			strings.Join([]string{
				`{"type":"watermark","partition":0,"time":5000}`,
				`{"type":"watermark","partition":0,"time":4000}`, // no effective wm yet, still fatal
			}, "\n"),
			2,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := runAggregatePartitions(t, tc.input, 1000, 2)
			inputErr := requireInputError(t, err)
			if inputErr.Line != tc.wantLine {
				t.Errorf("line = %d, want %d", inputErr.Line, tc.wantLine)
			}
			if !strings.Contains(inputErr.Reason, "backwards") {
				t.Errorf("reason = %q, want substring %q", inputErr.Reason, "backwards")
			}
		})
	}
}

func TestAggregatePartitionsPriorOutputRetainedOnRegression(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","partition":0,"key":"k","time":1,"value":1}`,
		`{"type":"watermark","partition":0,"time":1000}`,
		`{"type":"watermark","partition":1,"time":1000}`, // closes [0,1000)
		`{"type":"watermark","partition":1,"time":900}`,  // fatal regression
	}, "\n")
	stdout, _, err := runAggregatePartitions(t, input, 1000, 2)
	requireInputError(t, err)
	want := `{"key":"k","start":0,"end":1000,"count":1,"sum":1}` + "\n"
	if stdout != want {
		t.Fatalf("earlier output must be retained, got %q", stdout)
	}
}

func TestAggregatePartitionsFieldErrors(t *testing.T) {
	cases := []struct {
		name      string
		input     string
		wantLine  int
		wantInMsg string
	}{
		{"missing partition on event", `{"type":"event","key":"k","time":1,"value":1}` + "\n", 1, `"partition"`},
		{"missing partition on watermark", `{"type":"watermark","time":1}` + "\n", 1, `"partition"`},
		{"string partition", `{"type":"event","partition":"0","key":"k","time":1,"value":1}` + "\n", 1, `"partition"`},
		{"fractional partition", `{"type":"event","partition":0.5,"key":"k","time":1,"value":1}` + "\n", 1, `"partition"`},
		{"boolean partition", `{"type":"event","partition":true,"key":"k","time":1,"value":1}` + "\n", 1, `"partition"`},
		{"null partition", `{"type":"event","partition":null,"key":"k","time":1,"value":1}` + "\n", 1, `"partition"`},
		{"negative partition", `{"type":"event","partition":-1,"key":"k","time":1,"value":1}` + "\n", 1, "[0,2)"},
		{"partition equal to count", `{"type":"event","partition":2,"key":"k","time":1,"value":1}` + "\n", 1, "[0,2)"},
		{"partition beyond count", `{"type":"event","partition":9,"key":"k","time":1,"value":1}` + "\n", 1, "[0,2)"},
		{
			"partition too large for int64",
			`{"type":"event","partition":9223372036854775808,"key":"k","time":1,"value":1}` + "\n",
			1, "range",
		},
		{
			"partition out of range on later physical line",
			" \n" + `{"type":"watermark","partition":0,"time":1}` + "\n" +
				`{"type":"watermark","partition":1,"time":1}` + "\n" +
				`{"type":"event","partition":3,"key":"k","time":1,"value":1}` + "\n",
			4, "[0,2)",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := runAggregatePartitions(t, tc.input, 3, 2)
			inputErr := requireInputError(t, err)
			if inputErr.Line != tc.wantLine {
				t.Errorf("line = %d, want %d", inputErr.Line, tc.wantLine)
			}
			if !strings.Contains(inputErr.Reason, tc.wantInMsg) {
				t.Errorf("reason = %q, want substring %q", inputErr.Reason, tc.wantInMsg)
			}
		})
	}
}

func TestAggregatePartitionsInvalidCount(t *testing.T) {
	for _, n := range []int64{0, -1, math.MinInt64} {
		if err := RunAggregatePartitions(strings.NewReader(""), 1000, n, &bytes.Buffer{}, &bytes.Buffer{}); err == nil {
			t.Fatalf("expected error for partition count %d", n)
		}
	}
}

func TestAggregatePartitionsNoFlushAtEOF(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","partition":0,"key":"k","time":1,"value":1}`,
		`{"type":"event","partition":1,"key":"k","time":2,"value":2}`,
	}, "\n")
	stdout, _, err := runAggregatePartitions(t, input, 1000, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stdout != "" {
		t.Fatalf("open windows must not flush at EOF, got %q", stdout)
	}
}

func requireInputError(t *testing.T, err error) *InputError {
	t.Helper()
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	inputErr, ok := err.(*InputError)
	if !ok {
		t.Fatalf("expected *InputError, got %T: %v", err, err)
	}
	return inputErr
}
