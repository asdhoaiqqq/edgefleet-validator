package edgefleet

import (
	"encoding/json"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// These tests pin down what a legal JSON key escape means as it travels
// through the three stages that touch it: reading the line-delimited record,
// grouping events by the decoded key within a window, and writing the result
// back out as JSON. The same character written with different legal escapes
// must decode to the same key and merge; characters that only look alike after
// a second decode must stay apart; and every result must reparse to the exact
// original key, with embedded line-control characters never breaking the one
// result per physical output line rule. Damaged input (raw control characters
// inside a string, unsupported escapes) stays on the existing fatal input
// error path with the physical line number.

// Equivalent legal spellings of one character decode to the same key, so the
// two events merge into count=2, sum=5 regardless of which spelling arrives
// first. spell1 and spell2 are raw JSON string tokens (quotes included) that
// must both decode to wantKey.
func TestAggregateEquivalentEscapeSpellingsMerge(t *testing.T) {
	cases := []struct {
		name    string
		spell1  string // raw JSON string token, quotes included
		spell2  string // a different legal spelling of the same decoded key
		wantKey string // the decoded Go string
	}{
		{"newline short vs unicode", `"a\nb"`, `"a\u000ab"`, "a\nb"},
		{"carriage return short vs unicode", `"c\rd"`, `"c\u000dd"`, "c\rd"},
		{"tab short vs unicode", `"e\tf"`, `"e\u0009f"`, "e\tf"},
		{"quote short vs unicode", `"g\"h"`, `"g\u0022h"`, "g\"h"},
		{"backslash short vs unicode", `"i\\j"`, `"i\u005cj"`, `i\j`},
		{"slash escaped vs unicode", `"p\/q"`, `"p\u002fq"`, "p/q"},
		{"plain slash vs escaped slash", `"p/q"`, `"p\/q"`, "p/q"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, reverse := range []bool{false, true} {
				t.Run(map[bool]string{false: "forward", true: "reversed"}[reverse], func(t *testing.T) {
					first, second := tc.spell1, tc.spell2
					if reverse {
						first, second = second, first
					}
					input := strings.Join([]string{
						`{"type":"event","key":` + first + `,"time":100,"value":2}`,
						`{"type":"event","key":` + second + `,"time":200,"value":3}`,
						`{"type":"watermark","time":1000}`,
					}, "\n") + "\n"
					stdout, stderr, err := runAggregate(t, input, 1000)
					if err != nil {
						t.Fatalf("unexpected error: %v", err)
					}
					if stderr != "" {
						t.Fatalf("unexpected late log: %q", stderr)
					}
					results := parseResultLines(t, stdout)
					if len(results) != 1 {
						t.Fatalf("equivalent spellings must merge to one key, got %d results:\n%s", len(results), stdout)
					}
					got := results[0]
					if got.Key != tc.wantKey {
						t.Errorf("decoded key = %q, want %q", got.Key, tc.wantKey)
					}
					if got.Start != 0 || got.End != 1000 {
						t.Errorf("window = [%d,%d), want [0,1000)", got.Start, got.End)
					}
					if got.Count != 2 || got.Sum != 5 {
						t.Errorf("count=%d sum=%d, want count=2 sum=5", got.Count, got.Sum)
					}
				})
			}
		})
	}
}

// A key that genuinely contains the two characters backslash and 'n' must not
// be decoded a second time into a newline and merge with the real newline key.
// It is a separate window result, while the short-escape and Unicode-escape
// spellings of a real newline merge.
func TestAggregateLiteralBackslashNStaysDistinctFromNewline(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"a\nb","time":100,"value":2}`,     // real newline (short escape)
		`{"type":"event","key":"a\u000ab","time":200,"value":3}`, // same key via Unicode escape
		`{"type":"event","key":"a\\nb","time":300,"value":7}`,    // backslash + 'n': a different key
		`{"type":"watermark","time":1000}`,
	}, "\n") + "\n"
	stdout, _, err := runAggregate(t, input, 1000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	results := parseResultLines(t, stdout)
	want := []AggregateResult{
		{Key: "a\nb", Start: 0, End: 1000, Count: 2, Sum: 5}, // 0x0a sorts before 0x5c
		{Key: `a\nb`, Start: 0, End: 1000, Count: 1, Sum: 7},
	}
	if len(results) != len(want) {
		t.Fatalf("got %d results, want %d:\n%s", len(results), len(want), stdout)
	}
	for i := range want {
		if results[i] != want[i] {
			t.Errorf("result %d = %+v, want %+v", i, results[i], want[i])
		}
	}
	// Lock the wire encoding too: a real newline is an escaped \n on output,
	// a real backslash is escaped as \\, so the two lines never look alike.
	wantOut := strings.Join([]string{
		`{"key":"a\nb","start":0,"end":1000,"count":2,"sum":5}`,
		`{"key":"a\\nb","start":0,"end":1000,"count":1,"sum":7}`,
		``,
	}, "\n")
	if stdout != wantOut {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, wantOut)
	}
}

// Keys containing newline, carriage return, tab, quote, NUL, single and
// consecutive backslashes -- including special characters at the very end of
// the key -- each produce exactly one complete physical output line, whose
// JSON decodes back to the exact original key. The output must not keep just
// the visible part or add/drop a layer of escaping. Results still close in
// decoded-key UTF-8 byte order.
func TestAggregateSpecialKeysRoundTripOnePhysicalLineEach(t *testing.T) {
	cases := []struct {
		name    string
		literal string // raw JSON string token fed in, quotes included
		wantKey string // decoded Go string that must come back out
	}{
		{"embedded newline", `"a\nb"`, "a\nb"},
		{"embedded carriage return", `"c\rd"`, "c\rd"},
		{"embedded tab", `"e\tf"`, "e\tf"},
		{"embedded quote", `"g\"h"`, `g"h`},
		{"embedded backslash", `"i\\j"`, `i\j`},
		{"two trailing backslashes", `"dd\\\\"`, `dd\\`},
		{"trailing quote", `"q\""`, `q"`},
		{"trailing newline", `"r\n"`, "r\n"},
		{"trailing nul", `"s\u0000"`, "s\x00"},
		{"trailing carriage return", `"t\r"`, "t\r"},
		{"literal backslash n", `"u\\n"`, `u\n`},
	}
	var lines []string
	for _, tc := range cases {
		lines = append(lines, `{"type":"event","key":`+tc.literal+`,"time":100,"value":1}`)
	}
	lines = append(lines, `{"type":"watermark","time":1000}`)
	stdout, _, err := runAggregate(t, strings.Join(lines, "\n")+"\n", 1000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// One trailing newline per result and nothing else: an embedded raw
	// newline or carriage return would split a result into extra physical
	// lines and fail both this count and the per-line JSON parse below.
	if got := strings.Count(stdout, "\n"); got != len(cases) {
		t.Fatalf("stdout has %d physical line(s), want exactly %d (one per result):\n%q", got, len(cases), stdout)
	}
	body := strings.TrimSuffix(stdout, "\n")
	physicalLines := strings.Split(body, "\n")
	results := parseResultLines(t, stdout)
	if len(results) != len(cases) {
		t.Fatalf("got %d results, want %d", len(results), len(cases))
	}

	// Canonical wire form: re-marshalling each decoded result must reproduce
	// the exact physical line. This fails if the output embedded a raw
	// line-control byte or added/dropped a layer of escaping -- that would
	// either no longer split 1:1 here or marshal back to different bytes.
	for i, r := range results {
		canonical, err := json.Marshal(r)
		if err != nil {
			t.Fatalf("re-marshal result %d: %v", i, err)
		}
		if physicalLines[i] != string(canonical) {
			t.Errorf("physical line %d is not canonical JSON for its key:\n got %s\nwant %s", i+1, physicalLines[i], canonical)
		}
	}

	// Order must be the decoded keys' UTF-8 byte order.
	var sortedKeys []string
	for _, tc := range cases {
		sortedKeys = append(sortedKeys, tc.wantKey)
	}
	sort.Strings(sortedKeys)
	for i, r := range results {
		if r.Key != sortedKeys[i] {
			t.Errorf("position %d key = %q, want %q in UTF-8 byte order (full order: %q)", i, r.Key, sortedKeys[i], sortedKeys)
		}
	}

	// Every output key must reparsed to the exact key that went in.
	byKey := make(map[string]AggregateResult, len(results))
	for _, r := range results {
		byKey[r.Key] = r
	}
	for _, tc := range cases {
		r, ok := byKey[tc.wantKey]
		if !ok {
			t.Errorf("result for key %q is missing (no output key round-tripped to it)", tc.wantKey)
			continue
		}
		if r.Count != 1 || r.Sum != 1 || r.Start != 0 || r.End != 1000 {
			t.Errorf("result for key %q = %+v, want count=1 sum=1 in [0,1000)", tc.wantKey, r)
		}
	}

	// Lock the exact wire form at the tricky tail positions: consecutive
	// backslashes need doubling, a real newline/trailing CR stay escaped, and
	// NUL uses its Unicode escape -- none of these may be a raw byte.
	for _, fragment := range []string{
		`"key":"dd\\\\",`,  // two decoded trailing backslashes -> four on the wire
		`"key":"r\n",`,     // trailing real newline -> escaped
		`"key":"t\r",`,     // trailing real CR -> escaped
		`"key":"s\u0000",`, // trailing NUL -> Unicode escape
	} {
		if !strings.Contains(stdout, fragment) {
			t.Errorf("stdout missing wire fragment %s:\n%s", fragment, stdout)
		}
	}
	// No individual result line may carry a raw C0 control byte (0x00-0x1F):
	// an embedded newline, CR or NUL must stay escaped and can never split a
	// result into more than one physical output line.
	for i, line := range physicalLines {
		for _, c := range []byte(line) {
			if c < 0x20 {
				t.Errorf("physical line %d contains raw control byte 0x%02x:\n%q", i+1, c, line)
			}
		}
	}
}

// When the same keys close at the same time, their order follows the decoded
// keys' UTF-8 bytes no matter which legal escape spelling was used nor the
// order events arrived in.
func TestAggregateEscapeSpellingAndArrivalDoNotAffectOrder(t *testing.T) {
	// The newline key appears once via the short escape and once via the
	// Unicode escape; those two events merge, leaving the five decoded keys
	// regardless of arrival order.
	build := func(tokens []string) string {
		var ls []string
		for i, tok := range tokens {
			ls = append(ls, `{"type":"event","key":`+tok+`,"time":100,"value":`+strconv.Itoa(i+1)+`}`)
		}
		ls = append(ls, `{"type":"watermark","time":1000}`)
		return strings.Join(ls, "\n") + "\n"
	}
	const (
		nul      = `"\u0000z"`
		newlineS = `"\nz"`     // real newline, short escape
		newlineU = `"\u000az"` // same key, Unicode escape
		ascii    = `"az"`
		eacute   = `"\u00e9z"` // é via Unicode escape
		multi    = `"中z"`      // multibyte literal
	)
	shuffles := map[string][]string{
		"desc":  {multi, eacute, ascii, newlineS, nul},
		"asc":   {nul, newlineU, ascii, eacute, multi},
		"mixed": {ascii, nul, multi, newlineS, eacute},
	}
	wantKeys := []string{"\x00z", "\nz", "az", "éz", "中z"}
	for name, tokens := range shuffles {
		t.Run(name, func(t *testing.T) {
			stdout, _, err := runAggregate(t, build(tokens), 1000)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			results := parseResultLines(t, stdout)
			if len(results) != len(wantKeys) {
				t.Fatalf("got %d results, want %d (the two newline spellings must merge):\n%s", len(results), len(wantKeys), stdout)
			}
			for i, want := range wantKeys {
				if results[i].Key != want {
					t.Errorf("position %d key = %q, want %q\nfull output:\n%s", i, results[i].Key, want, stdout)
				}
			}
		})
	}
}

// The legal/illegal boundary stays exactly where JSON draws it: a raw control
// character sitting inside the string (which for a raw newline also splits the
// record across two physical lines) and an escape JSON does not support are
// fatal record errors on the existing path, carrying the physical input line
// number. A newline produced by a legal escape occupies one physical line and
// therefore does not advance the line count; blank lines still count.
func TestAggregateRawControlAndUnsupportedEscapeAreFatal(t *testing.T) {
	cases := []struct {
		name     string
		record   string // the offending physical input (a raw newline case is two physical lines)
		wantLine int
	}{
		{"raw tab in string", "{\"type\":\"event\",\"key\":\"a\tb\",\"time\":1,\"value\":1}", 1},
		{"raw carriage return in string", "{\"type\":\"event\",\"key\":\"a\rb\",\"time\":1,\"value\":1}", 1},
		{"raw nul in string", "{\"type\":\"event\",\"key\":\"a\x00b\",\"time\":1,\"value\":1}", 1},
		{"raw newline in string splits the record", "{\"type\":\"event\",\"key\":\"a\nb\",\"time\":1,\"value\":1}", 1},
		{"unsupported escape x", `{"type":"event","key":"a\xb","time":1,"value":1}`, 1},
		{"unsupported escape o", `{"type":"event","key":"a\ob","time":1,"value":1}`, 1},
		{"non-hex unicode escape", `{"type":"event","key":"a\u00g0b","time":1,"value":1}`, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			input := tc.record + "\n"
			stdout, stderr, err := runAggregate(t, input, 1000)
			inputErr, ok := err.(*InputError)
			if !ok {
				t.Fatalf("want *InputError, got %T: %v", err, err)
			}
			if inputErr.Line != tc.wantLine {
				t.Errorf("line = %d, want %d", inputErr.Line, tc.wantLine)
			}
			if !strings.Contains(inputErr.Reason, "invalid JSON") {
				t.Errorf("reason = %q, want the existing invalid JSON record reason", inputErr.Reason)
			}
			if stdout != "" || stderr != "" {
				t.Errorf("fatal first record must output nothing, got stdout=%q stderr=%q", stdout, stderr)
			}
		})
	}

	// Line accounting with prior output retained: a legal escaped newline in
	// the key is still one physical line and must not shift later numbers;
	// the blank line 3 counts; the fatal record is line 5; line 6 is never
	// read and later windows stay open.
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":100,"value":2}`,    // line 1
		`{"type":"event","key":"a\nb","time":200,"value":3}`, // line 2: legal escaped LF in key, one physical line
		``,                                 // line 3: blank, still counted
		`{"type":"watermark","time":1000}`, // line 4: closes [0,1000), both keys
		`{"type":"event","key":"o\ops","time":1,"value":1}`, // line 5: unsupported escape, fatal
		`{"type":"watermark","time":2000}`,                  // line 6: never read
	}, "\n") + "\n"
	stdout, stderr, err := runAggregate(t, input, 1000)
	if err == nil {
		t.Fatal("expected a fatal error for the unsupported escape")
	}
	inputErr, ok := err.(*InputError)
	if !ok {
		t.Fatalf("want *InputError, got %T: %v", err, err)
	}
	if inputErr.Line != 5 {
		t.Fatalf("line = %d, want 5 (legal key escapes add no input line; blank line 3 counts)", inputErr.Line)
	}
	if !strings.Contains(inputErr.Reason, "invalid JSON") {
		t.Fatalf("reason = %q, want the invalid JSON record reason", inputErr.Reason)
	}
	if stderr != "" {
		t.Errorf("fatal record produces no late notice, got %q", stderr)
	}
	// Exactly the two results closed at line 4 survive, including the key
	// whose newline came from a legal escape, proving it aggregated normally.
	results := parseResultLines(t, stdout)
	want := []AggregateResult{
		{Key: "a\nb", Start: 0, End: 1000, Count: 1, Sum: 3},
		{Key: "k", Start: 0, End: 1000, Count: 1, Sum: 2},
	}
	if len(results) != len(want) {
		t.Fatalf("got %d surviving results, want %d:\n%s", len(results), len(want), stdout)
	}
	for i := range want {
		if results[i] != want[i] {
			t.Errorf("surviving result %d = %+v, want %+v", i, results[i], want[i])
		}
	}
}

// parseResultLines splits stdout into its physical result lines and decodes
// each as a complete JSON AggregateResult. Any embedded raw line-control byte
// or extra/missing escaping surfaces here as a line count mismatch or a parse
// failure.
func parseResultLines(t *testing.T, stdout string) []AggregateResult {
	t.Helper()
	body := strings.TrimSuffix(stdout, "\n")
	if body == "" {
		return nil
	}
	var results []AggregateResult
	for i, line := range strings.Split(body, "\n") {
		var r AggregateResult
		if err := json.Unmarshal([]byte(line), &r); err != nil {
			t.Fatalf("physical output line %d is not a complete JSON object: %v\nraw stdout: %q", i+1, err, stdout)
		}
		results = append(results, r)
	}
	return results
}
