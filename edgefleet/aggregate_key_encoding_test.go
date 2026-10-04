package edgefleet

import (
	"bytes"
	"strings"
	"testing"
)

// TestDecodeKeyStrictTables covers the decoder directly: every legal JSON
// string decodes to exactly its Unicode value, while the damage the standard
// library rewrites to U+FFFD (lone surrogates, bad UTF-8) is rejected.
func TestDecodeKeyStrictTables(t *testing.T) {
	valid := []struct {
		raw  string
		want string
	}{
		{`""`, ""},
		{`"abc"`, "abc"},
		{`"é"`, "é"},
		{`"中文"`, "中文"},
		{`"😀"`, "😀"},
		{`"é"`, "é"},
		{`"\uD83D\uDE00"`, "😀"},     // matched surrogate pair
		{`"a\uD800\uDC00b"`, "a𐀀b"}, // first supplementary character U+10000
		{`"\uDBFF\uDFFF"`, "􏿿"},     // last supplementary character U+10FFFF
		{`"\uFFFD"`, "�"},           // explicit replacement character is a normal string
		{`"�"`, "�"},                // directly typed replacement character is valid
		{`"\\uD800"`, `\uD800`},     // escaped backslash: literal text, not a surrogate
		{`"\\uDC00"`, `\uDC00`},
		{`"\n\t\\\"\/\b\f\r"`, "\n\t\\\"/\b\f\r"},
		{`"퟿"`, "퟿"}, // code points adjacent to the surrogate range
	}
	for _, tc := range valid {
		got, err := decodeKeyStrict([]byte(tc.raw))
		if err != nil {
			t.Errorf("decodeKeyStrict(%s) unexpected error: %v", tc.raw, err)
			continue
		}
		if got != tc.want {
			t.Errorf("decodeKeyStrict(%s) = %q, want %q", tc.raw, got, tc.want)
		}
	}

	invalid := []string{
		`"\uD800"`,             // lone high surrogate
		`"\uDC00"`,             // lone low surrogate
		`"\uD800x"`,            // high surrogate followed by plain text
		`"\uD83Dx"`,            // high surrogate followed by plain text
		`"\uD83DA"`,            // high surrogate followed by a plain letter run
		`"\uD83D\uD800"`,       // high followed by high, not low
		`"\uDC00\uD800"`,       // reversed pair
		`"\uD83D\\"`,           // high followed by an escaped backslash at end
		`"\uD83D\uDE0"`,        // second escape truncated
		`"\u12"`,               // truncated hex digits
		`"\uGGGG"`,             // non-hex digits
		`"\q"`,                 // unknown escape
		`"ab` + "\xff" + `cd"`, // raw invalid UTF-8 byte
		`"` + "\xe4\xb8" + `"`, // truncated three-byte sequence
		`"` + "a\n" + `b"`,     // unescaped control character
		`"abc`,                 // unterminated
		`"abc"x`,               // trailing data
		`5`,                    // not a string
		``,                     // empty token
	}
	for _, raw := range invalid {
		if got, err := decodeKeyStrict([]byte(raw)); err == nil {
			t.Errorf("decodeKeyStrict(%s) = %q, want encoding/escape error", raw, got)
		}
	}
}

// TestAggregateDamagedKeyIsFatal exercises every public entry point: each
// kind of damaged key must come back as *InputError with the physical line
// number and a reason naming the key encoding or Unicode escapes.
func TestAggregateDamagedKeyIsFatal(t *testing.T) {
	events := []struct {
		name string
		line string
	}{
		{"lone high surrogate", `{"type":"event","key":"a\uD800","time":1,"value":1}`},
		{"lone low surrogate", `{"type":"event","key":"a\uDC00","time":1,"value":1}`},
		{"high surrogate then text", `{"type":"event","key":"\uD800x","time":1,"value":1}`},
		{"invalid utf8 byte", `{"type":"event","key":"ab` + "\xff" + `cd","time":1,"value":1}`},
	}
	entryPoints := []struct {
		name string
		run  func(input string) error
	}{
		{"fixed", func(input string) error {
			return RunAggregate(strings.NewReader(input), 1000, &bytes.Buffer{}, &bytes.Buffer{})
		}},
		{"sliding", func(input string) error {
			return RunAggregateSliding(strings.NewReader(input), 1000, 600, &bytes.Buffer{}, &bytes.Buffer{})
		}},
		{"partitioned", func(input string) error {
			return RunAggregatePartitioned(strings.NewReader(input), 1000, 2, &bytes.Buffer{}, &bytes.Buffer{})
		}},
	}
	for _, ep := range entryPoints {
		for _, ev := range events {
			t.Run(ep.name+"/"+ev.name, func(t *testing.T) {
				var input string
				if ep.name == "partitioned" {
					input = strings.TrimSuffix(ev.line, "}") + `,"partition":0}` + "\n"
				} else {
					input = ev.line + "\n"
				}
				err := ep.run(input)
				inputErr, ok := err.(*InputError)
				if !ok {
					t.Fatalf("want *InputError, got %T: %v", err, err)
				}
				if inputErr.Line != 1 {
					t.Errorf("line = %d, want 1", inputErr.Line)
				}
				reason := strings.ToLower(inputErr.Reason)
				if !strings.Contains(reason, "key") {
					t.Errorf("reason %q must name the key field", inputErr.Reason)
				}
				if !strings.Contains(reason, "utf-8") && !strings.Contains(reason, "unicode") && !strings.Contains(reason, "surrogate") {
					t.Errorf("reason %q must point at the character encoding or Unicode escapes", inputErr.Reason)
				}
			})
		}
	}
}

// TestAggregateDamagedKeyLineNumber: blank lines count toward the physical
// line number, and a fully written earlier result stays on output.
func TestAggregateDamagedKeyLineNumber(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":100,"value":5}`, // line 1
		``,                                 // line 2 blank, still counted
		`{"type":"watermark","time":1000}`, // line 3 closes [0,1000)
		`{"type":"event","key":"\uDC00","time":200,"value":99}`, // line 4 fatal
		`{"type":"watermark","time":9000}`,                      // line 5 must never be processed
	}, "\n") + "\n"
	var stdout, late bytes.Buffer
	err := RunAggregate(strings.NewReader(input), 1000, &stdout, &late)
	inputErr, ok := err.(*InputError)
	if !ok {
		t.Fatalf("want *InputError, got %T: %v", err, err)
	}
	if inputErr.Line != 4 {
		t.Fatalf("line = %d, want 4", inputErr.Line)
	}
	if got := stdout.String(); got != `{"key":"k","start":0,"end":1000,"count":1,"sum":5}`+"\n" {
		t.Fatalf("earlier result must be retained and no later window emitted, got %q", got)
	}
	if late.Len() != 0 {
		t.Fatalf("damaged key is fatal, not a late notice; late log = %q", late.String())
	}
}

// TestAggregateDamagedKeyBelowWatermarkIsFatal: even an event whose time is
// below the current watermark must be reported as an input error, not just
// logged as late and skipped.
func TestAggregateDamagedKeyBelowWatermarkIsFatal(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"watermark","time":5000}`,                    // line 1
		`{"type":"event","key":"x\uD800","time":1,"value":1}`, // line 2: late AND damaged
	}, "\n") + "\n"
	var stdout, late bytes.Buffer
	err := RunAggregate(strings.NewReader(input), 1000, &stdout, &late)
	if _, ok := err.(*InputError); !ok {
		t.Fatalf("want *InputError for damaged key below watermark, got %T: %v", err, err)
	}
	if late.Len() != 0 {
		t.Fatalf("damaged key must not produce a late-event notice, got %q", late.String())
	}
	if stdout.Len() != 0 {
		t.Fatalf("no window output expected, got %q", stdout.String())
	}
}

// TestAggregateDamagedKeyMutatesNoWindow: processing the fatal record must
// leave every open window exactly as it was -- the damaged bytes are never
// folded into a real U+FFFD key.
func TestAggregateDamagedKeyMutatesNoWindow(t *testing.T) {
	s := &aggregateState{
		windowMillis: 1000,
		slideMillis:  1000,
		windows:      make(map[windowID]*windowState),
		out:          &bytes.Buffer{},
		lateLog:      &bytes.Buffer{},
	}
	if err := s.processLine(`{"type":"event","key":"�","time":100,"value":10}`, 1); err != nil {
		t.Fatalf("valid U+FFFD key: %v", err)
	}
	if err := s.processLine(`{"type":"event","key":"\uD800","time":200,"value":99}`, 2); err == nil {
		t.Fatal("lone surrogate key must be fatal")
	}
	st := s.windows[windowID{start: 0, key: "�"}]
	if st == nil {
		t.Fatal("the real U+FFFD window must still exist")
	}
	if st.count != 1 || st.sum != 10 {
		t.Fatalf("U+FFFD window = count %d sum %d, want 1 and 10; damaged event changed state", st.count, st.sum)
	}
	if len(s.windows) != 1 {
		t.Fatalf("want exactly one open window, got %d: %v", len(s.windows), s.windows)
	}
}

// TestAggregateValidUnicodeKeysAggregate: legal characters aggregate by their
// decoded value, so a directly typed supplementary character and its matched
// surrogate-pair escape land in one key; literal U+FFFD (typed or escaped),
// CJK text and the literal text \uD800 are distinct legal keys; ordering keeps
// the UTF-8 byte order.
func TestAggregateValidUnicodeKeysAggregate(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"😀","time":100,"value":1}`,
		`{"type":"event","key":"\uD83D\uDE00","time":200,"value":2}`, // same key via surrogate pair
		`{"type":"event","key":"中","time":100,"value":5}`,
		`{"type":"event","key":"\\uD800","time":100,"value":7}`, // literal text \uD800
		`{"type":"event","key":"�","time":100,"value":11}`,
		`{"type":"event","key":"\uFFFD","time":100,"value":13}`, // same key as the typed U+FFFD
		`{"type":"event","key":"é","time":100,"value":1}`,
		`{"type":"event","key":"\u00E9","time":200,"value":4}`, // same key via escape
		`{"type":"watermark","time":1000}`,
	}, "\n") + "\n"
	var stdout, late bytes.Buffer
	if err := RunAggregate(strings.NewReader(input), 1000, &stdout, &late); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if late.Len() != 0 {
		t.Fatalf("unexpected late notices: %q", late.String())
	}
	want := strings.Join([]string{
		// key \uD800 (literal text, ASCII 0x5C) sorts first
		`{"key":"\\uD800","start":0,"end":1000,"count":1,"sum":7}`,
		`{"key":"é","start":0,"end":1000,"count":2,"sum":5}`,
		`{"key":"中","start":0,"end":1000,"count":1,"sum":5}`,
		`{"key":"�","start":0,"end":1000,"count":2,"sum":24}`,
		`{"key":"😀","start":0,"end":1000,"count":2,"sum":3}`,
		``,
	}, "\n")
	if got := stdout.String(); got != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", got, want)
	}
}

// TestAggregateSlidingValidEmojiCountsInEveryWindow proves the slide math is
// unchanged for valid supplementary-plane keys.
func TestAggregateSlidingValidEmojiCountsInEveryWindow(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"😀","time":700,"value":2}`,
		`{"type":"event","key":"😀","time":800,"value":3}`,
		`{"type":"watermark","time":1600}`,
	}, "\n") + "\n"
	var stdout, late bytes.Buffer
	if err := RunAggregateSliding(strings.NewReader(input), 1000, 600, &stdout, &late); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := strings.Join([]string{
		`{"key":"😀","start":0,"end":1000,"count":2,"sum":5}`,
		`{"key":"😀","start":600,"end":1600,"count":2,"sum":5}`,
		``,
	}, "\n")
	if got := stdout.String(); got != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", got, want)
	}
}

// TestAggregatePartitionedUnicodeKeysMergeAcrossPartitions: the same
// supplementary character arriving from different partitions merges into one
// count and sum per window; a damaged key is fatal with its line number.
func TestAggregatePartitionedUnicodeKeysMergeAcrossPartitions(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"😀","time":100,"value":1,"partition":0}`,
		`{"type":"event","key":"😀","time":200,"value":2,"partition":1}`,
		`{"type":"watermark","time":1000,"partition":0}`,
		`{"type":"watermark","time":1000,"partition":1}`,
		``, // blank line 5 counts
		`{"type":"event","key":"\uD800","time":300,"value":9,"partition":0}`, // line 6 fatal
		`{"type":"watermark","time":9000,"partition":0}`,                     // never processed
	}, "\n") + "\n"
	var stdout, late bytes.Buffer
	err := RunAggregatePartitioned(strings.NewReader(input), 1000, 2, &stdout, &late)
	inputErr, ok := err.(*InputError)
	if !ok {
		t.Fatalf("want *InputError, got %T: %v", err, err)
	}
	if inputErr.Line != 6 {
		t.Fatalf("line = %d, want 6", inputErr.Line)
	}
	want := `{"key":"😀","start":0,"end":1000,"count":2,"sum":3}` + "\n"
	if got := stdout.String(); got != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", got, want)
	}
}
