package main

import (
	"bytes"
	"encoding/json"
	"sort"
	"strings"
	"testing"
)

// These tests drive the real aggregate command end to end to guard what a
// legal JSON key escape means on the wire: reading the record, grouping by the
// decoded key within one fixed window, and writing one JSON result per
// physical output line. The same character written with different legal
// escapes must merge; a literal backslash-n must stay distinct; and keys
// carrying line-control characters must still occupy exactly one output line
// that reparses to the original key. Damaged input stays on the existing
// fatal path with the physical input line number.

// TestAggregateCLIKeyEscapeSpellingsMerge is the headline case at window
// length 1000ms with every event time in the first window: a real newline
// written with the short escape and with the Unicode escape is the same key,
// so its two events (values 2 and 3) close as count=2, sum=5. A key that
// actually stores backslash and 'n' is a different key and must not be decoded
// a second time into the newline key; a plain slash and an escaped slash are
// likewise one key.
func TestAggregateCLIKeyEscapeSpellingsMerge(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"a\nb","time":100,"value":2}`,     // real newline, short escape
		`{"type":"event","key":"a\u000ab","time":200,"value":3}`, // same key, Unicode escape
		`{"type":"event","key":"a\\nb","time":300,"value":7}`,    // backslash + 'n': separate key
		`{"type":"event","key":"p/q","time":400,"value":4}`,      // plain slash
		`{"type":"event","key":"p\/q","time":500,"value":9}`,     // escaped slash: same key
		`{"type":"watermark","time":1000}`,
	}, "\n") + "\n"

	cmd := aggregateCommand(t, input, "--window-ms", "1000")
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("aggregate failed: %v\nstderr: %s", err, stderr.String())
	}
	if code := cmd.ProcessState.ExitCode(); code != 0 {
		t.Fatalf("exit code = %d, want 0", code)
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}

	assertWindowLines(t, stdout.String(), []windowLine{
		{Key: "a\nb", Start: 0, End: 1000, Count: 2, Sum: 5},
		{Key: `a\nb`, Start: 0, End: 1000, Count: 1, Sum: 7},
		{Key: "p/q", Start: 0, End: 1000, Count: 2, Sum: 13},
	})

	// The newline key's output keeps the newline escaped, so it is one
	// physical line; the literal backslash-n key shows a doubled backslash and
	// can never be confused with the first line.
	want := strings.Join([]string{
		`{"key":"a\nb","start":0,"end":1000,"count":2,"sum":5}`,
		`{"key":"a\\nb","start":0,"end":1000,"count":1,"sum":7}`,
		`{"key":"p/q","start":0,"end":1000,"count":2,"sum":13}`,
		``,
	}, "\n")
	if stdout.String() != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout.String(), want)
	}
}

// TestAggregateCLIKeyWithLineControlsIsOnePhysicalLine proves that special
// characters inside a key -- newline, carriage return, tab, quote, NUL and
// trailing/consecutive backslashes -- neither delete nor replace characters
// and cannot split one result across multiple physical output lines. Each
// output line is complete JSON that decodes back to the exact input key, and
// several keys closing together stay in decoded-key UTF-8 byte order.
func TestAggregateCLIKeyWithLineControlsIsOnePhysicalLine(t *testing.T) {
	type row struct {
		token string // raw JSON string token, quotes included
		key   string // decoded key expected back on output
	}
	rows := []row{
		{`"a\nb"`, "a\nb"},
		{`"c\rd"`, "c\rd"},
		{`"e\tf"`, "e\tf"},
		{`"g\"h"`, `g"h`},
		{`"i\\j"`, `i\j`},
		{`"dd\\\\"`, `dd\\`},   // two trailing backslashes
		{`"r\n"`, "r\n"},       // trailing newline
		{`"t\r"`, "t\r"},       // trailing carriage return
		{`"s\u0000"`, "s\x00"}, // trailing NUL
	}
	var lines []string
	for _, r := range rows {
		lines = append(lines, `{"type":"event","key":`+r.token+`,"time":100,"value":1}`)
	}
	lines = append(lines, `{"type":"watermark","time":1000}`)
	input := strings.Join(lines, "\n") + "\n"

	cmd := aggregateCommand(t, input, "--window-ms", "1000")
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("aggregate failed: %v\nstderr: %s", err, stderr.String())
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}

	got := stdout.String()
	// Exactly one trailing newline per result: a raw LF or CR inside a key
	// would split a result and raise this physical-line count.
	if n := strings.Count(got, "\n"); n != len(rows) {
		t.Fatalf("stdout has %d physical line(s), want %d (one result each):\n%q", n, len(rows), got)
	}
	physical := strings.Split(strings.TrimSuffix(got, "\n"), "\n")
	if len(physical) != len(rows) {
		t.Fatalf("got %d physical lines, want %d", len(physical), len(rows))
	}
	// No physical result line carries a raw control byte; every one is full
	// JSON that re-marshals to identical canonical bytes.
	var decoded []windowLine
	for i, text := range physical {
		for _, c := range []byte(text) {
			if c < 0x20 {
				t.Errorf("physical line %d has raw control byte 0x%02x:\n%q", i+1, c, text)
			}
		}
		var wl windowLine
		if err := json.Unmarshal([]byte(text), &wl); err != nil {
			t.Fatalf("physical line %d is not complete JSON: %v\n%q", i+1, err, text)
		}
		decoded = append(decoded, wl)
	}

	// Expected order is decoded-key UTF-8 byte order; sort a copy of the
	// input keys to derive it rather than hard-coding it.
	wantKeys := make([]string, len(rows))
	for i, r := range rows {
		wantKeys[i] = r.key
	}
	sort.Strings(wantKeys)
	for i, wl := range decoded {
		if wl.Key != wantKeys[i] {
			t.Errorf("position %d key = %q, want %q (decoded UTF-8 order %q)", i, wl.Key, wantKeys[i], wantKeys)
		}
		if wl != (windowLine{Key: wl.Key, Start: 0, End: 1000, Count: 1, Sum: 1}) {
			t.Errorf("result for key %q = %+v, want count=1 sum=1 in [0,1000)", wl.Key, wl)
		}
	}
}

// TestAggregateCLIRawControlAndBadEscapeAreFatal keeps the legal/corrupt
// boundary where JSON draws it: a raw control character inside the string (a
// raw newline also breaks the record across two physical lines) and an escape
// JSON does not support are fatal, report the physical input line and reason,
// exit 1, and stop before later records, while results already output remain.
// A newline obtained through a legal escape is one physical line and does not
// move later line numbers; the blank line still counts.
func TestAggregateCLIRawControlAndBadEscapeAreFatal(t *testing.T) {
	cases := []struct {
		name   string
		record string
	}{
		{"raw tab", "{\"type\":\"event\",\"key\":\"a\tb\",\"time\":1,\"value\":1}"},
		{"raw carriage return", "{\"type\":\"event\",\"key\":\"a\rb\",\"time\":1,\"value\":1}"},
		{"raw newline splits record", "{\"type\":\"event\",\"key\":\"a\nb\",\"time\":1,\"value\":1}"},
		{"unsupported escape", `{"type":"event","key":"a\xb","time":1,"value":1}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Line 1 closes a healthy window; line 2 is blank; the bad record
			// is physical line 3; line 4 must never be read.
			input := strings.Join([]string{
				`{"type":"event","key":"ok","time":100,"value":2}`, // line 1
				`{"type":"watermark","time":1000}`,                 // line 2 closes [0,1000)
				``,                                                 // line 3 blank, still counted
				tc.record,                                          // line 4: corrupt
				`{"type":"watermark","time":2000}`,                 // line 5: never read
			}, "\n") + "\n"

			cmd := aggregateCommand(t, input, "--window-ms", "1000")
			var stdout, stderr bytes.Buffer
			cmd.Stdout = &stdout
			cmd.Stderr = &stderr
			if err := cmd.Run(); err == nil {
				t.Fatal("aggregate succeeded, want exit code 1 for corrupt input")
			}
			if code := cmd.ProcessState.ExitCode(); code != 1 {
				t.Fatalf("exit code = %d, want 1", code)
			}
			errText := stderr.String()
			if !strings.Contains(errText, "line 4") {
				t.Errorf("stderr = %q, want it to name physical line 4 (blank line 3 counts)", errText)
			}
			// The healthy window closed on line 2 is retained verbatim.
			assertWindowLines(t, stdout.String(), []windowLine{
				{Key: "ok", Start: 0, End: 1000, Count: 1, Sum: 2},
			})
		})
	}

	// A legal escaped newline in a key stays one physical line, so the later
	// corrupt record lands on line 5 (not shifted), and its own earlier result
	// is retained.
	input := strings.Join([]string{
		`{"type":"event","key":"a\nb","time":100,"value":3}`, // line 1: legal escaped LF, one physical line
		``,                                 // line 2: blank, counts
		`{"type":"watermark","time":1000}`, // line 3 closes [0,1000)
		`{"type":"event","key":"x","time":1100,"value":1}`,  // line 4: later window, stays open
		`{"type":"event","key":"o\ops","time":1,"value":1}`, // line 5: unsupported escape, fatal
		`{"type":"watermark","time":2000}`,                  // line 6 never read
	}, "\n") + "\n"
	cmd := aggregateCommand(t, input, "--window-ms", "1000")
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err == nil {
		t.Fatal("aggregate succeeded, want exit code 1 for the unsupported escape")
	}
	if code := cmd.ProcessState.ExitCode(); code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "line 5") {
		t.Fatalf("stderr = %q, want physical line 5 (a legal key escape adds no input line)", stderr.String())
	}
	assertWindowLines(t, stdout.String(), []windowLine{
		{Key: "a\nb", Start: 0, End: 1000, Count: 1, Sum: 3},
	})
}
