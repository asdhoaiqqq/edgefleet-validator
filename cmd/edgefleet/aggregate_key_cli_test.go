package main

import (
	"bytes"
	"strings"
	"testing"
)

// TestAggregateCLIDamagedKeyIsFatal drives the command line with event keys
// whose character encoding or Unicode escape is damaged: invalid UTF-8 bytes
// and \u escapes with unpaired or mispaired surrogates. Each must print the
// physical input line number and the reason to standard error and exit with
// code 1, leaving already completed results on standard output and never
// downgrading the record to a late-event notice.
func TestAggregateCLIDamagedKeyIsFatal(t *testing.T) {
	cases := []struct {
		name      string
		key       string // raw JSON string literal, quotes included
		wantInMsg string
	}{
		{"invalid utf-8 bytes", "\"a\xffb\"", "UTF-8"},
		{"lone high surrogate", `"\uD800"`, "surrogate"},
		{"lone low surrogate", `"\uDC00"`, "surrogate"},
		{"high surrogate then text", `"\uD800x"`, "surrogate"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			input := strings.Join([]string{
				`{"type":"event","key":"ok","time":100,"value":2}`, // line 1
				`{"type":"watermark","time":1000}`,                 // line 2: closes [0,1000)
				``,                                                 // line 3: blank, still counted
				`{"type":"event","key":` + tc.key + `,"time":500,"value":1}`, // line 4: damaged, and below the watermark
				`{"type":"watermark","time":2000}`,                           // line 5: must never be read
			}, "\n") + "\n"

			cmd := aggregateCommand(t, input, "--window-ms", "1000")
			var stdout, stderr bytes.Buffer
			cmd.Stdout = &stdout
			cmd.Stderr = &stderr
			err := cmd.Run()
			if err == nil {
				t.Fatal("aggregate succeeded, want exit code 1 for a damaged key")
			}
			if code := cmd.ProcessState.ExitCode(); code != 1 {
				t.Fatalf("exit code = %d, want 1", code)
			}

			errText := stderr.String()
			for _, want := range []string{"line 4", `"key"`, tc.wantInMsg} {
				if !strings.Contains(errText, want) {
					t.Fatalf("stderr = %q, want it to mention %q", errText, want)
				}
			}
			if strings.Contains(errText, "late event") {
				t.Fatalf("damaged key must not be reported as an ordinary late event: stderr = %q", errText)
			}

			// Only the window closed before the fatal record appears; the
			// damaged event contributed nothing and line 5 was never read.
			assertWindowLines(t, stdout.String(), []windowLine{
				{Key: "ok", Start: 0, End: 1000, Count: 1, Sum: 2},
			})
		})
	}
}

// TestAggregateCLIGenuineReplacementCharKeyAccepted guards the other side of
// the boundary: a key that really is U+FFFD -- typed directly or written as
// the "�" escape -- is a legal string and both spellings merge into one
// key. Only damage that loses characters is fatal.
func TestAggregateCLIGenuineReplacementCharKeyAccepted(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"�","time":100,"value":2}`,
		"{\"type\":\"event\",\"key\":\"\\uFFFD\",\"time\":200,\"value\":3}", // escaped U+FFFD: same key
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
		{Key: "�", Start: 0, End: 1000, Count: 2, Sum: 5},
	})
}
