package streaming

import (
	"bufio"
	"strings"
	"testing"
)

func TestParseSSESingle(t *testing.T) {
	input := "data: {\"a\":1}\n\n"
	r := bufio.NewReader(strings.NewReader(input))
	data, err := ParseSSE(r)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if data != `{"a":1}` {
		t.Errorf("data = %q", data)
	}
}

func TestParseSSEMultiLine(t *testing.T) {
	input := "data: line1\ndata: line2\n\n"
	r := bufio.NewReader(strings.NewReader(input))
	data, _ := ParseSSE(r)
	if data != "line1\nline2" {
		t.Errorf("data = %q", data)
	}
}

func TestParseSSEIgnoresOtherFields(t *testing.T) {
	input := "event: message\nid: 42\ndata: hello\n\n"
	r := bufio.NewReader(strings.NewReader(input))
	data, _ := ParseSSE(r)
	if data != "hello" {
		t.Errorf("data = %q", data)
	}
}

func TestParseSSEDone(t *testing.T) {
	input := "data: [DONE]\n\n"
	r := bufio.NewReader(strings.NewReader(input))
	data, _ := ParseSSE(r)
	if data != "[DONE]" {
		t.Errorf("data = %q", data)
	}
}
