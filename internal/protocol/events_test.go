package protocol

import (
	"encoding/json"
	"testing"
)

func TestEventRoundTrip(t *testing.T) {
	ev, err := NewEvent("01K7ABCDEF123456789", 1532, TypeRawSSE, RawSSEPayload{Data: `{"choices":[{"delta":{"reasoning_content":"thinking"}}]}`})
	if err != nil {
		t.Fatalf("NewEvent: %v", err)
	}
	if ev.RequestID != "01K7ABCDEF123456789" {
		t.Errorf("RequestID = %q", ev.RequestID)
	}
	if ev.Sequence != 1532 {
		t.Errorf("Sequence = %d", ev.Sequence)
	}
	b, err := json.Marshal(ev)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var back Event
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if back.RequestID != ev.RequestID || back.Sequence != ev.Sequence || back.Type != ev.Type {
		t.Errorf("round trip mismatch: %+v vs %+v", back, ev)
	}
	// The raw payload must be preserved verbatim.
	var raw RawSSEPayload
	if err := json.Unmarshal(back.Payload, &raw); err != nil {
		t.Fatalf("payload: %v", err)
	}
	if raw.Data != `{"choices":[{"delta":{"reasoning_content":"thinking"}}]}` {
		t.Errorf("raw payload not preserved: %q", raw.Data)
	}
}

func TestEventIDStable(t *testing.T) {
	a, _ := NewEvent("REQ", 5, TypeContent, map[string]string{"delta": "x"})
	b, _ := NewEvent("REQ", 5, TypeContent, map[string]string{"delta": "x"})
	if a.EventID != b.EventID {
		t.Errorf("event id not stable: %q vs %q", a.EventID, b.EventID)
	}
}

func TestChatRequestPreservesUnknownFields(t *testing.T) {
	in := `{"model":"heavy","messages":[{"role":"user","content":"hi"}],"temperature":0.7,"custom_param":{"a":1}}`
	var r ChatRequest
	if err := json.Unmarshal([]byte(in), &r); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if r.Model != "heavy" {
		t.Errorf("model = %q", r.Model)
	}
	if _, ok := r.Extra["custom_param"]; !ok {
		t.Errorf("custom_param not preserved: %v", r.Extra)
	}
	out, err := json.Marshal(r)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(out, &m); err != nil {
		t.Fatalf("re-unmarshal: %v", err)
	}
	if _, ok := m["custom_param"]; !ok {
		t.Errorf("custom_param lost after round trip: %s", out)
	}
}

func TestChatMessagePreservesUnknownFields(t *testing.T) {
	in := `{"role":"assistant","content":"hi","tool_calls":[{"id":"x"}]}`
	var m ChatMessage
	if err := json.Unmarshal([]byte(in), &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, ok := m.Extra["tool_calls"]; !ok {
		t.Errorf("tool_calls not preserved: %v", m.Extra)
	}
	out, _ := json.Marshal(m)
	var mm map[string]json.RawMessage
	_ = json.Unmarshal(out, &mm)
	if _, ok := mm["tool_calls"]; !ok {
		t.Errorf("tool_calls lost: %s", out)
	}
}
