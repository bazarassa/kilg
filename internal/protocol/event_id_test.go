package protocol

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestNewEventIDFormat(t *testing.T) {
	ev, err := NewEvent("REQ", 5, TypeStarted, map[string]string{})
	if err != nil {
		t.Fatalf("NewEvent: %v", err)
	}
	// id = request_id + "-" + 10-digit zero-padded sequence.
	if ev.EventID != "REQ-0000000005" {
		t.Errorf("event id = %q, want REQ-0000000005", ev.EventID)
	}

	big, err := NewEvent("R", 12345678901, TypeContent, map[string]string{})
	if err != nil {
		t.Fatalf("NewEvent: %v", err)
	}
	if !strings.HasSuffix(big.EventID, "-12345678901") {
		t.Errorf("event id = %q, want sequence suffix", big.EventID)
	}
}

func TestNewEventSetsMetadata(t *testing.T) {
	ev, err := NewEvent("REQ", 1, TypeContent, map[string]string{"text": "x"})
	if err != nil {
		t.Fatalf("NewEvent: %v", err)
	}
	if ev.RequestID != "REQ" {
		t.Errorf("request id = %q", ev.RequestID)
	}
	if ev.Sequence != 1 {
		t.Errorf("sequence = %d", ev.Sequence)
	}
	if ev.Timestamp.IsZero() {
		t.Error("timestamp must be set")
	}
	if ev.Type != TypeContent {
		t.Errorf("type = %q", ev.Type)
	}
	if ev.Payload == nil {
		t.Error("payload must be serialized")
	}
}

func TestNewEventMarshalError(t *testing.T) {
	_, err := NewEvent("REQ", 1, TypeContent, make(chan int))
	if err == nil {
		t.Fatal("expected marshal error for unserializable payload")
	}
}

func TestNewEventKnownTypes(t *testing.T) {
	types := []string{
		TypeQueued, TypeStarted, TypeContent, TypeCompleted,
		TypeFailed, TypeCancelled, TypeRawSSE,
	}
	for _, typ := range types {
		ev, err := NewEvent("REQ", 1, typ, map[string]string{})
		if err != nil {
			t.Errorf("NewEvent %s: %v", typ, err)
			continue
		}
		if ev.Type != typ {
			t.Errorf("type = %q, want %q", ev.Type, typ)
		}
	}
}

func TestChatRequestStreamRoundTrip(t *testing.T) {
	in := `{"model":"m","messages":[{"role":"user","content":"hi"}],"stream":true}`
	var r ChatRequest
	if err := json.Unmarshal([]byte(in), &r); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !r.Stream {
		t.Error("stream = false, want true")
	}
	out, err := json.Marshal(r)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var back ChatRequest
	if err := json.Unmarshal(out, &back); err != nil {
		t.Fatalf("re-unmarshal: %v", err)
	}
	if !back.Stream {
		t.Errorf("stream lost after round trip: %s", out)
	}
}
