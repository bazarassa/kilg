package jobs

import (
	"testing"

	"github.com/kafka-llm-gateway/gateway/internal/protocol"
)

func TestManagerApplyLifecycle(t *testing.T) {
	m := NewManager(NewMemoryStore())
	m.Ensure("REQ", "heavy", "model")

	// queued
	ev, _ := protocol.NewEvent("REQ", 0, protocol.TypeQueued, map[string]string{})
	m.Apply(ev)
	j, _ := m.Store().Get("REQ")
	if j.Status != protocol.StatusQueued {
		t.Errorf("status = %q", j.Status)
	}

	// started
	ev, _ = protocol.NewEvent("REQ", 1, protocol.TypeStarted, map[string]string{})
	m.Apply(ev)
	j, _ = m.Store().Get("REQ")
	if j.Status != protocol.StatusRunning {
		t.Errorf("status = %q", j.Status)
	}

	// completed
	ev, _ = protocol.NewEvent("REQ", 2, protocol.TypeCompleted, protocol.CompletedPayload{
		FinishReason: "stop", Content: "hi", Reasoning: "think", ReasoningAvailable: true,
	})
	terminal := m.Apply(ev)
	if !terminal {
		t.Error("expected terminal after completed")
	}
	j, _ = m.Store().Get("REQ")
	if j.Status != protocol.StatusCompleted {
		t.Errorf("status = %q", j.Status)
	}
	if j.Content != "hi" || j.Reasoning != "think" {
		t.Errorf("content/reasoning = %q/%q", j.Content, j.Reasoning)
	}
	if j.Sequence != 2 {
		t.Errorf("sequence = %d", j.Sequence)
	}
}

func TestManagerMonotonicSequence(t *testing.T) {
	m := NewManager(NewMemoryStore())
	m.Ensure("REQ", "heavy", "model")
	m.Apply(mustEvent("REQ", 5, protocol.TypeContent))
	m.Apply(mustEvent("REQ", 3, protocol.TypeContent)) // out of order, lower
	j, _ := m.Store().Get("REQ")
	if j.Sequence != 5 {
		t.Errorf("sequence should stay at max: %d", j.Sequence)
	}
}

func mustEvent(id string, seq uint64, typ string) protocol.Event {
	ev, _ := protocol.NewEvent(id, seq, typ, map[string]string{})
	return ev
}
