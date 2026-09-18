package streaming

import (
	"testing"

	"github.com/kafka-llm-gateway/gateway/internal/protocol"
)

func TestDispatcherDelivers(t *testing.T) {
	d := NewDispatcher(10)
	s := d.Register("req1")
	ev := protocol.Event{RequestID: "req1", Sequence: 1, Type: protocol.TypeContent}
	if !d.Dispatch(ev) {
		t.Fatal("expected dispatch to deliver")
	}
	select {
	case got := <-s.Ch:
		if got.Sequence != 1 {
			t.Errorf("sequence = %d", got.Sequence)
		}
	default:
		t.Fatal("expected event in channel")
	}
}

func TestDispatcherDropsWhenNoStream(t *testing.T) {
	d := NewDispatcher(10)
	ev := protocol.Event{RequestID: "missing", Sequence: 1}
	if d.Dispatch(ev) {
		t.Fatal("expected no delivery for unknown request")
	}
}

func TestDispatcherGet(t *testing.T) {
	d := NewDispatcher(10)
	s := d.Register("req1")
	got, ok := d.Get("req1")
	if !ok || got != s {
		t.Fatal("expected to retrieve the registered stream")
	}
}

func TestDispatcherDropsWhenFull(t *testing.T) {
	d := NewDispatcher(1)
	d.Register("req1")
	// Fill the buffer.
	ev := protocol.Event{RequestID: "req1", Sequence: 1}
	if !d.Dispatch(ev) {
		t.Fatal("first dispatch should succeed")
	}
	// Buffer full; next should drop.
	if d.Dispatch(ev) {
		t.Fatal("expected drop when buffer full")
	}
}

func TestDispatcherUnregister(t *testing.T) {
	d := NewDispatcher(10)
	d.Register("req1")
	d.Unregister("req1")
	if _, ok := d.Get("req1"); ok {
		t.Fatal("expected stream removed")
	}
}
