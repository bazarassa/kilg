package streaming

import (
	"bufio"
	"errors"
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/kafka-llm-gateway/gateway/internal/protocol"
)

type nopFlusher struct{}

func (nopFlusher) Flush() {}

func TestWriteEventWithID(t *testing.T) {
	rec := httptest.NewRecorder()

	err := WriteEvent(rec, nopFlusher{}, "01K7REQ-0000000005",
		`{"a":1}`)
	if err != nil {
		t.Fatalf("WriteEvent: %v", err)
	}

	body := rec.Body.String()

	if !strings.HasPrefix(body, "id: 01K7REQ-0000000005\n") {
		t.Errorf("body missing id line: %q", body)
	}
	if !strings.Contains(body, "data: {\"a\":1}\n\n") {
		t.Errorf("body missing data line: %q", body)
	}
}

func TestWriteEventMultiLineData(t *testing.T) {
	rec := httptest.NewRecorder()

	err := WriteEvent(rec, nil, "", "line1\nline2")
	if err != nil {
		t.Fatalf("WriteEvent: %v", err)
	}

	want := "data: line1\ndata: line2\n\n"
	if rec.Body.String() != want {
		t.Errorf("body = %q, want %q", rec.Body.String(), want)
	}
}

func TestWriteDone(t *testing.T) {
	rec := httptest.NewRecorder()
	if err := WriteDone(rec, nil); err != nil {
		t.Fatalf("WriteDone: %v", err)
	}
	if rec.Body.String() != "data: [DONE]\n\n" {
		t.Errorf("body = %q", rec.Body.String())
	}
}

func TestSSEHeaders(t *testing.T) {
	rec := httptest.NewRecorder()
	SSEHeaders(rec)
	if got := rec.Header().Get("Content-Type"); got != "text/event-stream" {
		t.Errorf("content-type = %q", got)
	}
	if got := rec.Header().Get("Cache-Control"); got != "no-cache" {
		t.Errorf("cache-control = %q", got)
	}
}

func TestStreamCloseIsIdempotent(t *testing.T) {
	d := NewDispatcher(8)
	s := d.Register("REQ")

	s.Close()
	s.Close()

	select {
	case <-s.Closed:
	default:
		t.Fatal("Closed channel must be closed")
	}

	// Unregistering a closed stream must not panic.
	d.Unregister("REQ")
}

func TestDispatcherDefaultBuffer(t *testing.T) {
	d := NewDispatcher(0)
	s := d.Register("REQ")
	if cap(s.Ch) != 1024 {
		t.Errorf("buffer = %d, want 1024", cap(s.Ch))
	}
}

func TestDispatcherDispatchAfterUnregister(t *testing.T) {
	d := NewDispatcher(8)
	d.Register("REQ")
	d.Unregister("REQ")

	ev, _ := protocol.NewEvent("REQ", 1, protocol.TypeStarted,
		map[string]string{})
	if d.Dispatch(ev) {
		t.Error("dispatch after unregister must return false")
	}
}

func TestDispatcherRegisterReplacesStream(t *testing.T) {
	d := NewDispatcher(8)
	old := d.Register("REQ")
	_ = old
	fresh := d.Register("REQ")

	ev, _ := protocol.NewEvent("REQ", 1, protocol.TypeStarted,
		map[string]string{})
	if !d.Dispatch(ev) {
		t.Fatal("dispatch must succeed for the fresh stream")
	}

	select {
	case got := <-fresh.Ch:
		if got.Sequence != 1 {
			t.Errorf("sequence = %d", got.Sequence)
		}
	default:
		t.Error("fresh stream did not receive the event")
	}
}

func TestParseSEETraillingWithoutBlankLine(t *testing.T) {
	reader := bufio.NewReader(
		strings.NewReader("data: tail"),
	)

	data, err := ParseSSE(reader)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v, want io.EOF", err)
	}
	if data != "tail" {
		t.Errorf("data = %q, want tail", data)
	}
}
