package worker

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/kafka-llm-gateway/gateway/internal/kafka"
	"github.com/kafka-llm-gateway/gateway/internal/observability"
	"github.com/kafka-llm-gateway/gateway/internal/protocol"
	"github.com/kafka-llm-gateway/gateway/internal/providers"
	"github.com/prometheus/client_golang/prometheus"
)

// fakeProvider emits a fixed sequence of raw SSE events.
type fakeProvider struct {
	chunks []string
}

func (f *fakeProvider) Name() string  { return "heavy" }
func (f *fakeProvider) Model() string { return "test-model" }
func (f *fakeProvider) Complete(ctx context.Context, req *providers.ChatRequest, events chan<- providers.ProviderEvent) error {
	for _, c := range f.chunks {
		events <- providers.ProviderEvent{Type: providers.EventRawSSE, Raw: c}
	}
	events <- providers.ProviderEvent{Type: providers.EventDone}
	close(events)
	return nil
}

func TestWorkerPublishesAllEvents(t *testing.T) {
	prod := kafka.NewFakeProducer()
	topics := kafka.Topics{
		Requests:  "llm.requests",
		Events:    "llm.events",
		Completed: "llm.completed",
		Failed:    "llm.failed",
		DLQ:       "llm.dlq",
	}
	reg := prometheus.NewRegistry()
	metrics := observability.NewMetrics(reg)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	prov := &fakeProvider{chunks: []string{
		`{"choices":[{"delta":{"reasoning_content":"step 1"}}]}`,
		`{"choices":[{"delta":{"reasoning_content":"step 2"}}]}`,
		`{"choices":[{"delta":{"content":"Hello"}}]}`,
		`{"choices":[{"delta":{"content":" world"}}]}`,
		`{"choices":[{"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":10,"completion_tokens":5,"total_tokens":15}}`,
	}}

	w := New(prod, prov, topics, metrics, log, 2, 3, 0)

	req := protocol.Request{
		RequestID: "REQ1",
		Provider:  "heavy",
		Model:     "test-model",
		Payload:   []byte(`{"model":"test-model","messages":[{"role":"user","content":"hi"}]}`),
	}
	w.process(context.Background(), req)

	// Check events topic.
	events := prod.RecordsFor("llm.events")
	if len(events) == 0 {
		t.Fatal("no events published")
	}
	// First event should be started.
	var first protocol.Event
	if err := json.Unmarshal(events[0].Value, &first); err != nil {
		t.Fatalf("unmarshal first: %v", err)
	}
	if first.Type != protocol.TypeStarted {
		t.Errorf("first event type = %q, want started", first.Type)
	}
	// Last event should be completed.
	var last protocol.Event
	if err := json.Unmarshal(events[len(events)-1].Value, &last); err != nil {
		t.Fatalf("unmarshal last: %v", err)
	}
	if last.Type != protocol.TypeCompleted {
		t.Errorf("last event type = %q, want completed", last.Type)
	}
	// Verify reasoning is preserved in the completed payload.
	var cp protocol.CompletedPayload
	if err := json.Unmarshal(last.Payload, &cp); err != nil {
		t.Fatalf("unmarshal completed payload: %v", err)
	}
	if cp.Reasoning != "step 1step 2" {
		t.Errorf("reasoning = %q, want %q", cp.Reasoning, "step 1step 2")
	}
	if cp.Content != "Hello world" {
		t.Errorf("content = %q, want %q", cp.Content, "Hello world")
	}
	if !cp.ReasoningAvailable {
		t.Error("reasoning_available should be true")
	}
	if cp.FinishReason != "stop" {
		t.Errorf("finish_reason = %q", cp.FinishReason)
	}

	// Verify raw SSE events are preserved verbatim.
	var rawCount int
	for _, e := range events {
		var ev protocol.Event
		_ = json.Unmarshal(e.Value, &ev)
		if ev.Type == protocol.TypeRawSSE {
			rawCount++
		}
	}
	if rawCount != 5 {
		t.Errorf("raw sse events = %d, want 5", rawCount)
	}

	// Verify completion record.
	comps := prod.RecordsFor("llm.completed")
	if len(comps) != 1 {
		t.Fatalf("completion records = %d, want 1", len(comps))
	}
	var comp protocol.Completion
	if err := json.Unmarshal(comps[0].Value, &comp); err != nil {
		t.Fatalf("unmarshal completion: %v", err)
	}
	if comp.Content != "Hello world" {
		t.Errorf("completion content = %q", comp.Content)
	}
	if comp.Reasoning != "step 1step 2" {
		t.Errorf("completion reasoning = %q", comp.Reasoning)
	}
}

func TestWorkerSequenceMonotonic(t *testing.T) {
	prod := kafka.NewFakeProducer()
	topics := kafka.Topics{Events: "llm.events"}
	reg := prometheus.NewRegistry()
	metrics := observability.NewMetrics(reg)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	prov := &fakeProvider{chunks: []string{
		`{"choices":[{"delta":{"content":"a"}}]}`,
		`{"choices":[{"delta":{"content":"b"}}]}`,
	}}
	w := New(prod, prov, topics, metrics, log, 1, 1, 0)
	req := protocol.Request{RequestID: "REQ2", Provider: "heavy", Model: "m",
		Payload: []byte(`{"model":"m","messages":[{"role":"user","content":"x"}]}`)}
	w.process(context.Background(), req)

	var prev uint64
	for _, e := range prod.RecordsFor("llm.events") {
		var ev protocol.Event
		_ = json.Unmarshal(e.Value, &ev)
		if ev.Sequence <= prev {
			t.Errorf("sequence not monotonic: %d after %d", ev.Sequence, prev)
		}
		prev = ev.Sequence
	}
}

func TestWorkerFailedPublishesFailed(t *testing.T) {
	prod := kafka.NewFakeProducer()
	topics := kafka.Topics{Events: "llm.events", Failed: "llm.failed", DLQ: "llm.dlq"}
	reg := prometheus.NewRegistry()
	metrics := observability.NewMetrics(reg)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	// Provider that errors.
	prov := &errorProvider{err: providers.ErrNonRetryable}
	w := New(prod, prov, topics, metrics, log, 1, 1, 0)
	req := protocol.Request{RequestID: "REQ3", Provider: "heavy", Model: "m",
		Payload: []byte(`{"model":"m","messages":[{"role":"user","content":"x"}]}`)}
	w.process(context.Background(), req)

	if len(prod.RecordsFor("llm.failed")) != 1 {
		t.Errorf("failed records = %d, want 1", len(prod.RecordsFor("llm.failed")))
	}
	// Non-retryable should not go to DLQ.
	if len(prod.RecordsFor("llm.dlq")) != 0 {
		t.Errorf("dlq records = %d, want 0 for non-retryable", len(prod.RecordsFor("llm.dlq")))
	}
}

type errorProvider struct{ err error }

func (e *errorProvider) Name() string  { return "heavy" }
func (e *errorProvider) Model() string { return "m" }
func (e *errorProvider) Complete(ctx context.Context, req *providers.ChatRequest, events chan<- providers.ProviderEvent) error {
	events <- providers.ProviderEvent{Type: providers.EventError, Err: e.err}
	close(events)
	return e.err
}

var _ = strings.TrimSpace
