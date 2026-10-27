package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kafka-llm-gateway/gateway/internal/kafka"
	"github.com/kafka-llm-gateway/gateway/internal/observability"
	"github.com/kafka-llm-gateway/gateway/internal/protocol"
	"github.com/kafka-llm-gateway/gateway/internal/providers"
	"github.com/prometheus/client_golang/prometheus"
)

// testTopics returns the standard topic set used in tests.
func testTopics() kafka.Topics {
	return kafka.Topics{
		Requests:  "llm.requests",
		Events:    "llm.events",
		Completed: "llm.completed",
		Failed:    "llm.failed",
		DLQ:       "llm.dlq",
	}
}

func testMetrics() *observability.Metrics {
	return observability.NewMetrics(prometheus.NewRegistry())
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// fakeProvider is a SyncProvider with a configurable outcome.
type fakeProvider struct {
	mu   sync.Mutex
	n    int
	last *providers.ChatRequest
	resp *protocol.ChatResponse
	err  error
}

func (f *fakeProvider) Name() string { return "heavy" }

func (f *fakeProvider) Model() string { return "test-model" }

func (f *fakeProvider) CompleteSync(
	_ context.Context,
	req *providers.ChatRequest,
) (*protocol.ChatResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.n++
	f.last = req
	return f.resp, f.err
}

// callCount reports how many times the provider was invoked.
func (f *fakeProvider) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.n
}

// lastRequest reports the last request passed to the provider.
func (f *fakeProvider) lastRequest() *providers.ChatRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.last
}

func newTestWorker(
	t *testing.T,
	prod kafka.EventProducer,
	prov *fakeProvider,
	workers int,
	maxRetry int,
) *Worker {
	t.Helper()
	return New(prod, prov, testTopics(), testMetrics(), testLogger(),
		workers, maxRetry, time.Second)
}

func testRequest(id, model string) protocol.Request {
	return protocol.Request{
		RequestID: id,
		Provider:  "heavy",
		Model:     model,
		Payload: []byte(`{"model":"payload-model",
			"messages":[{"role":"user","content":"hi"}]}`),
	}
}

func stringRaw(s string) json.RawMessage {
	b, err := json.Marshal(s)
	if err != nil {
		panic(err)
	}
	return b
}

func syncResponse(content string) *protocol.ChatResponse {
	reason := "stop"
	return &protocol.ChatResponse{
		ID:     "cmpl-1",
		Object: "chat.completion",
		Model:  "test-model",
		Choices: []protocol.Choice{
			{
				Index: 0,
				Message: protocol.ChatMessage{
					Role:    "assistant",
					Content: stringRaw(content),
				},
				FinishReason: &reason,
			},
		},
		Usage: &protocol.Usage{
			PromptTokens:     10,
			CompletionTokens: 5,
			TotalTokens:      15,
		},
	}
}

// eventValues decodes all records of a topic into protocol.Event slices.
func eventValues(
	t *testing.T,
	prod *kafka.FakeProducer,
	topic string,
) []protocol.Event {
	t.Helper()
	recs := prod.RecordsFor(topic)
	out := make([]protocol.Event, 0, len(recs))
	for _, r := range recs {
		var ev protocol.Event
		if err := json.Unmarshal(r.Value, &ev); err != nil {
			t.Fatalf("unmarshal %s record: %v", topic, err)
		}
		out = append(out, ev)
	}
	return out
}

// expectStartedAndCompleted checks the standard two-event success sequence.
func expectStartedAndCompleted(t *testing.T, events []protocol.Event) {
	t.Helper()
	if len(events) != 2 {
		t.Fatalf("events = %d, want 2 (started + completed)", len(events))
	}
	if events[0].Type != protocol.TypeStarted || events[0].Sequence != 1 {
		t.Errorf("first event = %q seq %d, want started/1",
			events[0].Type, events[0].Sequence)
	}
	if events[1].Type != protocol.TypeCompleted || events[1].Sequence != 2 {
		t.Errorf("second event = %q seq %d, want completed/2",
			events[1].Type, events[1].Sequence)
	}
}

// completedPayloadOf decodes the completed event payload.
func completedPayloadOf(
	t *testing.T,
	ev protocol.Event,
) protocol.CompletedPayload {
	t.Helper()
	var cp protocol.CompletedPayload
	if err := json.Unmarshal(ev.Payload, &cp); err != nil {
		t.Fatalf("completed payload: %v", err)
	}
	return cp
}

// failedPayloadOf decodes the failed event payload.
func failedPayloadOf(t *testing.T, ev protocol.Event) protocol.FailedPayload {
	t.Helper()
	var fp protocol.FailedPayload
	if err := json.Unmarshal(ev.Payload, &fp); err != nil {
		t.Fatalf("failed payload: %v", err)
	}
	return fp
}

// expectCompletionRecord checks the llm.completed aggregate record.
func expectCompletionRecord(
	t *testing.T,
	prod *kafka.FakeProducer,
	reqID string,
	content string,
) {
	t.Helper()
	comps := prod.RecordsFor(testTopics().Completed)
	if len(comps) != 1 {
		t.Fatalf("completion records = %d, want 1", len(comps))
	}
	var comp protocol.Completion
	if err := json.Unmarshal(comps[0].Value, &comp); err != nil {
		t.Fatalf("unmarshal completion: %v", err)
	}
	if comp.Content != content {
		t.Errorf("completion content = %q", comp.Content)
	}
	if comp.Status != protocol.StatusCompleted {
		t.Errorf("status = %q", comp.Status)
	}
	if comp.RequestID != reqID {
		t.Errorf("request_id = %q", comp.RequestID)
	}
	if string(comps[0].Key) != reqID {
		t.Errorf("kafka key = %q, want request id", comps[0].Key)
	}
}

// expectDLQRecord checks one DLQ record with the original payload.
func expectDLQRecord(
	t *testing.T,
	prod *kafka.FakeProducer,
	reqID string,
	attempt int,
) {
	t.Helper()
	dlq := prod.RecordsFor(testTopics().DLQ)
	if len(dlq) != 1 {
		t.Fatalf("dlq records = %d, want 1", len(dlq))
	}
	var dr protocol.DLQRecord
	if err := json.Unmarshal(dlq[0].Value, &dr); err != nil {
		t.Fatalf("dlq record: %v", err)
	}
	if dr.RequestID != reqID || dr.Attempt != attempt {
		t.Errorf("dlq record = %+v", dr)
	}
	if len(dr.Original) == 0 {
		t.Error("dlq record must keep the original payload")
	}
}

func TestWorkerSuccessPublishesStartedAndCompleted(t *testing.T) {
	top := testTopics()
	prod := kafka.NewFakeProducer()
	prov := &fakeProvider{resp: syncResponse("Hello world")}
	w := newTestWorker(t, prod, prov, 2, 3)

	err := w.process(context.Background(), testRequest("REQ1", "test-model"))
	if err != nil {
		t.Fatalf("process: %v", err)
	}

	events := eventValues(t, prod, top.Events)
	expectStartedAndCompleted(t, events)

	cp := completedPayloadOf(t, events[1])
	if cp.Content != "Hello world" {
		t.Errorf("content = %q", cp.Content)
	}
	if cp.FinishReason != "stop" {
		t.Errorf("finish_reason = %q", cp.FinishReason)
	}
	if cp.Usage == nil {
		t.Error("usage missing in completed payload")
	}

	expectCompletionRecord(t, prod, "REQ1", "Hello world")

	if len(prod.RecordsFor(top.Failed)) != 0 {
		t.Error("failed records must be empty on success")
	}
}

func TestWorkerSequenceMonotonic(t *testing.T) {
	prod := kafka.NewFakeProducer()
	prov := &fakeProvider{resp: syncResponse("x")}
	w := newTestWorker(t, prod, prov, 1, 1)

	if err := w.process(context.Background(), testRequest("REQ2", "m")); err != nil {
		t.Fatalf("process: %v", err)
	}

	var prev uint64
	for _, ev := range eventValues(t, prod, testTopics().Events) {
		if ev.Sequence <= prev {
			t.Errorf("sequence not monotonic: %d after %d",
				ev.Sequence, prev)
		}
		prev = ev.Sequence
	}
}

func TestWorkerNonRetryableFailure(t *testing.T) {
	top := testTopics()
	prod := kafka.NewFakeProducer()
	prov := &fakeProvider{err: fmt.Errorf("%w: provider status 401",
		providers.ErrNonRetryable)}
	w := newTestWorker(t, prod, prov, 1, 3)

	// Terminal failure is published, so process reports success to the
	// consumer (offset can be committed).
	err := w.process(context.Background(), testRequest("REQ3", "m"))
	if err != nil {
		t.Fatalf("process: %v", err)
	}
	if prov.callCount() != 1 {
		t.Errorf("provider calls = %d, want 1", prov.callCount())
	}

	events := eventValues(t, prod, top.Events)
	if len(events) != 2 || events[1].Type != protocol.TypeFailed {
		t.Fatalf("events = %+v, want started + failed", events)
	}

	fp := failedPayloadOf(t, events[1])
	if fp.Retryable {
		t.Error("401 failure must be non-retryable")
	}
	if fp.Attempt != 1 {
		t.Errorf("attempt = %d, want 1", fp.Attempt)
	}

	if len(prod.RecordsFor(top.Failed)) != 1 {
		t.Errorf("failed records = %d, want 1",
			len(prod.RecordsFor(top.Failed)))
	}
	if len(prod.RecordsFor(top.DLQ)) != 0 {
		t.Errorf("dlq records = %d, want 0 for non-retryable",
			len(prod.RecordsFor(top.DLQ)))
	}
}

func TestWorkerRetryExhaustedPublishesDLQ(t *testing.T) {
	top := testTopics()
	prod := kafka.NewFakeProducer()
	prov := &fakeProvider{err: errors.New("connection refused")}
	w := newTestWorker(t, prod, prov, 1, 2)

	start := time.Now()
	err := w.process(context.Background(), testRequest("REQ4", "m"))
	if err != nil {
		t.Fatalf("process: %v", err)
	}

	// Two attempts: first fails, backoff, second fails and is terminal.
	if prov.callCount() != 2 {
		t.Errorf("provider calls = %d, want 2", prov.callCount())
	}
	if time.Since(start) < time.Second {
		t.Errorf("expected ~2s backoff between attempts")
	}

	events := eventValues(t, prod, top.Events)
	if len(events) != 3 {
		t.Fatalf("events = %d, want 3 (started x2 + failed)", len(events))
	}

	fp := failedPayloadOf(t, events[2])
	if !fp.Retryable {
		t.Error("retryable error must stay retryable in failed payload")
	}
	if fp.Attempt != 2 {
		t.Errorf("attempt = %d, want 2", fp.Attempt)
	}

	expectDLQRecord(t, prod, "REQ4", 2)
}

func TestWorkerContextCancelledDuringBackoff(t *testing.T) {
	top := testTopics()
	prod := kafka.NewFakeProducer()
	prov := &fakeProvider{err: errors.New("temporary failure")}
	w := newTestWorker(t, prod, prov, 1, 3)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(300 * time.Millisecond)
		cancel()
	}()

	err := w.process(ctx, testRequest("REQ5", "m"))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}

	// Terminal failure must NOT be published on cancellation.
	if len(prod.RecordsFor(top.Failed)) != 0 {
		t.Error("failed records must be empty on cancellation")
	}
	if len(prod.RecordsFor(top.DLQ)) != 0 {
		t.Error("dlq records must be empty on cancellation")
	}
}

// failingProducer wraps FakeProducer and fails publishes on one topic.
type failingProducer struct {
	inner     *kafka.FakeProducer
	failTopic string
}

func (p *failingProducer) Publish(
	ctx context.Context,
	topic string,
	key string,
	value []byte,
) error {
	if topic == p.failTopic {
		return fmt.Errorf("injected publish failure on %s", topic)
	}
	return p.inner.Publish(ctx, topic, key, value)
}

func (p *failingProducer) Close() error { return p.inner.Close() }

func TestWorkerFailedPublishFailureIsError(t *testing.T) {
	inner := kafka.NewFakeProducer()
	prod := &failingProducer{inner: inner, failTopic: testTopics().Failed}
	prov := &fakeProvider{err: fmt.Errorf("%w: bad request",
		providers.ErrNonRetryable)}
	w := newTestWorker(t, prod, prov, 1, 3)

	err := w.process(context.Background(), testRequest("REQ6", "m"))
	if err == nil {
		t.Fatal("expected error when failed topic publish fails")
	}
	if len(inner.RecordsFor(testTopics().DLQ)) != 0 {
		t.Error("dlq must not be published when failed publish fails")
	}
}

func TestWorkerUsesEnvelopeModel(t *testing.T) {
	prod := kafka.NewFakeProducer()
	prov := &fakeProvider{resp: syncResponse("ok")}
	w := newTestWorker(t, prod, prov, 1, 1)

	req := testRequest("REQ7", "envelope-model")
	if err := w.process(context.Background(), req); err != nil {
		t.Fatalf("process: %v", err)
	}

	got := prov.lastRequest()
	if got == nil {
		t.Fatal("provider was not called")
	}
	// The envelope model must win over the model inside the payload.
	if got.Model != "envelope-model" {
		t.Errorf("model = %q, want envelope-model", got.Model)
	}
	if got.Stream {
		t.Error("worker must force stream=false")
	}
	if string(got.Extra) != string(req.Payload) {
		t.Error("original payload must be preserved in Extra")
	}
	if len(got.Messages) != 1 || got.Messages[0].Role != "user" {
		t.Errorf("messages = %+v", got.Messages)
	}
}

func TestWorkerContentExtraction(t *testing.T) {
	// Content null must produce an empty content string.
	prod := kafka.NewFakeProducer()
	resp := syncResponse("x")
	resp.Choices[0].Message.Content = json.RawMessage("null")
	prov := &fakeProvider{resp: resp}
	w := newTestWorker(t, prod, prov, 1, 1)

	if err := w.process(context.Background(), testRequest("REQ8", "m")); err != nil {
		t.Fatalf("process: %v", err)
	}

	events := eventValues(t, prod, testTopics().Events)
	cp := completedPayloadOf(t, events[1])
	if cp.Content != "" {
		t.Errorf("content = %q, want empty for null content", cp.Content)
	}
}

func TestWorkerNoChoicesIsRetryable(t *testing.T) {
	prod := kafka.NewFakeProducer()
	prov := &fakeProvider{resp: &protocol.ChatResponse{Choices: nil}}
	w := newTestWorker(t, prod, prov, 1, 1)

	err := w.process(context.Background(), testRequest("REQ9", "m"))
	if err != nil {
		t.Fatalf("process: %v", err)
	}

	// maxRetry=1: single attempt, terminal failure, retryable flag on.
	events := eventValues(t, prod, testTopics().Events)
	if len(events) != 2 || events[1].Type != protocol.TypeFailed {
		t.Fatalf("events = %+v, want started + failed", events)
	}
	fp := failedPayloadOf(t, events[1])
	if !fp.Retryable {
		t.Error("no-choices error must be retryable")
	}
}

// fakeConsumer drives the worker handler with canned records.
type fakeConsumer struct {
	records []kafka.Record
	// closeErr is returned by Consume after all records were handled.
	closeErr error
}

func (c *fakeConsumer) Consume(
	_ context.Context,
	handler func(context.Context, kafka.Record) error,
) error {
	for _, rec := range c.records {
		if err := handler(context.Background(), rec); err != nil {
			return err
		}
	}
	return c.closeErr
}

func (c *fakeConsumer) Close() error { return nil }

func requestRecord(id string, value []byte) kafka.Record {
	return kafka.Record{
		Topic: "llm.requests",
		Key:   []byte(id),
		Value: value,
	}
}

func TestRunInvalidRecordGoesToDLQ(t *testing.T) {
	top := testTopics()
	prod := kafka.NewFakeProducer()
	prov := &fakeProvider{resp: syncResponse("ok")}
	w := newTestWorker(t, prod, prov, 1, 1)

	// Malformed JSON must go to DLQ without blocking the consumer.
	malformed := requestRecord("REQ-BAD", []byte("not-json"))
	emptyID := requestRecord("",
		[]byte(`{"provider":"heavy","model":"m","payload":{}}`))
	valid := requestRecord("REQ-OK",
		[]byte(`{"request_id":"REQ-OK","provider":"heavy",
			"model":"m","payload":{"model":"m","messages":[]}}`))

	consumer := &fakeConsumer{records: []kafka.Record{malformed, emptyID, valid}}

	err := w.Run(context.Background(), consumer)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	dlq := prod.RecordsFor(top.DLQ)
	if len(dlq) != 2 {
		t.Fatalf("dlq records = %d, want 2", len(dlq))
	}
	var dr protocol.DLQRecord
	if err := json.Unmarshal(dlq[0].Value, &dr); err != nil {
		t.Fatalf("dlq record: %v", err)
	}
	if dr.RequestID != "REQ-BAD" {
		t.Errorf("dlq request id = %q", dr.RequestID)
	}
	if dr.Attempt != 0 {
		t.Errorf("dlq attempt = %d, want 0 for malformed", dr.Attempt)
	}

	// The valid record must be processed normally.
	if len(prod.RecordsFor(top.Events)) != 2 {
		t.Errorf("events for valid record = %d, want 2",
			len(prod.RecordsFor(top.Events)))
	}
}

func TestRunHandlerErrorPropagates(t *testing.T) {
	inner := kafka.NewFakeProducer()
	prod := &failingProducer{inner: inner, failTopic: testTopics().Failed}
	prov := &fakeProvider{err: fmt.Errorf("%w: bad request",
		providers.ErrNonRetryable)}
	w := newTestWorker(t, prod, prov, 1, 1)

	value, err := json.Marshal(testRequest("REQ-ERR", "m"))
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}

	consumer := &fakeConsumer{records: []kafka.Record{
		requestRecord("REQ-ERR", value),
	}}

	err = w.Run(context.Background(), consumer)
	if err == nil {
		t.Fatal("expected error: failed publish failure must propagate")
	}
	if !strings.Contains(err.Error(), testTopics().Failed) {
		t.Errorf("err = %v, want mention of failed topic", err)
	}
}
