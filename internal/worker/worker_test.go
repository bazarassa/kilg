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

// fakeProvider is a streaming test provider: it replays the same
// script of provider events on every Complete call.
type fakeProvider struct {
	mu     sync.Mutex
	n      int
	last   *providers.ChatRequest
	script []providers.ProviderEvent
	err    error
	// delay before the first event (to force heartbeats).
	delay time.Duration
}

func (f *fakeProvider) Complete(
	ctx context.Context,
	req *providers.ChatRequest,
	events chan<- providers.ProviderEvent,
) error {
	// Real providers always close the channel when Complete returns.
	defer close(events)

	f.mu.Lock()
	f.n++
	f.last = req
	f.mu.Unlock()

	if f.delay > 0 {
		select {
		case <-time.After(f.delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	for _, ev := range f.script {
		select {
		case events <- ev:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return f.err
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

// failingProducer fails publishes to one topic, passes the rest.
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
		return fmt.Errorf("publish to %s failed", topic)
	}

	return p.inner.Publish(ctx, topic, key, value)
}

func (p *failingProducer) Close() error {
	return p.inner.Close()
}

// fakeConsumer replays a fixed set of records.
type fakeConsumer struct {
	records  []kafka.Record
	closeErr error
}

func (c *fakeConsumer) Consume(
	ctx context.Context,
	fn func(context.Context, kafka.Record) error,
) error {
	for _, rec := range c.records {
		if err := fn(ctx, rec); err != nil {
			return err
		}
	}

	return c.closeErr
}

func (c *fakeConsumer) Close() error { return nil }

func newTestWorker(
	t *testing.T,
	prod kafka.EventProducer,
	prov StreamProvider,
	workers int,
	maxRetry int,
	heartbeat time.Duration,
) *Worker {
	t.Helper()
	return New(prod, prov, testTopics(), testMetrics(), testLogger(),
		workers, maxRetry, heartbeat)
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

// rawEvent builds a raw SSE provider event with the given data.
func rawEvent(s string) providers.ProviderEvent {
	return providers.ProviderEvent{
		Type: providers.EventRawSSE,
		Raw:  s,
	}
}

// eventValues decodes all records of a topic into protocol.Event.
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
	reasoning string,
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
		t.Errorf("completion content = %q, want %q", comp.Content, content)
	}
	if comp.Reasoning != reasoning {
		t.Errorf("completion reasoning = %q, want %q",
			comp.Reasoning, reasoning)
	}
	if comp.Status != protocol.StatusCompleted {
		t.Errorf("status = %q", comp.Status)
	}
	if comp.RequestID != reqID {
		t.Errorf("request_id = %q, want %q", comp.RequestID, reqID)
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

// eventTypes returns the event type column of an event list.
func eventTypes(events []protocol.Event) []string {
	out := make([]string, len(events))

	for i, ev := range events {
		out[i] = ev.Type
	}

	return out
}

// expectEventSequence checks that event types and sequence numbers
// match the wanted column exactly (sequences 1..N).
func expectEventSequence(
	t *testing.T,
	events []protocol.Event,
	want []string,
) {
	t.Helper()
	if len(events) != len(want) {
		t.Fatalf("events = %v, want %v", eventTypes(events), want)
	}

	for i := range want {
		if events[i].Type != want[i] || events[i].Sequence != uint64(i+1) {
			t.Errorf("event %d = %q seq %d, want %q seq %d",
				i, events[i].Type, events[i].Sequence, want[i], i+1)
		}
	}
}

// Raw SSE-чанки success-сценария (verbatim, как отдаёт provider).
const (
	chunkHello = `{"id":"c1","choices":[{"index":0,
		"delta":{"role":"assistant","content":"Hello "}}]}`
	chunkWorld = `{"id":"c1","choices":[{"index":0,
		"delta":{"content":"world"}}]}`
	chunkDone = `{"id":"c1","choices":[{"index":0,
		"delta":{"reasoning_content":"thought"},
		"finish_reason":"stop"}],
		"usage":{"prompt_tokens":10,
		"completion_tokens":5,"total_tokens":15}}`
)

// successScript — полный успешный streaming-поток.
func successScript() []providers.ProviderEvent {
	return []providers.ProviderEvent{
		rawEvent(chunkHello),
		rawEvent(chunkWorld),
		rawEvent(chunkDone),
		{Type: providers.EventDone},
	}
}

// assertProviderRequest checks the request the provider received.
func assertProviderRequest(t *testing.T, prov *fakeProvider) {
	t.Helper()
	last := prov.lastRequest()
	if last.Model != "env-model" {
		t.Errorf("model = %q, want env-model (envelope wins)", last.Model)
	}
	if !last.Stream {
		t.Error("stream must be true on heavy path")
	}
	if len(last.Extra) == 0 {
		t.Error("Extra must preserve the original request payload")
	}
}

// assertCompletedPayload checks aggregated completed payload.
func assertCompletedPayload(t *testing.T, cp protocol.CompletedPayload) {
	t.Helper()
	if cp.Content != "Hello world" {
		t.Errorf("content = %q, want 'Hello world'", cp.Content)
	}

	if cp.Reasoning != "thought" {
		t.Errorf("reasoning = %q, want 'thought'", cp.Reasoning)
	}

	if !cp.ReasoningAvailable {
		t.Error("reasoning_available must be true")
	}

	if cp.FinishReason != "stop" {
		t.Errorf("finish_reason = %q, want stop", cp.FinishReason)
	}
	if !cp.Stream {
		t.Error("stream must be true in completed payload")
	}
	if cp.Events != 3 {
		t.Errorf("events = %d, want 3", cp.Events)
	}
	if !strings.Contains(string(cp.Usage), `"total_tokens":15`) {
		t.Errorf("usage = %s, want total_tokens 15", cp.Usage)
	}
}

func TestWorkerSuccessStreamsRawEvents(t *testing.T) {
	top := testTopics()
	prod := kafka.NewFakeProducer()
	prov := &fakeProvider{script: successScript()}
	w := newTestWorker(t, prod, prov, 2, 3, 0)

	err := w.process(context.Background(), testRequest("REQ1", "env-model"))
	if err != nil {
		t.Fatalf("process: %v", err)
	}

	if n := prov.callCount(); n != 1 {
		t.Fatalf("provider calls = %d, want 1", n)
	}

	assertProviderRequest(t, prov)

	events := eventValues(t, prod, top.Events)
	expectEventSequence(t, events, []string{
		protocol.TypeStarted,
		protocol.TypeRawSSE,
		protocol.TypeRawSSE,
		protocol.TypeRawSSE,
		protocol.TypeCompleted,
	})

	// Raw чанки сохраняются verbatim (PLAN §9).
	var raw1 protocol.RawSSEPayload
	if err := json.Unmarshal(events[1].Payload, &raw1); err != nil {
		t.Fatalf("raw payload: %v", err)
	}
	if raw1.Data != chunkHello {
		t.Errorf("raw data = %q, want verbatim chunk", raw1.Data)
	}

	assertCompletedPayload(t, completedPayloadOf(t, events[4]))

	expectCompletionRecord(t, prod, "REQ1", "Hello world", "thought")

	if n := len(prod.RecordsFor(top.Failed)); n != 0 {
		t.Errorf("failed records = %d, want 0", n)
	}
	if n := len(prod.RecordsFor(top.DLQ)); n != 0 {
		t.Errorf("dlq records = %d, want 0", n)
	}
}

func TestWorkerNullContentAggregatesEmpty(t *testing.T) {
	top := testTopics()
	prod := kafka.NewFakeProducer()
	prov := &fakeProvider{script: []providers.ProviderEvent{
		rawEvent(`{"id":"c1","choices":[{"index":0,"delta":{"content":null}}]}`),
		{Type: providers.EventDone},
	}}
	w := newTestWorker(t, prod, prov, 1, 1, 0)

	if err := w.process(context.Background(), testRequest("REQ1", "m")); err != nil {
		t.Fatalf("process: %v", err)
	}

	events := eventValues(t, prod, top.Events)
	if len(events) != 3 {
		t.Fatalf("events = %d, want 3", len(events))
	}

	cp := completedPayloadOf(t, events[2])
	if cp.Content != "" {
		t.Errorf("content = %q, want empty for null delta", cp.Content)
	}
}

func TestWorkerUnknownRawEventPreserved(t *testing.T) {
	top := testTopics()
	prod := kafka.NewFakeProducer()
	// Non-JSON raw line: aggregation must skip it, but the event
	// must be preserved in the durable transcript.
	prov := &fakeProvider{script: []providers.ProviderEvent{
		rawEvent("provider-specific line"),
		{Type: providers.EventDone},
	}}
	w := newTestWorker(t, prod, prov, 1, 1, 0)

	if err := w.process(context.Background(), testRequest("REQ1", "m")); err != nil {
		t.Fatalf("process: %v", err)
	}

	events := eventValues(t, prod, top.Events)
	if len(events) != 3 {
		t.Fatalf("events = %d, want 3", len(events))
	}

	var raw protocol.RawSSEPayload

	if err := json.Unmarshal(events[1].Payload, &raw); err != nil {
		t.Fatalf("raw payload: %v", err)
	}
	if raw.Data != "provider-specific line" {
		t.Errorf("raw data = %q", raw.Data)
	}
	cp := completedPayloadOf(t, events[2])
	if cp.Content != "" || cp.Reasoning != "" {
		t.Errorf("aggregation of non-JSON chunk must be empty, got %+v", cp)
	}
}

func TestWorkerSequenceMonotonicAcrossRetries(t *testing.T) {
	top := testTopics()
	prod := kafka.NewFakeProducer()
	// Mid-stream retryable failure on every attempt.
	prov := &fakeProvider{
		script: []providers.ProviderEvent{
			rawEvent(`{"id":"c1","choices":[{"index":0,"delta":{"content":"part "}}]}`),
			{Type: providers.EventError, Err: errors.New("network reset")},
		},
		err: errors.New("network reset"),
	}
	w := newTestWorker(t, prod, prov, 2, 2, 0)

	if err := w.process(context.Background(), testRequest("REQ1", "m")); err != nil {
		t.Fatalf("process: %v", err)
	}

	if n := prov.callCount(); n != 2 {
		t.Fatalf("provider calls = %d, want 2", n)
	}

	events := eventValues(t, prod, top.Events)
	// started(1) raw(2) started(3) raw(4) failed(5)
	expectEventSequence(t, events, []string{
		protocol.TypeStarted,
		protocol.TypeRawSSE,
		protocol.TypeStarted,
		protocol.TypeRawSSE,
		protocol.TypeFailed,
	})

	fp := failedPayloadOf(t, events[len(events)-1])
	if !fp.Retryable || fp.Attempt != 2 {
		t.Errorf("failed payload = %+v, want retryable attempt 2", fp)
	}

	expectDLQRecord(t, prod, "REQ1", 2)
}

func TestWorkerEmptyStreamFails(t *testing.T) {
	top := testTopics()
	prod := kafka.NewFakeProducer()
	prov := &fakeProvider{script: nil}
	w := newTestWorker(t, prod, prov, 2, 2, 0)

	if err := w.process(context.Background(), testRequest("REQ1", "m")); err != nil {
		t.Fatalf("process: %v", err)
	}

	if n := prov.callCount(); n != 2 {
		t.Fatalf("provider calls = %d, want 2", n)
	}

	events := eventValues(t, prod, top.Events)
	if got := eventTypes(events); len(got) != 3 {
		t.Fatalf("events = %v, want started, started, failed", got)
	}
	if events[2].Type != protocol.TypeFailed {
		t.Errorf("last event = %q, want failed", events[2].Type)
	}
	fp := failedPayloadOf(t, events[2])
	if !fp.Retryable {
		t.Error("empty stream must be retryable")
	}

	expectDLQRecord(t, prod, "REQ1", 2)
}

func TestWorkerNonRetryableNoRetryNoDLQ(t *testing.T) {
	top := testTopics()
	prod := kafka.NewFakeProducer()
	prov := &fakeProvider{
		err: fmt.Errorf("%w: provider status 401", providers.ErrNonRetryable),
	}
	w := newTestWorker(t, prod, prov, 2, 3, 0)

	if err := w.process(context.Background(), testRequest("REQ1", "m")); err != nil {
		t.Fatalf("process: %v", err)
	}

	if n := prov.callCount(); n != 1 {
		t.Fatalf("provider calls = %d, want 1 (no retry)", n)
	}

	events := eventValues(t, prod, top.Events)
	if got := eventTypes(events); len(got) != 2 {
		t.Fatalf("events = %v, want started + failed", got)
	}
	fp := failedPayloadOf(t, events[1])
	if fp.Retryable || fp.Attempt != 1 {
		t.Errorf("failed payload = %+v, want non-retryable attempt 1", fp)
	}

	failed := prod.RecordsFor(top.Failed)
	if len(failed) != 1 {
		t.Fatalf("llm.failed records = %d, want 1", len(failed))
	}
	if len(prod.RecordsFor(top.DLQ)) != 0 {
		t.Error("dlq records must be empty for non-retryable failure")
	}
}

func TestWorkerHeartbeatDuringSilentProvider(t *testing.T) {
	top := testTopics()
	prod := kafka.NewFakeProducer()
	prov := &fakeProvider{
		script: []providers.ProviderEvent{
			rawEvent(`{"id":"c1","choices":[{"index":0,"delta":{"content":"ok"}}]}`),
		},
		delay: 150 * time.Millisecond,
	}
	w := newTestWorker(t, prod, prov, 2, 3, 40*time.Millisecond)

	if err := w.process(context.Background(), testRequest("REQ1", "m")); err != nil {
		t.Fatalf("process: %v", err)
	}

	events := eventValues(t, prod, top.Events)
	var firstHBLastSeq uint64
	hbCount := 0

	for _, ev := range events {
		if ev.Type != protocol.TypeHeartbeat {
			continue
		}

		hbCount++

		if hbCount == 1 {
			var hb protocol.HeartbeatPayload

			if err := json.Unmarshal(ev.Payload, &hb); err != nil {
				t.Fatalf("heartbeat payload: %v", err)
			}

			firstHBLastSeq = hb.LastSequence
		}
	}
	if hbCount < 1 {
		t.Errorf("heartbeat count = %d, want >= 1 (provider silent 150ms, period 40ms)", hbCount)
	}
	if firstHBLastSeq != 1 {
		t.Errorf("first heartbeat last_sequence = %d, want 1 (only started published)", firstHBLastSeq)
	}
}

func TestWorkerContextCancelDuringBackoff(t *testing.T) {
	prod := kafka.NewFakeProducer()
	prov := &fakeProvider{err: errors.New("boom")}
	w := newTestWorker(t, prod, prov, 2, 5, 0)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- w.process(ctx, testRequest("REQ1", "m"))
	}()

	time.Sleep(300 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("err = %v, want context.Canceled", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("process did not return after cancel")
	}

	top := testTopics()
	if len(prod.RecordsFor(top.Failed)) != 0 {
		t.Error("failed records must be empty after cancel")
	}
	if len(prod.RecordsFor(top.DLQ)) != 0 {
		t.Error("dlq records must be empty after cancel")
	}
}

func TestWorkerFailedTopicPublishFailure(t *testing.T) {
	top := testTopics()
	prod := kafka.NewFakeProducer()
	prov := &fakeProvider{
		err: fmt.Errorf("%w: 401", providers.ErrNonRetryable),
	}
	w := newTestWorker(
		t,
		&failingProducer{inner: prod, failTopic: top.Failed},
		prov,
		2,
		3,
		0,
	)

	err := w.process(context.Background(), testRequest("REQ1", "m"))
	if err == nil {
		t.Fatal("process must fail when llm.failed publish fails")
	}
	if !strings.Contains(err.Error(), top.Failed) {
		t.Errorf("err = %v, want mention of %s", err, top.Failed)
	}
	if len(prod.RecordsFor(top.DLQ)) != 0 {
		t.Error("dlq records must be empty when failed publish fails")
	}
}

func TestRunInvalidRecordsGoToDLQ(t *testing.T) {
	top := testTopics()
	prod := kafka.NewFakeProducer()
	prov := &fakeProvider{script: []providers.ProviderEvent{
		rawEvent(`{"id":"c1","choices":[{"index":0,"delta":{"content":"ok"}}]}`),
	}}
	w := newTestWorker(t, prod, prov, 2, 3, 0)

	records := []kafka.Record{
		{
			Topic:     top.Requests,
			Key:       []byte("bad-1"),
			Value:     []byte("{invalid-json"),
			Partition: 0,
			Offset:    0,
		},
		{
			Topic:     top.Requests,
			Key:       []byte("bad-2"),
			Value:     []byte(`{"request_id":""}`),
			Partition: 0,
			Offset:    1,
		},
		{
			Topic:     top.Requests,
			Key:       []byte("REQ-ok"),
			Value:     mustMarshal(t, testRequest("REQ-ok", "m")),
			Partition: 0,
			Offset:    2,
		},
	}

	err := w.Run(context.Background(), &fakeConsumer{records: records})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	// Two poison messages -> two DLQ records, both with attempt 0.
	dlq := prod.RecordsFor(top.DLQ)
	if len(dlq) != 2 {
		t.Fatalf("dlq records = %d, want 2", len(dlq))
	}

	for i, r := range dlq {
		var dr protocol.DLQRecord
		if err := json.Unmarshal(r.Value, &dr); err != nil {
			t.Fatalf("dlq record %d: %v", i, err)
		}
		if dr.Attempt != 0 {
			t.Errorf("dlq record %d attempt = %d, want 0", i, dr.Attempt)
		}
		if len(dr.Original) == 0 {
			t.Errorf("dlq record %d must keep the original payload", i)
		}
	}

	// The valid record must still be processed.
	if n := prov.callCount(); n != 1 {
		t.Fatalf("provider calls = %d, want 1", n)
	}
}

func TestRunHandlerErrorPropagates(t *testing.T) {
	top := testTopics()
	prod := kafka.NewFakeProducer()
	prov := &fakeProvider{
		err: fmt.Errorf("%w: 401", providers.ErrNonRetryable),
	}
	w := newTestWorker(
		t,
		&failingProducer{inner: prod, failTopic: top.Failed},
		prov,
		2,
		3,
		0,
	)

	records := []kafka.Record{
		{
			Topic:     top.Requests,
			Key:       []byte("REQ1"),
			Value:     mustMarshal(t, testRequest("REQ1", "m")),
			Partition: 0,
			Offset:    0,
		},
	}

	err := w.Run(context.Background(), &fakeConsumer{records: records})
	if err == nil {
		t.Fatal("Run must propagate handler error")
	}
	if !strings.Contains(err.Error(), top.Failed) {
		t.Errorf("err = %v, want mention of %s", err, top.Failed)
	}
}

func mustMarshal(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	return b
}
