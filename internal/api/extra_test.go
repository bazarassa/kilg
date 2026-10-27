package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/kafka-llm-gateway/gateway/internal/config"
	"github.com/kafka-llm-gateway/gateway/internal/jobs"
	"github.com/kafka-llm-gateway/gateway/internal/kafka"
	"github.com/kafka-llm-gateway/gateway/internal/protocol"
	"github.com/kafka-llm-gateway/gateway/internal/providers"
	"github.com/kafka-llm-gateway/gateway/internal/providers/ollama"
)

const (
	// providerLiteLLM is the direct-route provider name in tests.
	providerLiteLLM = "litellm"
	// providerHeavy is the default async provider name in tests.
	providerHeavy = "heavy"
	// embeddingsObject is the OpenAI-compatible list object.
	embeddingsObject = "list"
)

// chatBodyLiteLLM is the standard direct-route chat request body
// (streaming: SSE response).
const chatBodyLiteLLM = `{"model":"litellm","stream":true,
	"messages":[{"role":"user","content":"hi"}]}`

// finishStop is the standard stop finish reason in tests.
const finishStop = "stop"

// fakeStreamProvider emits a fixed list of events.
type fakeStreamProvider struct {
	name   string
	model  string
	events []providers.ProviderEvent
	err    error
}

func (p *fakeStreamProvider) Name() string { return p.name }

func (p *fakeStreamProvider) Model() string { return p.model }

func (p *fakeStreamProvider) Complete(
	_ context.Context,
	_ *providers.ChatRequest,
	events chan<- providers.ProviderEvent,
) error {
	for _, ev := range p.events {
		events <- ev
	}
	close(events)
	return p.err
}

func newTestServerWithProviders(
	t *testing.T,
	provs map[string]providers.Provider,
) *Server {
	t.Helper()
	srv, _, _ := newTestServer(t)
	// Rebuild the server with the given providers map.
	return &Server{
		cfg:       srv.cfg,
		producer:  srv.producer,
		topics:    srv.topics,
		router:    srv.router,
		jobs:      srv.jobs,
		dispatch:  srv.dispatch,
		replay:    srv.replay,
		metrics:   srv.metrics,
		log:       srv.log,
		providers: provs,
		idem:      map[string]string{},
	}
}

func embeddingsServer(t *testing.T, status int, body string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(status)
			_, _ = io.WriteString(w, body)
		}))
}

func serverWithOllama(t *testing.T, url string) *Server {
	t.Helper()
	cfg := config.Default()
	cfg.OllamaURL = url
	cfg.OllamaEmbedModel = "nomic-embed-text:latest"

	srv := newTestServerWithProviders(t, map[string]providers.Provider{
		"ollama": ollama.New(cfg),
	})
	return srv
}

func TestEmbeddingsNoProvider(t *testing.T) {
	srv, _, _ := newTestServer(t)

	req := httptest.NewRequest(http.MethodPost, "/v1/embeddings",
		strings.NewReader(`{"input":"hi"}`))
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502, body = %s",
			rec.Code, rec.Body.String())
	}
}

func TestEmbeddingsStringInput(t *testing.T) {
	upstream := embeddingsServer(t, http.StatusOK,
		`{"model":"nomic-embed-text:latest","embeddings":[[0.1,0.2]]}`)
	defer upstream.Close()

	srv := serverWithOllama(t, upstream.URL)

	req := httptest.NewRequest(http.MethodPost, "/v1/embeddings",
		strings.NewReader(`{"input":"hello world"}`))
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s",
			rec.Code, rec.Body.String())
	}

	var resp struct {
		Object string `json:"object"`
		Data   []struct {
			Index     int       `json:"index"`
			Embedding []float64 `json:"embedding"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.Object != embeddingsObject || len(resp.Data) != 1 {
		t.Errorf("resp = %+v", resp)
	}
}

func TestEmbeddingsArrayInput(t *testing.T) {
	upstream := embeddingsServer(t, http.StatusOK,
		`{"model":"m","embeddings":[[0.1],[0.2],[0.3]]}`)
	defer upstream.Close()

	srv := serverWithOllama(t, upstream.URL)

	req := httptest.NewRequest(http.MethodPost, "/v1/embeddings",
		strings.NewReader(`{"input":["a","b","c"]}`))
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s",
			rec.Code, rec.Body.String())
	}

	var resp struct {
		Data []struct {
			Index int `json:"index"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(resp.Data) != 3 {
		t.Errorf("data = %d, want 3", len(resp.Data))
	}
}

func TestEmbeddingsInvalidInputType(t *testing.T) {
	upstream := embeddingsServer(t, http.StatusOK,
		`{"model":"m","embeddings":[]}`)
	defer upstream.Close()

	srv := serverWithOllama(t, upstream.URL)

	for _, body := range []string{
		`{"input":42}`,
		`{"input":[]}`,
	} {
		req := httptest.NewRequest(http.MethodPost, "/v1/embeddings",
			strings.NewReader(body))
		rec := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Errorf("body %s: status = %d, want 400",
				body, rec.Code)
		}
	}
}

func TestEmbeddingsUpstreamError(t *testing.T) {
	upstream := embeddingsServer(t, http.StatusInternalServerError,
		`boom`)
	defer upstream.Close()

	srv := serverWithOllama(t, upstream.URL)

	req := httptest.NewRequest(http.MethodPost, "/v1/embeddings",
		strings.NewReader(`{"input":"hi"}`))
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", rec.Code)
	}
}

func TestChatDefaultModelAsync(t *testing.T) {
	srv, prod, _ := newTestServer(t)
	// newTestServer sets DefaultModel = "heavy".

	body := `{"messages":[{"role":"user","content":"hi"}]}`
	req := httptest.NewRequest(http.MethodPost,
		"/v1/chat/completions", strings.NewReader(body))
	req.Header.Set("Prefer", "respond-async")
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, body = %s",
			rec.Code, rec.Body.String())
	}

	var accepted struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &accepted); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if accepted.ID == "" {
		t.Error("empty job id")
	}

	recs := prod.RecordsFor("llm.requests")
	if len(recs) != 1 {
		t.Fatalf("llm.requests records = %d, want 1", len(recs))
	}

	var kafkaReq struct {
		RequestID string `json:"request_id"`
		Provider  string `json:"provider"`
	}
	if err := json.Unmarshal(recs[0].Value, &kafkaReq); err != nil {
		t.Fatalf("unmarshal request: %v", err)
	}
	if kafkaReq.RequestID != accepted.ID {
		t.Errorf("kafka request id = %q, want %q",
			kafkaReq.RequestID, accepted.ID)
	}
	if kafkaReq.Provider != providerHeavy {
		t.Errorf("provider = %q, want %q",
			kafkaReq.Provider, providerHeavy)
	}
}

func TestChatDirectRouteStreamsSSE(t *testing.T) {
	srv := newTestServerWithProviders(t, map[string]providers.Provider{
		providerLiteLLM: &fakeStreamProvider{
			name:  providerLiteLLM,
			model: "gemma-4",
			events: []providers.ProviderEvent{
				{Type: providers.EventRawSSE,
					Raw: `{"id":"c1","object":"chat.completion.chunk",
						"choices":[{"index":0,
						"delta":{"role":"assistant","content":"Hi"}}]}`},
				{Type: providers.EventDone},
			},
		},
	})

	req := httptest.NewRequest(http.MethodPost,
		"/v1/chat/completions", strings.NewReader(chatBodyLiteLLM))
	rec := &testFlusher{httptest.NewRecorder()}
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s",
			rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "text/event-stream" {
		t.Errorf("content-type = %q", ct)
	}

	out := rec.Body.String()
	if !strings.Contains(out, "Hi") {
		t.Errorf("missing content chunk: %q", out)
	}
	if !strings.Contains(out, "[DONE]") {
		t.Errorf("missing [DONE]: %q", out)
	}

	// Job must end completed.
	// The request id is the SSE event id prefix.
	if !strings.Contains(out, "id: ") {
		t.Errorf("missing event id: %q", out)
	}
}

func TestChatDirectProviderNotConfigured(t *testing.T) {
	srv := newTestServerWithProviders(t, nil)

	req := httptest.NewRequest(http.MethodPost,
		"/v1/chat/completions", strings.NewReader(chatBodyLiteLLM))
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, body = %s",
			rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "provider not configured") {
		t.Errorf("body = %s", rec.Body.String())
	}
}

func TestChatDirectProviderError(t *testing.T) {
	srv := newTestServerWithProviders(t, map[string]providers.Provider{
		providerLiteLLM: &fakeStreamProvider{
			name:  providerLiteLLM,
			model: "gemma-4",
			events: []providers.ProviderEvent{
				{Type: providers.EventError,
					Err: errProviderBoom},
			},
		},
	})

	req := httptest.NewRequest(http.MethodPost,
		"/v1/chat/completions", strings.NewReader(chatBodyLiteLLM))
	rec := &testFlusher{httptest.NewRecorder()}
	srv.Handler().ServeHTTP(rec, req)

	out := rec.Body.String()
	if !strings.Contains(out, "provider boom") {
		t.Errorf("missing provider error: %q", out)
	}
	if !strings.Contains(out, "[DONE]") {
		t.Errorf("missing [DONE]: %q", out)
	}
}

// errProviderBoom is a stable error used in direct-route tests.
var errProviderBoom = errors.New("provider boom")

func TestWriteEventCompletedStreamedNoContentDup(t *testing.T) {
	srv, _, _ := newTestServer(t)

	rec := httptest.NewRecorder()
	flusher := &testFlusher{rec}

	ev, err := protocol.NewEvent(
		"REQ1",
		10,
		protocol.TypeCompleted,
		protocol.CompletedPayload{
			FinishReason: finishStop,
			Content:      "full answer",
			Stream:       true,
		},
	)
	if err != nil {
		t.Fatalf("NewEvent: %v", err)
	}

	if err := srv.writeEvent(rec, flusher, ev); err != nil {
		t.Fatalf("writeEvent: %v", err)
	}

	out := rec.Body.String()

	// Streaming-режим: content уже доставлен raw-чанками,
	// повторная отправка агрегата недопустима.
	if strings.Contains(out, "full answer") {
		t.Errorf("streamed completed re-emitted content: %s", out)
	}
	if !strings.Contains(out, `"finish_reason":"stop"`) {
		t.Errorf("finish chunk missing: %s", out)
	}
}

func TestWriteEventCompletedLegacyEmitsContent(t *testing.T) {
	srv, _, _ := newTestServer(t)

	rec := httptest.NewRecorder()
	flusher := &testFlusher{rec}

	// Non-streaming completed (legacy/старые producer'ы):
	// content + finish.
	ev, err := protocol.NewEvent(
		"REQ1",
		2,
		protocol.TypeCompleted,
		protocol.CompletedPayload{
			FinishReason: finishStop,
			Content:      "full answer",
		},
	)
	if err != nil {
		t.Fatalf("NewEvent: %v", err)
	}

	if err := srv.writeEvent(rec, flusher, ev); err != nil {
		t.Fatalf("writeEvent: %v", err)
	}

	out := rec.Body.String()
	if !strings.Contains(out, "full answer") {
		t.Errorf("legacy completed must emit content: %s", out)
	}
	if !strings.Contains(out, `"finish_reason":"stop"`) {
		t.Errorf("finish chunk missing: %s", out)
	}
}

func TestJobInfoIncludesFinalResult(t *testing.T) {
	j := &jobs.Job{
		ID:                 "REQ1",
		Status:             protocol.StatusCompleted,
		FinishReason:       finishStop,
		Content:            "answer",
		Reasoning:          "thinking",
		ReasoningAvailable: true,
		Usage:              json.RawMessage(`{"total_tokens":15}`),
	}

	info := jobInfo(j)

	if info.Content != "answer" {
		t.Errorf("content = %q, want answer", info.Content)
	}
	if info.Reasoning != "thinking" {
		t.Errorf("reasoning = %q, want thinking", info.Reasoning)
	}
	if !info.ReasoningOK {
		t.Error("reasoning_available must be true")
	}

	var u protocol.Usage
	if err := json.Unmarshal(info.Usage, &u); err != nil {
		t.Fatalf("usage: %v", err)
	}
	if u.TotalTokens != 15 {
		t.Errorf("total_tokens = %d, want 15", u.TotalTokens)
	}
}

// --- Non-streaming mode (stream:false) -> chat.completion JSON ---

const chatBodyNonStream = `{"model":"litellm","stream":false,
	"messages":[{"role":"user","content":"hi"}]}`

// providerLiteLLMModel is the fake upstream model name in direct tests.
const providerLiteLLMModel = "litellm-model"

const providerCompletionJSON = `{"id":"chatcmpl-abc",
	"object":"chat.completion","created":1793115291,
	"model":"upstream-model",
	"choices":[{"index":0,
	"message":{"role":"assistant","content":"Hello world"},
	"finish_reason":"stop"}],
	"usage":{"prompt_tokens":10,
	"completion_tokens":5,"total_tokens":15}}`

const providerChunkHello = `{"id":"chatcmpl-ch","object":"chat.completion.chunk",
	"created":1793115291,"model":"upstream-model",
	"choices":[{"index":0,"delta":{"content":"Hello "}}]}`

const providerChunkWorld = `{"id":"chatcmpl-ch","object":"chat.completion.chunk",
	"created":1793115291,"model":"upstream-model",
	"choices":[{"index":0,"delta":{"content":"world"}}]}`

const providerChunkFinal = `{"id":"chatcmpl-ch","object":"chat.completion.chunk",
	"created":1793115291,"model":"upstream-model",
	"choices":[{"index":0,"delta":{},
	"finish_reason":"stop"}],
	"usage":{"prompt_tokens":10,
	"completion_tokens":5,"total_tokens":15}}`

// TestChatJSONNonStreamingDirectRoute: provider отвечает одним
// объектом chat.completion (non-streaming) -> 200 JSON по спеке.
func TestChatJSONNonStreamingDirectRoute(t *testing.T) {
	srv := newTestServerWithProviders(t, map[string]providers.Provider{
		providerLiteLLM: &fakeStreamProvider{
			name:  providerLiteLLM,
			model: providerLiteLLMModel,
			events: []providers.ProviderEvent{
				{Type: providers.EventRawSSE, Raw: providerCompletionJSON},
				{Type: providers.EventDone},
			},
		},
	})

	req := httptest.NewRequest(
		http.MethodPost,
		"/v1/chat/completions",
		strings.NewReader(chatBodyNonStream),
	)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200, body = %s",
			rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("content-type = %q, want application/json", ct)
	}

	var resp protocol.ChatResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v, body = %s", err, rec.Body.String())
	}
	assertChatCompletionJSON(t, &resp, "Hello world",
		finishStop, 15)
}

// TestChatJSONNonStreamingChunksAccumulate: provider стримит чанки
// несмотря на stream:false -> Gateway агрегирует их в один JSON-ответ.
func TestChatJSONNonStreamingChunksAccumulate(t *testing.T) {
	srv := newTestServerWithProviders(t, map[string]providers.Provider{
		providerLiteLLM: &fakeStreamProvider{
			name:  providerLiteLLM,
			model: providerLiteLLMModel,
			events: []providers.ProviderEvent{
				{Type: providers.EventRawSSE, Raw: providerChunkHello},
				{Type: providers.EventRawSSE, Raw: providerChunkWorld},
				{Type: providers.EventRawSSE, Raw: providerChunkFinal},
				{Type: providers.EventDone},
			},
		},
	})

	req := httptest.NewRequest(
		http.MethodPost,
		"/v1/chat/completions",
		strings.NewReader(chatBodyNonStream),
	)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200, body = %s",
			rec.Code, rec.Body.String())
	}

	var resp protocol.ChatResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	assertChatCompletionJSON(t, &resp, "Hello world",
		finishStop, 15)
}

// TestChatJSONNonStreamingDirectError: provider error -> JSON error
// (OpenAI Error schema) с кодом 502, а не SSE.
func TestChatJSONNonStreamingDirectError(t *testing.T) {
	srv := newTestServerWithProviders(t, map[string]providers.Provider{
		providerLiteLLM: &fakeStreamProvider{
			name:  providerLiteLLM,
			model: providerLiteLLMModel,
			err:   errors.New("boom"),
		},
	})

	req := httptest.NewRequest(
		http.MethodPost,
		"/v1/chat/completions",
		strings.NewReader(chatBodyNonStream),
	)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502, body = %s",
			rec.Code, rec.Body.String())
	}

	var errObj struct {
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &errObj); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !strings.Contains(errObj.Error.Message, "boom") {
		t.Errorf("message = %q, want contains 'boom'",
			errObj.Error.Message)
	}
}

// TestChatJSONNonStreamingKafkaRoute: heavy-маршрут, stream:false ->
// Gateway ждёт terminal-событие и отвечает одним JSON-объектом.
func TestChatJSONNonStreamingKafkaRoute(t *testing.T) {
	srv, prod, _ := newTestServer(t)

	body := `{"model":"heavy","stream":false,
		"messages":[{"role":"user","content":"hi"}]}`

	req := httptest.NewRequest(
		http.MethodPost,
		"/v1/chat/completions",
		strings.NewReader(body),
	)
	rec := httptest.NewRecorder()

	done := make(chan struct{})

	go func() {
		srv.Handler().ServeHTTP(rec, req)
		close(done)
	}()

	requestID := waitForRequestsRecord(t, prod)

	srv.dispatch.Dispatch(protocol.Event{
		RequestID: requestID,
		Type:      protocol.TypeCompleted,
		Sequence:  2,
		Timestamp: time.Now(),
		Model:     "heavy-model",
		Payload: mustMarshal(t, protocol.CompletedPayload{
			FinishReason: finishStop,
			Content:      "Hello world",
			Stream:       true,
			Usage: mustMarshal(t, protocol.Usage{
				PromptTokens:     10,
				CompletionTokens: 5,
				TotalTokens:      15,
			}),
		}),
	})

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for JSON response")
	}

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200, body = %s",
			rec.Code, rec.Body.String())
	}

	var resp protocol.ChatResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	assertChatCompletionJSON(t, &resp, "Hello world",
		finishStop, 15)

	if resp.ID != requestID {
		t.Errorf("id = %q, want request id %q", resp.ID, requestID)
	}
}

// TestChatJSONNonStreamingKafkaFailed: failed terminal event -> 502
// JSON error.
func TestChatJSONNonStreamingKafkaFailed(t *testing.T) {
	srv, prod, _ := newTestServer(t)

	body := `{"model":"heavy","stream":false,
		"messages":[{"role":"user","content":"hi"}]}`

	req := httptest.NewRequest(
		http.MethodPost,
		"/v1/chat/completions",
		strings.NewReader(body),
	)
	rec := httptest.NewRecorder()

	done := make(chan struct{})

	go func() {
		srv.Handler().ServeHTTP(rec, req)
		close(done)
	}()

	requestID := waitForRequestsRecord(t, prod)

	srv.dispatch.Dispatch(protocol.Event{
		RequestID: requestID,
		Type:      protocol.TypeFailed,
		Sequence:  3,
		Timestamp: time.Now(),
		Payload: mustMarshal(t, protocol.FailedPayload{
			Error: "provider exploded",
		}),
	})

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for JSON response")
	}

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502, body = %s",
			rec.Code, rec.Body.String())
	}

	var errObj struct {
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &errObj); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !strings.Contains(errObj.Error.Message, "provider exploded") {
		t.Errorf("message = %q, want contains 'provider exploded'",
			errObj.Error.Message)
	}
}

// assertChatCompletionJSON проверяет обязательные поля chat.completion.
func assertChatCompletionJSON(
	t *testing.T,
	resp *protocol.ChatResponse,
	wantContent string,
	wantFinish string,
	wantTotal int,
) {
	t.Helper()

	if resp.Object != "chat.completion" {
		t.Errorf("object = %q, want chat.completion", resp.Object)
	}
	if resp.ID == "" {
		t.Error("id must not be empty")
	}
	if len(resp.Choices) != 1 {
		t.Fatalf("choices = %d, want 1", len(resp.Choices))
	}

	assertChoiceJSON(t, resp.Choices[0], wantContent, wantFinish)

	if resp.Usage == nil || resp.Usage.TotalTokens != wantTotal {
		t.Errorf("usage = %+v, want total_tokens %d",
			resp.Usage, wantTotal)
	}
}

// assertChoiceJSON проверяет поля выбора (message/finish_reason).
func assertChoiceJSON(
	t *testing.T,
	c protocol.Choice,
	wantContent string,
	wantFinish string,
) {
	t.Helper()

	if c.Message.Role != "assistant" {
		t.Errorf("role = %q, want assistant", c.Message.Role)
	}
	if string(c.Message.Content) != fmt.Sprintf("%q", wantContent) {
		t.Errorf("content = %s, want %q", c.Message.Content,
			wantContent)
	}
	if c.FinishReason == nil || *c.FinishReason != wantFinish {
		t.Errorf("finish_reason = %v, want %s",
			c.FinishReason, wantFinish)
	}
}

// mustMarshal маршалирует значение в json.RawMessage для тестов.
func mustMarshal(t *testing.T, v any) json.RawMessage {
	t.Helper()

	b, err := json.Marshal(v)

	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	return b
}

// waitForRequestsRecord ждёт, пока в llm.requests появится запись,
// и возвращает её ключ (request ID).
func waitForRequestsRecord(
	t *testing.T,
	prod *kafka.FakeProducer,
) string {
	t.Helper()

	for i := 0; i < 100; i++ {
		recs := prod.RecordsFor("llm.requests")

		if len(recs) > 0 {
			return string(recs[0].Key)
		}

		time.Sleep(10 * time.Millisecond)
	}

	t.Fatal("no record in llm.requests")

	return ""
}
