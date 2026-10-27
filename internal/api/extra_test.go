package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/kafka-llm-gateway/gateway/internal/config"
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

// chatBodyLiteLLM is the standard direct-route chat request body.
const chatBodyLiteLLM = `{"model":"litellm",
	"messages":[{"role":"user","content":"hi"}]}`

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
