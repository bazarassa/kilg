package api

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/kafka-llm-gateway/gateway/internal/config"
	"github.com/kafka-llm-gateway/gateway/internal/jobs"
	"github.com/kafka-llm-gateway/gateway/internal/kafka"
	"github.com/kafka-llm-gateway/gateway/internal/observability"
	"github.com/kafka-llm-gateway/gateway/internal/protocol"
	"github.com/kafka-llm-gateway/gateway/internal/router"
	"github.com/kafka-llm-gateway/gateway/internal/streaming"
	"github.com/prometheus/client_golang/prometheus"
)

func newTestServer(t *testing.T) (*Server, *kafka.FakeProducer, *kafka.FakeReader) {
	t.Helper()
	cfg := config.Default()
	prod := kafka.NewFakeProducer()
	reader := kafka.NewFakeReader()
	topics := kafka.Topics{
		Requests:  "llm.requests",
		Events:    "llm.events",
		Completed: "llm.completed",
		Failed:    "llm.failed",
		DLQ:       "llm.dlq",
	}
	reg := prometheus.NewRegistry()
	metrics := observability.NewMetrics(reg)
	jm := jobs.NewManager(jobs.NewMemoryStore())
	dispatch := streaming.NewDispatcher(1024)
	rt := router.New(cfg)
	srv := NewServer(cfg, prod, topics, rt, jm, dispatch, reader, metrics, testLogger(), nil)
	return srv, prod, reader
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestModelsEndpoint(t *testing.T) {
	srv, _, _ := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	var list protocol.ModelsList
	if err := json.Unmarshal(rec.Body.Bytes(), &list); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if list.Object != "list" || len(list.Data) == 0 {
		t.Errorf("unexpected models: %+v", list)
	}
}

func TestHealthEndpoint(t *testing.T) {
	srv, _, _ := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
}

func TestChatValidation(t *testing.T) {
	srv, _, _ := newTestServer(t)
	// Missing messages.
	body := `{"model":"heavy"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

func TestChatAsyncEnqueues(t *testing.T) {
	srv, prod, _ := newTestServer(t)
	body := `{"model":"heavy","messages":[{"role":"user","content":"hi"}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Prefer", "respond-async")
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", rec.Code)
	}
	var accepted protocol.JobAccepted
	if err := json.Unmarshal(rec.Body.Bytes(), &accepted); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if accepted.Status != protocol.StatusQueued {
		t.Errorf("status = %q", accepted.Status)
	}
	if len(accepted.ID) != 26 {
		t.Errorf("id length = %d, want 26", len(accepted.ID))
	}
	// A request should have been published to llm.requests.
	if len(prod.RecordsFor("llm.requests")) != 1 {
		t.Errorf("request records = %d, want 1", len(prod.RecordsFor("llm.requests")))
	}
	// A queued event should have been published to llm.events.
	if len(prod.RecordsFor("llm.events")) != 1 {
		t.Errorf("event records = %d, want 1", len(prod.RecordsFor("llm.events")))
	}
}

func TestChatIdempotency(t *testing.T) {
	srv, prod, _ := newTestServer(t)
	body := `{"model":"heavy","messages":[{"role":"user","content":"hi"}]}`

	req1 := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	req1.Header.Set("Content-Type", "application/json")
	req1.Header.Set("Prefer", "respond-async")
	req1.Header.Set("Idempotency-Key", "key-1")
	rec1 := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec1, req1)
	var a1 protocol.JobAccepted
	_ = json.Unmarshal(rec1.Body.Bytes(), &a1)

	req2 := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	req2.Header.Set("Content-Type", "application/json")
	req2.Header.Set("Prefer", "respond-async")
	req2.Header.Set("Idempotency-Key", "key-1")
	rec2 := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec2, req2)
	var a2 protocol.JobAccepted
	_ = json.Unmarshal(rec2.Body.Bytes(), &a2)

	if a1.ID != a2.ID {
		t.Errorf("idempotency failed: %q vs %q", a1.ID, a2.ID)
	}
	// Only one request should be enqueued.
	if len(prod.RecordsFor("llm.requests")) != 1 {
		t.Errorf("request records = %d, want 1 (idempotent)", len(prod.RecordsFor("llm.requests")))
	}
}

func TestJobEventsReplay(t *testing.T) {
	srv, _, reader := newTestServer(t)
	// Seed the reader with events for a request.
	recs := []kafka.Record{}
	for i := uint64(1); i <= 3; i++ {
		ev, _ := protocol.NewEvent("REQX", i, protocol.TypeRawSSE, protocol.RawSSEPayload{Data: `{"choices":[{"delta":{"content":"c` + itoa(i) + `"}}]}`})
		b, _ := json.Marshal(ev)
		recs = append(recs, kafka.Record{Key: []byte("REQX"), Value: b})
	}
	reader.SetRecords("REQX", recs)

	req := httptest.NewRequest(http.MethodGet, "/v1/jobs/REQX/events", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "text/event-stream") && rec.Header().Get("Content-Type") != "text/event-stream" {
		t.Errorf("content-type = %q", rec.Header().Get("Content-Type"))
	}
	if !strings.Contains(body, "c1") || !strings.Contains(body, "c3") {
		t.Errorf("replay missing events: %q", body)
	}
	if !strings.Contains(body, "[DONE]") {
		t.Errorf("replay missing [DONE]: %q", body)
	}
}

func TestJobNotFound(t *testing.T) {
	srv, _, _ := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/jobs/UNKNOWN", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
}

func TestAuthEnforced(t *testing.T) {
	cfg := config.Default()
	cfg.APIKeys = []string{"secret"}
	prod := kafka.NewFakeProducer()
	reader := kafka.NewFakeReader()
	topics := kafka.Topics{Requests: "r", Events: "e", Completed: "c", Failed: "f", DLQ: "d"}
	reg := prometheus.NewRegistry()
	metrics := observability.NewMetrics(reg)
	jm := jobs.NewManager(jobs.NewMemoryStore())
	dispatch := streaming.NewDispatcher(1024)
	rt := router.New(cfg)
	srv := NewServer(cfg, prod, topics, rt, jm, dispatch, reader, metrics, testLogger(), nil)

	req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}

	req2 := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	req2.Header.Set("Authorization", "Bearer secret")
	rec2 := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec2.Code)
	}
}

var _ = context.Background
var _ = time.Second

func TestParseSequence(t *testing.T) {
	cases := []struct {
		in   string
		want uint64
	}{
		{"06G8TP5KZ6SBK59DC50F5HAWDR-0000000007", 7},
		{"06G8TP5KZ6SBK59DC50F5HAWDR-0000000011", 11},
		{"7", 7},
		{"0000000007", 7},
		{"", 0},
		{"garbage", 0},
	}
	for _, c := range cases {
		if got := parseSequence(c.in); got != c.want {
			t.Errorf("parseSequence(%q) = %d, want %d", c.in, got, c.want)
		}
	}
}
