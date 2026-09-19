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

// Test helpers

func newTestServer(t *testing.T) (*Server, *kafka.FakeProducer, *kafka.FakeReader) {
	t.Helper()
	cfg := config.Default()
	//FIX test
	cfg.DefaultModel = "heavy"
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

// testFlusher wraps ResponseRecorder to implement Flusher
type testFlusher struct {
	*httptest.ResponseRecorder
}

func (f *testFlusher) Flush() {}

// Original tests

func TestModelsEndpoint(t *testing.T) {
	srv, _, _ := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	rec := httptest.NewRecorder()

	done := make(chan bool)
	go func() {
		srv.Handler().ServeHTTP(rec, req)
		done <- true
	}()

	select {
	case <-done:
		// OK
	case <-time.After(5 * time.Second):
		t.Fatal("test timeout")
	}

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var list protocol.ModelsList
	if err := json.Unmarshal(rec.Body.Bytes(), &list); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if list.Object != "list" || len(list.Data) == 0 {
		t.Errorf("unexpected models: %+v", list)
	}
}

func TestGetModel(t *testing.T) {
	srv, _, _ := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/models/heavy", nil)
	rec := httptest.NewRecorder()

	done := make(chan bool)
	go func() {
		srv.Handler().ServeHTTP(rec, req)
		done <- true
	}()

	select {
	case <-done:
		// OK
	case <-time.After(5 * time.Second):
		t.Fatal("test timeout")
	}

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var model protocol.ModelInfo
	if err := json.Unmarshal(rec.Body.Bytes(), &model); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if model.ID != "heavy" {
		t.Errorf("model.ID = %q, want heavy", model.ID)
	}
}

func TestGetModelNotFound(t *testing.T) {
	srv, _, _ := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/models/nonexistent", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
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

func TestReadyEndpoint(t *testing.T) {
	srv, _, _ := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
}

func TestChatValidation(t *testing.T) {
	srv, _, _ := newTestServer(t)

	tests := []struct {
		name string
		body string
		want int
	}{
		{
			name: "missing messages",
			body: `{"model":"heavy"}`,
			want: http.StatusBadRequest,
		},
		{
			name: "missing model uses default",
			body: `{"messages":[{"role":"user","content":"hi"}]}`,
			want: http.StatusAccepted,
		},
		{
			name: "empty messages",
			body: `{"model":"heavy","messages":[]}`,
			want: http.StatusBadRequest,
		},
		{
			name: "invalid json",
			body: `{invalid}`,
			want: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(
				http.MethodPost,
				"/v1/chat/completions",
				strings.NewReader(tt.body),
			)
			req.Header.Set("Content-Type", "application/json")

			if tt.name == "missing model uses default" {
				req.Header.Set("Prefer", "respond-async")
			}

			rec := httptest.NewRecorder()

			srv.Handler().ServeHTTP(rec, req)

			if rec.Code != tt.want {
				t.Fatalf(
					"status = %d, want %d, body = %s",
					rec.Code,
					tt.want,
					rec.Body.String(),
				)
			}
		})
	}
}

func TestChatAsyncEnqueues(t *testing.T) {
	srv, prod, _ := newTestServer(t)
	body := `{"model":"heavy","messages":[{"role":"user","content":"hi"}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Prefer", "respond-async")
	rec := httptest.NewRecorder()

	done := make(chan bool)
	go func() {
		srv.Handler().ServeHTTP(rec, req)
		done <- true
	}()

	select {
	case <-done:
		// OK
	case <-time.After(5 * time.Second):
		t.Fatal("test timeout")
	}

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202, body = %s", rec.Code, rec.Body.String())
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
	if len(prod.RecordsFor("llm.requests")) != 1 {
		t.Errorf("request records = %d, want 1", len(prod.RecordsFor("llm.requests")))
	}
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
	if len(prod.RecordsFor("llm.requests")) != 1 {
		t.Errorf("request records = %d, want 1 (idempotent)", len(prod.RecordsFor("llm.requests")))
	}
}

func TestJobEventsReplay(t *testing.T) {
	srv, _, reader := newTestServer(t)
	recs := []kafka.Record{}
	for i := uint64(1); i <= 3; i++ {
		ev, _ := protocol.NewEvent("REQX", i, protocol.TypeRawSSE, protocol.RawSSEPayload{Data: `{"choices":[{"delta":{"content":"c` + itoa(i) + `"}}]}`})
		b, _ := json.Marshal(ev)
		recs = append(recs, kafka.Record{Key: []byte("REQX"), Value: b})
	}
	reader.SetRecords("REQX", recs)

	req := httptest.NewRequest(http.MethodGet, "/v1/jobs/REQX/events", nil)
	rec := httptest.NewRecorder()

	done := make(chan bool)
	go func() {
		srv.Handler().ServeHTTP(rec, req)
		done <- true
	}()

	select {
	case <-done:
		// OK
	case <-time.After(5 * time.Second):
		t.Fatal("test timeout - SSE stream didn't complete")
	}

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}

	contentType := rec.Header().Get("Content-Type")
	if contentType != "text/event-stream" {
		t.Errorf("content-type = %q, want text/event-stream", contentType)
	}

	body := rec.Body.String()
	if !strings.Contains(body, "c1") || !strings.Contains(body, "c3") {
		t.Errorf("replay missing events, body = %q", body)
	}
	if !strings.Contains(body, "[DONE]") {
		t.Errorf("replay missing [DONE], body = %q", body)
	}
}

func TestJobsList(t *testing.T) {
	srv, _, _ := newTestServer(t)

	srv.jobs.Ensure("job1", "heavy", "model1")
	srv.jobs.Ensure("job2", "litellm", "model2")

	req := httptest.NewRequest(http.MethodGet, "/v1/jobs", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}

	var resp map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if resp["object"] != "list" {
		t.Errorf("object = %q, want list", resp["object"])
	}
}

func TestJobGet(t *testing.T) {
	srv, _, _ := newTestServer(t)

	srv.jobs.Ensure("testjob", "heavy", "testmodel")
	srv.jobs.Update("testjob", func(j *jobs.Job) {
		j.Status = protocol.StatusRunning
	})

	req := httptest.NewRequest(http.MethodGet, "/v1/jobs/testjob", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}

	var jobInfo protocol.JobInfo
	if err := json.Unmarshal(rec.Body.Bytes(), &jobInfo); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if jobInfo.ID != "testjob" {
		t.Errorf("job.ID = %q, want testjob", jobInfo.ID)
	}
	if jobInfo.Status != protocol.StatusRunning {
		t.Errorf("job.Status = %q, want running", jobInfo.Status)
	}
}

func TestJobNotFound(t *testing.T) {
	srv, _, _ := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/jobs/UNKNOWN", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404, body = %s", rec.Code, rec.Body.String())
	}
}

func TestCancelJob(t *testing.T) {
	srv, _, _ := newTestServer(t)

	srv.jobs.Ensure("canceljob", "heavy", "testmodel")
	srv.jobs.Update("canceljob", func(j *jobs.Job) {
		j.Status = protocol.StatusQueued
	})

	req := httptest.NewRequest(http.MethodDelete, "/v1/jobs/canceljob", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}

	var resp map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if resp["status"] != protocol.StatusCancelled {
		t.Errorf("status = %q, want cancelled", resp["status"])
	}

	j, ok := srv.jobs.Store().Get("canceljob")
	if !ok {
		t.Fatal("job not found after cancel")
	}
	if j.Status != protocol.StatusCancelled {
		t.Errorf("job status = %q, want cancelled", j.Status)
	}
}

func TestCancelCompletedJob(t *testing.T) {
	srv, _, _ := newTestServer(t)

	srv.jobs.Ensure("completedjob", "heavy", "testmodel")
	srv.jobs.Update("completedjob", func(j *jobs.Job) {
		j.Status = protocol.StatusCompleted
	})

	req := httptest.NewRequest(http.MethodDelete, "/v1/jobs/completedjob", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409, body = %s", rec.Code, rec.Body.String())
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

func TestAuthPublicEndpoints(t *testing.T) {
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

	publicEndpoints := []string{"/health", "/ready", "/metrics", "/v1/health"}

	for _, endpoint := range publicEndpoints {
		t.Run(endpoint, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, endpoint, nil)
			rec := httptest.NewRecorder()
			srv.Handler().ServeHTTP(rec, req)
			if rec.Code == http.StatusUnauthorized {
				t.Errorf("endpoint %s should be public but got 401", endpoint)
			}
		})
	}
}

func TestCORS(t *testing.T) {
	srv, _, _ := newTestServer(t)

	req := httptest.NewRequest(http.MethodOptions, "/v1/models", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("OPTIONS status = %d, want 204", rec.Code)
	}

	if rec.Header().Get("Access-Control-Allow-Origin") != "*" {
		t.Errorf("CORS origin header missing")
	}
}

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

// Additional tests for coverage

func TestChatCompletionAlias(t *testing.T) {
	srv, _, _ := newTestServer(t)

	srv.jobs.Ensure("testcompletion", "heavy", "testmodel")
	srv.jobs.Update("testcompletion", func(j *jobs.Job) {
		j.Status = protocol.StatusCompleted
	})

	req := httptest.NewRequest(http.MethodGet, "/v1/chat/completions/testcompletion", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}

	var jobInfo protocol.JobInfo
	if err := json.Unmarshal(rec.Body.Bytes(), &jobInfo); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if jobInfo.Status != protocol.StatusCompleted {
		t.Errorf("status = %q, want completed", jobInfo.Status)
	}
}

func TestCancelCompletionAlias(t *testing.T) {
	srv, _, _ := newTestServer(t)

	srv.jobs.Ensure("cancelcomp", "heavy", "testmodel")
	srv.jobs.Update("cancelcomp", func(j *jobs.Job) {
		j.Status = protocol.StatusQueued
	})

	req := httptest.NewRequest(http.MethodDelete, "/v1/chat/completions/cancelcomp", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
}

func TestCompletionMessagesAlias(t *testing.T) {
	srv, _, reader := newTestServer(t)

	recs := []kafka.Record{}
	ev, _ := protocol.NewEvent("MSG1", 1, protocol.TypeRawSSE,
		protocol.RawSSEPayload{Data: `{"choices":[{"delta":{"content":"hi"}}]}`})
	b, _ := json.Marshal(ev)
	recs = append(recs, kafka.Record{Key: []byte("MSG1"), Value: b})
	reader.SetRecords("MSG1", recs)

	req := httptest.NewRequest(http.MethodGet, "/v1/chat/completions/MSG1/messages", nil)
	rec := httptest.NewRecorder()

	done := make(chan bool)
	go func() {
		srv.Handler().ServeHTTP(rec, req)
		done <- true
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("test timeout")
	}

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
}

func TestReplayFromWithLastEventID(t *testing.T) {
	srv, _, reader := newTestServer(t)

	recs := []kafka.Record{}
	for i := uint64(1); i <= 5; i++ {
		ev, _ := protocol.NewEvent("REPLAY1", i, protocol.TypeRawSSE,
			protocol.RawSSEPayload{Data: `{"seq":` + itoa(i) + `}`})
		b, _ := json.Marshal(ev)
		recs = append(recs, kafka.Record{Key: []byte("REPLAY1"), Value: b})
	}
	reader.SetRecords("REPLAY1", recs)

	req := httptest.NewRequest(http.MethodGet, "/v1/jobs/REPLAY1/events", nil)
	req.Header.Set("Last-Event-ID", "REPLAY1-0000000002")
	rec := httptest.NewRecorder()

	done := make(chan bool)
	go func() {
		srv.Handler().ServeHTTP(rec, req)
		done <- true
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("test timeout")
	}

	body := rec.Body.String()
	if strings.Contains(body, `"seq":1`) || strings.Contains(body, `"seq":2`) {
		t.Errorf("replay included already-sent events")
	}
	if !strings.Contains(body, `"seq":3`) {
		t.Errorf("replay missing event 3")
	}
}

func TestJobRecoveryFromKafka(t *testing.T) {
	srv, _, reader := newTestServer(t)

	recs := []kafka.Record{}

	ev1, _ := protocol.NewEvent("RECOVER1", 0, protocol.TypeQueued, map[string]string{
		"provider": "heavy", "model": "test",
	})
	ev1.Provider = "heavy"
	ev1.Model = "test"
	b1, _ := json.Marshal(ev1)
	recs = append(recs, kafka.Record{Key: []byte("RECOVER1"), Value: b1})

	ev2, _ := protocol.NewEvent("RECOVER1", 1, protocol.TypeStarted,
		protocol.StartedPayload{Provider: "heavy", Model: "test"})
	ev2.Provider = "heavy"
	ev2.Model = "test"
	b2, _ := json.Marshal(ev2)
	recs = append(recs, kafka.Record{Key: []byte("RECOVER1"), Value: b2})

	ev3, _ := protocol.NewEvent("RECOVER1", 2, protocol.TypeCompleted,
		protocol.CompletedPayload{FinishReason: "stop", Events: 2})
	ev3.Provider = "heavy"
	ev3.Model = "test"
	b3, _ := json.Marshal(ev3)
	recs = append(recs, kafka.Record{Key: []byte("RECOVER1"), Value: b3})

	reader.SetRecords("RECOVER1", recs)

	_, ok := srv.jobs.Store().Get("RECOVER1")
	if ok {
		t.Fatal("job should not exist yet")
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/jobs/RECOVER1", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}

	var jobInfo protocol.JobInfo
	if err := json.Unmarshal(rec.Body.Bytes(), &jobInfo); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if jobInfo.Status != protocol.StatusCompleted {
		t.Errorf("status = %q, want completed", jobInfo.Status)
	}
	if jobInfo.Sequence != 2 {
		t.Errorf("sequence = %d, want 2", jobInfo.Sequence)
	}
}

func TestIdempotencyKeyReuse(t *testing.T) {
	srv, _, _ := newTestServer(t)

	body := `{"model":"heavy","messages":[{"role":"user","content":"test"}]}`

	req1 := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	req1.Header.Set("Content-Type", "application/json")
	req1.Header.Set("Prefer", "respond-async")
	req1.Header.Set("Idempotency-Key", "unique-key-123")
	rec1 := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec1, req1)

	var resp1 protocol.JobAccepted
	json.Unmarshal(rec1.Body.Bytes(), &resp1)

	req2 := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	req2.Header.Set("Content-Type", "application/json")
	req2.Header.Set("Prefer", "respond-async")
	req2.Header.Set("Idempotency-Key", "unique-key-123")
	rec2 := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec2, req2)

	var resp2 protocol.JobAccepted
	json.Unmarshal(rec2.Body.Bytes(), &resp2)

	if resp1.ID != resp2.ID {
		t.Errorf("different IDs for same idempotency key: %s vs %s", resp1.ID, resp2.ID)
	}
}

func TestWriteJSONError(t *testing.T) {
	rec := httptest.NewRecorder()
	writeJSONError(rec, http.StatusBadRequest, "test error message")

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}

	var errResp map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &errResp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	errObj, ok := errResp["error"].(map[string]any)
	if !ok {
		t.Fatal("missing error object")
	}

	if errObj["message"] != "test error message" {
		t.Errorf("message = %q", errObj["message"])
	}
}

func TestSplitCommaHelper(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  []string
	}{
		{
			name:  "multiple values",
			input: "a,b,c",
			want:  []string{"a", "b", "c"},
		},
		{
			name:  "single value",
			input: "respond-async",
			want:  []string{"respond-async"},
		},
		{
			name:  "values with spaces",
			input: "a, b, c",
			want:  []string{"a", "b", "c"},
		},
		{
			name:  "empty input",
			input: "",
			want:  []string{},
		},
		{
			name:  "only commas",
			input: ",,,",
			want:  []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := splitComma(tt.input)

			if len(got) != len(tt.want) {
				t.Fatalf(
					"splitComma(%q) = %q, want %q",
					tt.input,
					got,
					tt.want,
				)
			}

			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf(
						"splitComma(%q)[%d] = %q, want %q",
						tt.input,
						i,
						got[i],
						tt.want[i],
					)
				}
			}
		})
	}
}

func TestTrimSpaceHelper(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"  hello  ", "hello"},
		{"\thello\t", "hello"},
		{"hello", "hello"},
		{"  ", ""},
		{"", ""},
	}

	for _, tt := range tests {
		got := trimSpace(tt.input)
		if got != tt.want {
			t.Errorf("trimSpace(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestIsAsyncHeader(t *testing.T) {
	tests := []struct {
		header string
		want   bool
	}{
		{"respond-async", true},
		{"respond-async, other", true},
		{"other, respond-async", true},
		{"", false},
		{"other", false},
	}

	for _, tt := range tests {
		req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
		req.Header.Set("Prefer", tt.header)
		got := isAsync(req)
		if got != tt.want {
			t.Errorf("isAsync with Prefer=%q = %v, want %v", tt.header, got, tt.want)
		}
	}
}

func TestMetricsEndpoint(t *testing.T) {
	srv, _, _ := newTestServer(t)

	srv.SetMetricsHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("# HELP test"))
	}))

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}

	if !strings.Contains(rec.Body.String(), "# HELP test") {
		t.Errorf("metrics response = %q", rec.Body.String())
	}
}

func TestJobInfoConversion(t *testing.T) {
	now := time.Now().UTC()
	job := &jobs.Job{
		ID:           "test123",
		Provider:     "heavy",
		Model:        "model1",
		Status:       protocol.StatusCompleted,
		Sequence:     42,
		Events:       10,
		CreatedAt:    now,
		UpdatedAt:    now,
		StartedAt:    now,
		CompletedAt:  now,
		FinishReason: "stop",
		Error:        "",
	}

	info := jobInfo(job)

	if info.ID != "test123" {
		t.Errorf("ID = %q", info.ID)
	}
	if info.Status != protocol.StatusCompleted {
		t.Errorf("Status = %q", info.Status)
	}
	if info.Sequence != 42 {
		t.Errorf("Sequence = %d", info.Sequence)
	}
}

func TestWriteEventTypes(t *testing.T) {
	srv, _, _ := newTestServer(t)

	tests := []struct {
		name      string
		eventType string
		payload   any
	}{
		{
			"raw_sse",
			protocol.TypeRawSSE,
			protocol.RawSSEPayload{Data: `{"test":"data"}`},
		},
		{
			"completed",
			protocol.TypeCompleted,
			protocol.CompletedPayload{FinishReason: "stop"},
		},
		{
			"failed",
			protocol.TypeFailed,
			protocol.FailedPayload{Error: "test error"},
		},
		{
			"heartbeat",
			protocol.TypeHeartbeat,
			protocol.HeartbeatPayload{LastSequence: 5},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ev, err := protocol.NewEvent("TEST", 1, tt.eventType, tt.payload)
			if err != nil {
				t.Fatalf("NewEvent: %v", err)
			}

			rec := httptest.NewRecorder()
			flusher := &testFlusher{rec}

			if err := srv.writeEvent(rec, flusher, ev); err != nil {
				t.Fatalf("writeEvent: %v", err)
			}
		})
	}
}

func TestTerminalChunkFormatting(t *testing.T) {
	ev1, _ := protocol.NewEvent("REQ1", 1, protocol.TypeCompleted,
		protocol.CompletedPayload{FinishReason: "length"})
	chunk1 := terminalChunk(ev1)
	if !strings.Contains(chunk1, `"finish_reason":"length"`) {
		t.Errorf("completed chunk = %q", chunk1)
	}

	ev2, _ := protocol.NewEvent("REQ2", 1, protocol.TypeFailed,
		protocol.FailedPayload{Error: "timeout"})
	chunk2 := terminalChunk(ev2)
	if !strings.Contains(chunk2, "timeout") {
		t.Errorf("failed chunk = %q", chunk2)
	}
}

func TestMetaChunkFormatting(t *testing.T) {
	ev, _ := protocol.NewEvent("REQ1", 42, protocol.TypeHeartbeat, nil)
	chunk := metaChunk(ev)

	if !strings.Contains(chunk, `"type":"heartbeat"`) {
		t.Errorf("missing type in chunk: %q", chunk)
	}
	if !strings.Contains(chunk, `"sequence":42`) {
		t.Errorf("missing sequence in chunk: %q", chunk)
	}
}

func TestJSONEscape(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{`hello"world`, `hello\"world`},
		{`line1\nline2`, `line1\\nline2`},
		{`simple`, `simple`},
	}

	for _, tt := range tests {
		got := jsonEscape(tt.input)
		if got != tt.want {
			t.Errorf("jsonEscape(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestItoaHelper(t *testing.T) {
	tests := []struct {
		input uint64
		want  string
	}{
		{0, "0"},
		{42, "42"},
		{12345, "12345"},
		{9223372036854775807, "9223372036854775807"},
	}

	for _, tt := range tests {
		got := itoa(tt.input)
		if got != tt.want {
			t.Errorf("itoa(%d) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

var _ = context.Background
var _ = time.Second
var _ = io.Discard
