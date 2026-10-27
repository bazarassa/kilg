package providers

import (
	"bufio"
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
)

// newTestClient builds an OpenAIClient with short timeouts for tests.
func newTestClient(
	baseURL string,
	model string,
	apiKey string,
	idle time.Duration,
	generation time.Duration,
) *OpenAIClient {
	return NewOpenAIClient(OpenAIConfig{
		BaseURL:           baseURL,
		Model:             model,
		APIKey:            apiKey,
		ConnectTimeout:    2 * time.Second,
		HeaderTimeout:     2 * time.Second,
		IdleTimeout:       idle,
		GenerationTimeout: generation,
	})
}

func TestBuildBodyNil(t *testing.T) {
	if _, err := buildBody(nil); err == nil {
		t.Fatal("expected error for nil request")
	}
}

func TestBuildBodyPreservesExtra(t *testing.T) {
	req := &ChatRequest{
		Model:  "override-model",
		Stream: true,
		Extra: []byte(`{
			"model":"original-model",
			"stream":false,
			"temperature":0.7,
			"custom_param":{"a":1},
			"messages":[{"role":"user","content":"hi"}]
		}`),
	}

	body, err := buildBody(req)
	if err != nil {
		t.Fatalf("buildBody: %v", err)
	}

	var obj map[string]json.RawMessage
	if err := json.Unmarshal(body, &obj); err != nil {
		t.Fatalf("unmarshal body: %v", err)
	}

	var model string
	if err := json.Unmarshal(obj["model"], &model); err != nil {
		t.Fatalf("model: %v", err)
	}
	if model != "override-model" {
		t.Errorf("model = %q, want override-model", model)
	}

	var stream bool
	if err := json.Unmarshal(obj["stream"], &stream); err != nil {
		t.Fatalf("stream: %v", err)
	}
	if !stream {
		t.Errorf("stream = %v, want true", stream)
	}

	if _, ok := obj["temperature"]; !ok {
		t.Errorf("temperature not preserved: %s", body)
	}
	if _, ok := obj["custom_param"]; !ok {
		t.Errorf("custom_param not preserved: %s", body)
	}
}

const roleUser = "user"

func TestBuildBodyFallbackWithoutExtra(t *testing.T) {
	req := &ChatRequest{
		Model:  "m",
		Stream: false,
		Messages: []Message{
			{Role: "system", Content: "sys"},
			{Role: roleUser, Content: roleUser},
		},
	}

	body, err := buildBody(req)
	if err != nil {
		t.Fatalf("buildBody: %v", err)
	}

	var obj struct {
		Model    string `json:"model"`
		Stream   bool   `json:"stream"`
		Messages []struct {
			Role    string `json:"role"`
			Content string `json:"content"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(body, &obj); err != nil {
		t.Fatalf("unmarshal body: %v", err)
	}
	if obj.Model != "m" {
		t.Errorf("model = %q", obj.Model)
	}
	if obj.Stream {
		t.Errorf("stream = true, want false")
	}
	if len(obj.Messages) != 2 || obj.Messages[1].Content != roleUser {
		t.Errorf("messages = %+v", obj.Messages)
	}
}

func TestBuildNonStreamingBodyForcesStreamFalse(t *testing.T) {
	req := &ChatRequest{
		Model:  "m",
		Stream: true,
		Extra:  []byte(`{"model":"m","stream":true,"temperature":0.5}`),
	}

	body, err := buildNonStreamingBody(req)
	if err != nil {
		t.Fatalf("buildNonStreamingBody: %v", err)
	}

	var obj map[string]json.RawMessage
	if err := json.Unmarshal(body, &obj); err != nil {
		t.Fatalf("unmarshal body: %v", err)
	}

	var stream bool
	if err := json.Unmarshal(obj["stream"], &stream); err != nil {
		t.Fatalf("stream: %v", err)
	}
	if stream {
		t.Errorf("stream = true, want false")
	}
	if _, ok := obj["temperature"]; !ok {
		t.Errorf("temperature not preserved: %s", body)
	}
}

func TestRawMessageToString(t *testing.T) {
	if got := rawMessageToString(nil); got != "" {
		t.Errorf("nil raw = %q, want empty", got)
	}
	if got := rawMessageToString(json.RawMessage(`"hello"`)); got != "hello" {
		t.Errorf("json string = %q, want hello", got)
	}
	// Non-string JSON is preserved as-is.
	object := json.RawMessage(`{"type":"text","text":"hi"}`)
	if got := rawMessageToString(object); got != string(object) {
		t.Errorf("object raw = %q, want unchanged", got)
	}
	// JSON null unmarshals into an empty string.
	if got := rawMessageToString(json.RawMessage(`null`)); got != "" {
		t.Errorf("null raw = %q, want empty", got)
	}
}

func TestMustJSONRaw(t *testing.T) {
	var got string
	if err := json.Unmarshal(mustJSONRaw(`a"b`), &got); err != nil {
		t.Fatalf("mustJSONRaw: %v", err)
	}
	if got != `a"b` {
		t.Errorf("round trip = %q", got)
	}
}

func TestReadSSEDataSingleAndMulti(t *testing.T) {
	stream := strings.NewReader(
		"id: 1\n" +
			": comment\n" +
			`data: {"a":1}` + "\n" +
			"\n" +
			`data: line1` + "\n" +
			`data: line2` + "\n" +
			"\n" +
			`data: [DONE]` + "\n" +
			"\n",
	)
	reader := bufio.NewReader(stream)

	first, err := readSSEData(reader)
	if err != nil {
		t.Fatalf("first read: %v", err)
	}
	if first != `{"a":1}` {
		t.Errorf("first = %q", first)
	}

	second, err := readSSEData(reader)
	if err != nil {
		t.Fatalf("second read: %v", err)
	}
	if second != "line1\nline2" {
		t.Errorf("second = %q, want joined multi-line", second)
	}

	done, err := readSSEData(reader)
	if err != nil {
		t.Fatalf("done read: %v", err)
	}
	if done != "[DONE]" {
		t.Errorf("done = %q", done)
	}

	if _, err := readSSEData(reader); !errors.Is(err, io.EOF) {
		t.Errorf("expected EOF, got %v", err)
	}
}

func TestReadSSEDataTrailingDataWithoutNewline(t *testing.T) {
	stream := strings.NewReader("data: tail")
	reader := bufio.NewReader(stream)

	data, err := readSSEData(reader)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("expected EOF, got %v", err)
	}
	if data != "tail" {
		t.Errorf("data = %q, want tail", data)
	}
}

func TestCompleteSyncSuccess(t *testing.T) {
	var gotAuth string
	var gotPath string

	srv := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			gotAuth = r.Header.Get("Authorization")
			gotPath = r.URL.Path
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{
				"id":"cmpl-1",
				"object":"chat.completion",
				"created":123,
				"model":"m",
				"choices":[{"index":0,
					"message":{"role":"assistant","content":"Hello"},
					"finish_reason":"stop"}],
				"usage":{"prompt_tokens":1,"completion_tokens":2,"total_tokens":3}
			}`))
		}))
	defer srv.Close()

	client := newTestClient(srv.URL, "m", "secret-key",
		time.Second, 5*time.Second)

	resp, err := client.CompleteSync(
		context.Background(),
		&ChatRequest{
			Model:    "m",
			Messages: []Message{{Role: roleUser, Content: "hi"}},
		},
	)
	if err != nil {
		t.Fatalf("CompleteSync: %v", err)
	}
	if gotPath != "/chat/completions" {
		t.Errorf("path = %q", gotPath)
	}
	if gotAuth != "Bearer secret-key" {
		t.Errorf("authorization = %q", gotAuth)
	}
	if resp.ID != "cmpl-1" {
		t.Errorf("id = %q", resp.ID)
	}
	var content string
	if err := json.Unmarshal(resp.Choices[0].Message.Content, &content); err != nil {
		t.Fatalf("content: %v", err)
	}
	if content != "Hello" {
		t.Errorf("content = %q", content)
	}
	if resp.Usage == nil || resp.Usage.TotalTokens != 3 {
		t.Errorf("usage = %+v", resp.Usage)
	}
}

func TestCompleteSyncNonRetryable401(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":{"message":"Invalid API Key"}}`))
		}))
	defer srv.Close()

	client := newTestClient(srv.URL, "m", "bad-key",
		time.Second, 5*time.Second)

	_, err := client.CompleteSync(
		context.Background(),
		&ChatRequest{Model: "m"},
	)
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, ErrNonRetryable) {
		t.Errorf("401 must be non-retryable, got %v", err)
	}
}

func TestCompleteSyncRetryable500(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`internal error`))
		}))
	defer srv.Close()

	client := newTestClient(srv.URL, "m", "",
		time.Second, 5*time.Second)

	_, err := client.CompleteSync(
		context.Background(),
		&ChatRequest{Model: "m"},
	)
	if err == nil {
		t.Fatal("expected error")
	}
	if errors.Is(err, ErrNonRetryable) {
		t.Errorf("500 must be retryable, got %v", err)
	}
}

func TestCompleteSyncNoChoices(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{"id":"x","choices":[]}`))
		}))
	defer srv.Close()

	client := newTestClient(srv.URL, "m", "",
		time.Second, 5*time.Second)

	_, err := client.CompleteSync(
		context.Background(),
		&ChatRequest{Model: "m"},
	)
	if err == nil {
		t.Fatal("expected error for empty choices")
	}
}

func collectEvents(
	t *testing.T,
	events <-chan ProviderEvent,
) []ProviderEvent {
	t.Helper()

	var out []ProviderEvent

	for ev := range events {
		out = append(out, ev)
		if ev.Type == EventDone || ev.Type == EventError {
			break
		}
	}

	return out
}

func TestCompleteNonStreamingConvertsToChunks(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{
				"id":"cmpl-2",
				"created":9,
				"model":"gemma",
				"choices":[{"index":0,
					"message":{"role":"assistant","content":"Answer text"},
					"finish_reason":"length"}]
			}`))
		}))
	defer srv.Close()

	client := newTestClient(srv.URL, "gemma", "",
		time.Second, 5*time.Second)

	events := make(chan ProviderEvent, 16)

	err := client.Complete(
		context.Background(),
		&ChatRequest{Model: "gemma", Stream: false},
		events,
	)
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}

	got := collectEvents(t, events)

	if len(got) != 3 {
		t.Fatalf("events = %d, want 3 (content + finish + done)", len(got))
	}
	if got[0].Type != EventRawSSE || !strings.Contains(got[0].Raw, "Answer text") {
		t.Errorf("first chunk = %q", got[0].Raw)
	}
	if got[1].Type != EventRawSSE || !strings.Contains(got[1].Raw, `"finish_reason":"length"`) {
		t.Errorf("finish chunk = %q", got[1].Raw)
	}
	if got[2].Type != EventDone {
		t.Errorf("last event = %q, want done", got[2].Type)
	}
}

func TestCompleteStreamingPassthrough(t *testing.T) {
	const chunk1 = `{"choices":[{"delta":{"reasoning_content":"think"}}]}`
	const chunk2 = `{"choices":[{"delta":{"content":"Hi"}}]}`

	srv := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Accept") != "text/event-stream" {
				t.Errorf("accept = %q, want text/event-stream",
					r.Header.Get("Accept"))
			}
			w.Header().Set("Content-Type", "text/event-stream")
			fl := w.(http.Flusher)
			_, _ = fmt.Fprintf(w, "data: %s\n\n", chunk1)
			fl.Flush()
			_, _ = fmt.Fprintf(w, "data: %s\n\n", chunk2)
			fl.Flush()
			_, _ = w.Write([]byte("data: [DONE]\n\n"))
			fl.Flush()
		}))
	defer srv.Close()

	client := newTestClient(srv.URL, "heavy", "",
		2*time.Second, 5*time.Second)

	events := make(chan ProviderEvent, 16)

	err := client.Complete(
		context.Background(),
		&ChatRequest{Model: "heavy", Stream: true},
		events,
	)
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}

	got := collectEvents(t, events)

	if len(got) != 3 {
		t.Fatalf("events = %d, want 3 (raw + raw + done)", len(got))
	}
	if got[0].Type != EventRawSSE || got[0].Raw != chunk1 {
		t.Errorf("first raw = %q, want verbatim %q", got[0].Raw, chunk1)
	}
	if got[1].Type != EventRawSSE || got[1].Raw != chunk2 {
		t.Errorf("second raw = %q, want verbatim %q", got[1].Raw, chunk2)
	}
	if got[2].Type != EventDone {
		t.Errorf("last = %q, want done", got[2].Type)
	}
}

func TestCompleteStreamingIdleTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			fl := w.(http.Flusher)
			_, _ = w.Write([]byte("data: {\"first\":true}\n\n"))
			fl.Flush()
			// Stay silent longer than the idle timeout, then close
			// the stream so the client read fails.
			time.Sleep(400 * time.Millisecond)
		}))
	defer srv.Close()

	client := newTestClient(srv.URL, "heavy", "",
		200*time.Millisecond, 5*time.Second)

	events := make(chan ProviderEvent, 16)

	err := client.Complete(
		context.Background(),
		&ChatRequest{Model: "heavy", Stream: true},
		events,
	)
	if err == nil {
		t.Fatal("expected idle timeout error")
	}
	if !strings.Contains(err.Error(), "idle timeout") {
		t.Errorf("err = %v, want idle timeout", err)
	}
}

func TestCompleteErrorEventsForNilRequest(t *testing.T) {
	client := newTestClient("http://127.0.0.1:1", "m", "",
		time.Second, time.Second)

	events := make(chan ProviderEvent, 4)

	err := client.Complete(context.Background(), nil, events)
	if err == nil {
		t.Fatal("expected error")
	}

	got := collectEvents(t, events)
	if len(got) != 1 || got[0].Type != EventError {
		t.Errorf("events = %+v, want single error", got)
	}
}

func TestIsRetryable(t *testing.T) {
	if IsRetryable(nil) {
		t.Error("nil must not be retryable")
	}
	if IsRetryable(ErrNonRetryable) {
		t.Error("ErrNonRetryable must not be retryable")
	}
	if !IsRetryable(errors.New("timeout")) {
		t.Error("plain error must be retryable")
	}
	wrapped := fmt.Errorf("outer: %w", ErrNonRetryable)
	if IsRetryable(wrapped) {
		t.Error("wrapped ErrNonRetryable must not be retryable")
	}
}
