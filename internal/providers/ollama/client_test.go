package ollama

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/kafka-llm-gateway/gateway/internal/config"
	"github.com/kafka-llm-gateway/gateway/internal/providers"
)

const llamaModel = "llama3.2"

func testClient(t *testing.T, url string) *Client {
	t.Helper()
	cfg := config.Default()
	cfg.OllamaURL = url
	cfg.OllamaModel = "llama3.2:3b-instruct-q4_K_M"
	cfg.OllamaEmbedModel = "nomic-embed-text:latest"
	cfg.OllamaTimeout = 5 * time.Second
	return New(cfg)
}

func TestBuildGenerateBody(t *testing.T) {
	req := &providers.ChatRequest{
		Model:  llamaModel,
		Stream: true,
		Messages: []providers.Message{
			{Role: "system", Content: "sys"},
			{Role: "user", Content: "hello"},
		},
		Extra: []byte(`{"options":{"temperature":0.2}}`),
	}

	body, err := buildGenerateBody(req, true)
	if err != nil {
		t.Fatalf("buildGenerateBody: %v", err)
	}

	var obj struct {
		Model  string `json:"model"`
		Prompt string `json:"prompt"`
		Stream bool   `json:"stream"`
	}
	if err := json.Unmarshal(body, &obj); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if obj.Model != llamaModel {
		t.Errorf("model = %q", obj.Model)
	}
	if !obj.Stream {
		t.Error("stream = false, want true")
	}
	for _, part := range []string{
		"System: sys", "User: hello", "Assistant:",
	} {
		if !strings.Contains(obj.Prompt, part) {
			t.Errorf("prompt missing %q: %q", part, obj.Prompt)
		}
	}

	var full struct {
		Options map[string]any `json:"options"`
	}
	if err := json.Unmarshal(body, &full); err != nil {
		t.Fatalf("unmarshal options: %v", err)
	}
	if full.Options == nil {
		t.Error("options not preserved from Extra")
	}
}

func TestBuildOllamaPromptRoles(t *testing.T) {
	prompt := buildOllamaPrompt([]providers.Message{
		{Role: "SYSTEM", Content: "upper"},
		{Role: "user", Content: "text"},
		{Role: "tool", Content: "special"},
	})
	for _, part := range []string{
		"System: upper", "User: text", "tool: special",
	} {
		if !strings.Contains(prompt, part) {
			t.Errorf("prompt missing %q: %q", part, prompt)
		}
	}
	if !strings.HasSuffix(prompt, "Assistant:") {
		t.Errorf("prompt must end with Assistant:: %q", prompt)
	}
}

func TestBuildOpenAIChunk(t *testing.T) {
	chunk := buildOpenAIChunk("id1", 123, "model1", "hi", "assistant", "", nil)

	var obj struct {
		ID      string `json:"id"`
		Object  string `json:"object"`
		Created int64  `json:"created"`
		Model   string `json:"model"`
		Choices []struct {
			Index        int            `json:"index"`
			Delta        map[string]any `json:"delta"`
			FinishReason *string        `json:"finish_reason"`
		} `json:"choices"`
	}
	if err := json.Unmarshal([]byte(chunk), &obj); err != nil {
		t.Fatalf("unmarshal chunk: %v", err)
	}
	if obj.Object != "chat.completion.chunk" {
		t.Errorf("object = %q", obj.Object)
	}
	if obj.Choices[0].Delta["role"] != "assistant" ||
		obj.Choices[0].Delta["content"] != "hi" {
		t.Errorf("delta = %v", obj.Choices[0].Delta)
	}
	if obj.Choices[0].FinishReason != nil {
		t.Errorf("finish_reason = %v, want null", obj.Choices[0].FinishReason)
	}

	finish := buildOpenAIChunk("id1", 123, "model1", "", "", "stop",
		&openAIUsage{PromptTokens: 1, CompletionTokens: 2, TotalTokens: 3})
	var fobj struct {
		Choices []struct {
			FinishReason string `json:"finish_reason"`
		} `json:"choices"`
		Usage *openAIUsage `json:"usage"`
	}
	if err := json.Unmarshal([]byte(finish), &fobj); err != nil {
		t.Fatalf("unmarshal finish chunk: %v", err)
	}
	if fobj.Choices[0].FinishReason != "stop" {
		t.Errorf("finish_reason = %q", fobj.Choices[0].FinishReason)
	}
	if fobj.Usage == nil || fobj.Usage.TotalTokens != 3 {
		t.Errorf("usage = %+v", fobj.Usage)
	}
}

func TestCompleteNonStreamingEmitsCompletionObject(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/api/generate" {
				t.Errorf("path = %q", r.URL.Path)
			}
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			if body["stream"] != false {
				t.Errorf("stream = %v, want false", body["stream"])
			}
			_, _ = w.Write([]byte(`{
				"model":"llama3.2",
				"response":"Hi there",
				"done":true,
				"done_reason":"stop",
				"prompt_eval_count":4,
				"eval_count":7
			}`))
		}))
	defer srv.Close()

	client := testClient(t, srv.URL)

	events := make(chan providers.ProviderEvent, 8)
	err := client.Complete(context.Background(),
		&providers.ChatRequest{Model: llamaModel, Stream: false}, events)
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}

	var raws []string
	for ev := range events {
		switch ev.Type {
		case providers.EventRawSSE:
			raws = append(raws, ev.Raw)
		case providers.EventError:
			t.Fatalf("unexpected error event: %v", ev.Err)
		}
	}

	if len(raws) != 1 {
		t.Fatalf("raw events = %d, want 1", len(raws))
	}
	checkCompletionObject(t, raws[0])
}

// checkCompletionObject verifies a chat.completion JSON payload.
func checkCompletionObject(t *testing.T, raw string) {
	t.Helper()
	var comp struct {
		Object  string `json:"object"`
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
			FinishReason string `json:"finish_reason"`
		} `json:"choices"`
		Usage *struct {
			PromptTokens     int `json:"prompt_tokens"`
			CompletionTokens int `json:"completion_tokens"`
			TotalTokens      int `json:"total_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal([]byte(raw), &comp); err != nil {
		t.Fatalf("unmarshal completion: %v", err)
	}
	if comp.Object != "chat.completion" {
		t.Errorf("object = %q, want chat.completion", comp.Object)
	}
	if comp.Choices[0].Message.Content != "Hi there" {
		t.Errorf("content = %q", comp.Choices[0].Message.Content)
	}
	if comp.Usage == nil || comp.Usage.TotalTokens != 11 {
		t.Errorf("usage = %+v", comp.Usage)
	}
}

func TestCompleteStreamingEmitsOpenAIChunks(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/x-ndjson")
			lines := []string{
				`{"model":"m","response":"Hel","done":false}`,
				`{"model":"m","response":"lo","done":false}`,
				`{"model":"m","response":"","done":true,"done_reason":"stop","prompt_eval_count":2,"eval_count":3}`,
			}
			for _, line := range lines {
				_, _ = fmt.Fprintln(w, line)
			}
		}))
	defer srv.Close()

	client := testClient(t, srv.URL)

	events := make(chan providers.ProviderEvent, 16)
	err := client.Complete(context.Background(),
		&providers.ChatRequest{Model: "m", Stream: true}, events)
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}

	var raws []string
	for ev := range events {
		if ev.Type == providers.EventRawSSE {
			raws = append(raws, ev.Raw)
		}
	}

	// role chunk + 2 content chunks + finish chunk.
	if len(raws) != 4 {
		t.Fatalf("raw events = %d, want 4", len(raws))
	}
	if !strings.Contains(raws[0], `"role":"assistant"`) {
		t.Errorf("first chunk missing role: %q", raws[0])
	}
	if !strings.Contains(raws[1], "Hel") ||
		!strings.Contains(raws[2], "lo") {
		t.Errorf("content chunks missing: %q %q", raws[1], raws[2])
	}
	if !strings.Contains(raws[3], `"finish_reason":"stop"`) {
		t.Errorf("finish chunk missing finish_reason: %q", raws[3])
	}
}

func TestCompleteStreamingErrorStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`model not found`))
		}))
	defer srv.Close()

	client := testClient(t, srv.URL)

	events := make(chan providers.ProviderEvent, 4)
	err := client.Complete(context.Background(),
		&providers.ChatRequest{Model: "m", Stream: true}, events)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "status 500") {
		t.Errorf("err = %v, want status 500", err)
	}

	var gotError bool
	for ev := range events {
		if ev.Type == providers.EventError {
			gotError = true
		}
	}
	if !gotError {
		t.Error("expected error event on channel")
	}
}

func TestEmbedSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/api/embed" {
				t.Errorf("path = %q", r.URL.Path)
			}
			var body struct {
				Model string   `json:"model"`
				In    []string `json:"input"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			if body.Model != "nomic-embed-text:latest" {
				t.Errorf("model = %q", body.Model)
			}
			if len(body.In) != 2 {
				t.Errorf("input = %v", body.In)
			}
			_, _ = w.Write([]byte(`{
				"model":"nomic-embed-text:latest",
				"embeddings":[[0.1,0.2],[0.3,0.4]]
			}`))
		}))
	defer srv.Close()

	client := testClient(t, srv.URL)

	resp, err := client.Embed(context.Background(), "", []string{"a", "b"})
	if err != nil {
		t.Fatalf("Embed: %v", err)
	}
	if resp.Object != "list" {
		t.Errorf("object = %q", resp.Object)
	}
	if len(resp.Data) != 2 || len(resp.Data[1].Embedding) != 2 {
		t.Errorf("data = %+v", resp.Data)
	}
	if resp.Data[0].Object != "embedding" || resp.Data[1].Index != 1 {
		t.Errorf("data objects = %+v", resp.Data)
	}
}

func TestEmbedStatusError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`model not found`))
		}))
	defer srv.Close()

	client := testClient(t, srv.URL)

	_, err := client.Embed(context.Background(), "m", []string{"a"})
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "status 404") {
		t.Errorf("err = %v, want status 404", err)
	}
}

func TestEnsureSuccess(t *testing.T) {
	ok := &http.Response{
		StatusCode: 200,
		Body:       io.NopCloser(strings.NewReader("")),
	}
	if err := ensureSuccess(ok); err != nil {
		t.Errorf("200: %v", err)
	}
	bad := &http.Response{
		StatusCode: 503,
		Body:       io.NopCloser(strings.NewReader("unavailable")),
	}
	err := ensureSuccess(bad)
	if err == nil {
		t.Fatal("503 must be an error")
	}
	if !strings.Contains(err.Error(), "status 503") {
		t.Errorf("err = %v, want status 503", err)
	}
}
