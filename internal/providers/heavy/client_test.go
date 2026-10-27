package heavy

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/kafka-llm-gateway/gateway/internal/config"
	"github.com/kafka-llm-gateway/gateway/internal/providers"
)

const heavyModel = "heavy-model"

func testConfig(url, model string) config.Config {
	cfg := config.Default()
	cfg.HeavyBaseURL = url
	cfg.HeavyModel = model
	cfg.HeavyIdle = 5 * time.Second
	cfg.HeavyTimeout = 10 * time.Second
	return cfg
}

func TestNameAndModel(t *testing.T) {
	client := New(testConfig("http://127.0.0.1:1", heavyModel))
	if client.Name() != "heavy" {
		t.Errorf("name = %q", client.Name())
	}
	if client.Model() != heavyModel {
		t.Errorf("model = %q", client.Model())
	}
}

func TestCompleteSyncDelegates(t *testing.T) {
	var gotModel string

	srv := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			gotModel, _ = body["model"].(string)
			_, _ = w.Write([]byte(`{
				"id":"cmpl-h",
				"created":1,
				"model":"heavy-model",
				"choices":[{"index":0,
					"message":{"role":"assistant","content":"ok"},
					"finish_reason":"stop"}]
			}`))
		}))
	defer srv.Close()

	client := New(testConfig(srv.URL, heavyModel))

	resp, err := client.CompleteSync(context.Background(),
		&providers.ChatRequest{
			Model:    heavyModel,
			Messages: []providers.Message{{Role: "user", Content: "hi"}},
		})
	if err != nil {
		t.Fatalf("CompleteSync: %v", err)
	}
	if gotModel != heavyModel {
		t.Errorf("upstream model = %q", gotModel)
	}
	if len(resp.Choices) != 1 {
		t.Errorf("choices = %d", len(resp.Choices))
	}
}

func TestCompleteErrorPropagates(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`invalid key`))
		}))
	defer srv.Close()

	client := New(testConfig(srv.URL, heavyModel))

	events := make(chan providers.ProviderEvent, 4)
	err := client.Complete(context.Background(),
		&providers.ChatRequest{Model: heavyModel}, events)
	if err == nil {
		t.Fatal("expected error")
	}
	if providers.IsRetryable(err) {
		t.Error("401 must be non-retryable")
	}

	var gotError bool
	for ev := range events {
		if ev.Type == providers.EventError {
			gotError = true
		}
	}
	if !gotError {
		t.Error("expected error event")
	}
}
