package litellm

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/kafka-llm-gateway/gateway/internal/config"
	"github.com/kafka-llm-gateway/gateway/internal/providers"
)

func testConfig(url, model string) config.Config {
	cfg := config.Default()
	cfg.LiteLLMURL = url
	cfg.LiteLLMModel = model
	cfg.LiteLLMTimeout = 10 * time.Second
	return cfg
}

func TestNameAndModel(t *testing.T) {
	client := New(testConfig("http://127.0.0.1:1", "gemma-4"))
	if client.Name() != "litellm" {
		t.Errorf("name = %q", client.Name())
	}
	if client.Model() != "gemma-4" {
		t.Errorf("model = %q", client.Model())
	}
}

func TestCompleteNonStreamingConvertsToChunks(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{
				"id":"cmpl-l",
				"created":1,
				"model":"gemma-4",
				"choices":[{"index":0,
					"message":{"role":"assistant","content":"lite answer"},
					"finish_reason":"stop"}]
			}`))
		}))
	defer srv.Close()

	client := New(testConfig(srv.URL, "gemma-4"))

	events := make(chan providers.ProviderEvent, 8)
	err := client.Complete(context.Background(),
		&providers.ChatRequest{Model: "gemma-4", Stream: false},
		events)
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}

	var raws []string
	done := false

	for ev := range events {
		switch ev.Type {
		case providers.EventRawSSE:
			raws = append(raws, ev.Raw)
		case providers.EventDone:
			done = true
		case providers.EventError:
			t.Fatalf("unexpected error: %v", ev.Err)
		}
	}

	if !done {
		t.Error("expected done event")
	}
	if len(raws) != 2 {
		t.Fatalf("raw events = %d, want 2", len(raws))
	}
	if !strings.Contains(raws[0], "lite answer") {
		t.Errorf("content chunk = %q", raws[0])
	}
}
