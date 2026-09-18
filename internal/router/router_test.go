package router

import (
	"testing"

	"github.com/kafka-llm-gateway/gateway/internal/config"
)

func cfg() config.Config {
	c := config.Default()
	c.HeavyModel = "HauhauCS/Qwen"
	c.LiteLLMModel = "gemma-4-E4B-it-GGUF"
	c.OllamaModel = "llama3.2:3b-instruct-q4_K_M"
	return c
}

func TestRouteHeavy(t *testing.T) {
	r := New(cfg())
	route := r.Route("heavy")
	if route.Provider != "heavy" || !route.Async {
		t.Errorf("route = %+v", route)
	}
	// Full model id also routes to heavy.
	route = r.Route("HauhauCS/Qwen")
	if route.Provider != "heavy" {
		t.Errorf("full model id route = %+v", route)
	}
}

func TestRouteLiteLLM(t *testing.T) {
	r := New(cfg())
	route := r.Route("litellm")
	if route.Provider != "litellm" || route.Async {
		t.Errorf("route = %+v", route)
	}
	route = r.Route("gemma-4-E4B-it-GGUF")
	if route.Provider != "litellm" {
		t.Errorf("full model id route = %+v", route)
	}
}

func TestRouteOllama(t *testing.T) {
	r := New(cfg())
	route := r.Route("ollama")
	if route.Provider != "ollama" || route.Async {
		t.Errorf("route = %+v", route)
	}
	route = r.Route("llama3.2:3b-instruct-q4_K_M")
	if route.Provider != "ollama" {
		t.Errorf("full model id route = %+v", route)
	}
}

func TestRouteUnknownDefaultsToHeavy(t *testing.T) {
	r := New(cfg())
	route := r.Route("some-unknown-model")
	if route.Provider != "heavy" || !route.Async {
		t.Errorf("unknown model should default to heavy async: %+v", route)
	}
}
