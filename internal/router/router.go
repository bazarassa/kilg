// Package router selects the provider for an incoming chat request.
//
// MVP policy (deterministic):
//   - model == "heavy" (or the configured heavy model id) -> heavy (Kafka + worker)
//   - model == "litellm" (or the configured LiteLLM model id) -> litellm (direct)
//   - model == "ollama" (or the configured Ollama model id) -> ollama (direct)
//   - unknown model -> heavy (safest default: durable execution)
package router

import (
	"strings"

	"github.com/kafka-llm-gateway/gateway/internal/config"
)

// Route is the routing decision for one request.
type Route struct {
	Provider string // "heavy" | "litellm" | "ollama"
	Model    string // concrete upstream model id
	Async    bool   // true when the request goes through the Kafka job queue
}

// Router resolves model names to providers.
type Router struct {
	cfg config.Config
}

// New builds a router.
func New(cfg config.Config) *Router {
	return &Router{cfg: cfg}
}

// Route resolves the provider for a model name.
func (r *Router) Route(model string) Route {
	m := strings.ToLower(strings.TrimSpace(model))
	switch m {
	case "heavy", "":
		if m == "" {
			m = "heavy"
		}
		return Route{Provider: "heavy", Model: r.cfg.HeavyModel, Async: true}
	case r.cfg.HeavyModel, strings.ToLower(r.cfg.HeavyModel):
		return Route{Provider: "heavy", Model: r.cfg.HeavyModel, Async: true}
	case "litellm":
		return Route{Provider: "litellm", Model: r.cfg.LiteLLMModel, Async: false}
	case r.cfg.LiteLLMModel, strings.ToLower(r.cfg.LiteLLMModel):
		return Route{Provider: "litellm", Model: r.cfg.LiteLLMModel, Async: false}
	case "ollama":
		return Route{Provider: "ollama", Model: r.cfg.OllamaModel, Async: false}
	case r.cfg.OllamaModel, strings.ToLower(r.cfg.OllamaModel):
		return Route{Provider: "ollama", Model: r.cfg.OllamaModel, Async: false}
	default:
		// Unknown model: run durably on the heavy worker.
		return Route{Provider: "heavy", Model: model, Async: true}
	}
}

// Models lists the model ids exposed by /v1/models.
func (r *Router) Models() []string {
	return []string{
		"heavy",
		r.cfg.HeavyModel,
		"litellm",
		r.cfg.LiteLLMModel,
		"ollama",
		r.cfg.OllamaModel,
	}
}
