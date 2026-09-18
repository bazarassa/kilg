// Package heavy implements the heavy reasoning LLM provider (llama.cpp
// behind an OpenAI-compatible endpoint). Execution happens in the worker
// process, decoupled from the HTTP lifecycle.
package heavy

import (
	"context"
	"time"

	"github.com/kafka-llm-gateway/gateway/internal/config"
	"github.com/kafka-llm-gateway/gateway/internal/providers"
)

// Client is the heavy LLM provider.
type Client struct {
	inner *providers.OpenAIClient
}

// New builds the heavy provider client.
func New(cfg config.Config) *Client {
	return &Client{
		inner: providers.NewOpenAIClient(providers.OpenAIConfig{
			BaseURL:           cfg.HeavyBaseURL,
			Model:             cfg.HeavyModel,
			APIKey:            cfg.HeavyAPIKey,
			ConnectTimeout:    10 * time.Second,
			HeaderTimeout:     60 * time.Second,
			IdleTimeout:       cfg.HeavyIdle,
			GenerationTimeout: cfg.HeavyTimeout,
		}),
	}
}

// Name implements providers.Provider.
func (c *Client) Name() string { return "heavy" }

// Model implements providers.Provider.
func (c *Client) Model() string { return c.inner.Model() }

// Complete implements providers.Provider.
func (c *Client) Complete(ctx context.Context, req *providers.ChatRequest, events chan<- providers.ProviderEvent) error {
	return c.inner.Complete(ctx, req, events)
}
