// Package litellm implements the LiteLLM provider (fast instruct models).
package litellm

import (
	"context"
	"time"

	"github.com/kafka-llm-gateway/gateway/internal/config"
	"github.com/kafka-llm-gateway/gateway/internal/providers"
)

// Client is the LiteLLM provider.
type Client struct {
	inner *providers.OpenAIClient
}

// New builds the LiteLLM provider client.
func New(cfg config.Config) *Client {
	return &Client{
		inner: providers.NewOpenAIClient(providers.OpenAIConfig{
			BaseURL:           cfg.LiteLLMURL,
			Model:             cfg.LiteLLMModel,
			APIKey:            cfg.LiteLLMAPIKey,
			ConnectTimeout:    10 * time.Second,
			HeaderTimeout:     30 * time.Second,
			IdleTimeout:       2 * time.Minute,
			GenerationTimeout: cfg.LiteLLMTimeout,
		}),
	}
}

// Name implements providers.Provider.
func (c *Client) Name() string { return "litellm" }

// Model implements providers.Provider.
func (c *Client) Model() string { return c.inner.Model() }

// Complete implements providers.Provider.
func (c *Client) Complete(ctx context.Context, req *providers.ChatRequest, events chan<- providers.ProviderEvent) error {
	return c.inner.Complete(ctx, req, events)
}
