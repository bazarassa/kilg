package litellm

// Package litellm implements the LiteLLM-compatible provider.
//
// LiteLLM использует OpenAI-compatible API:
//
// POST /v1/chat/completions
//
// Поэтому фактический HTTP client общий с heavy provider.

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
		inner: providers.NewOpenAIClient(
			providers.OpenAIConfig{
				BaseURL: cfg.LiteLLMURL,
				Model:   cfg.LiteLLMModel,
				APIKey:  cfg.LiteLLMAPIKey,

				ConnectTimeout: 10 * time.Second,

				// llama.cpp может долго загружать модель
				// или ждать начала generation.
				HeaderTimeout: 2 * time.Minute,

				// Для streaming важно не считать нормальную
				// паузу между chunks ошибкой.
				IdleTimeout: 10 * time.Minute,

				// Общий timeout всей генерации.
				GenerationTimeout: cfg.LiteLLMTimeout,
			},
		),
	}
}

// Name implements providers.Provider.
func (c *Client) Name() string {
	return "litellm"
}

// Model implements providers.Provider.
func (c *Client) Model() string {
	return c.inner.Model()
}

// Complete implements providers.Provider.
//
// Поддерживает:
//
// stream=true
// -> raw SSE passthrough
//
// stream=false
// -> OpenAI JSON completion преобразуется в SSE chunks
// на уровне OpenAIClient.
func (c *Client) Complete(
	ctx context.Context,
	req *providers.ChatRequest,
	events chan<- providers.ProviderEvent,
) error {
	return c.inner.Complete(ctx, req, events)
}
