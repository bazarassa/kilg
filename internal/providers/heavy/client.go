// Package heavy implements the heavy LLM provider.
//
// Heavy provider используется worker'ом и отделён от HTTP Gateway.
//
// В текущем MVP worker использует non-streaming CompleteSync():
//
//     Kafka -> Worker -> Heavy -> HTTP -> JSON -> Worker -> Kafka
//
// Позже сюда можно вернуть streaming Complete(), reasoning,
// tool calls и т.д. — это не требуется для текущего этапа.
package heavy

import (
	"context"
	"time"

	"github.com/kafka-llm-gateway/gateway/internal/config"
	"github.com/kafka-llm-gateway/gateway/internal/protocol"
	"github.com/kafka-llm-gateway/gateway/internal/providers"
)

// Client is the heavy LLM provider.
type Client struct {
	inner *providers.OpenAIClient
}

// New builds the heavy provider client.
func New(cfg config.Config) *Client {
	return &Client{
		inner: providers.NewOpenAIClient(
			providers.OpenAIConfig{
				BaseURL: cfg.HeavyBaseURL,
				Model:   cfg.HeavyModel,
				APIKey:  cfg.HeavyAPIKey,

				// TCP connect + TLS handshake.
				ConnectTimeout: 10 * time.Second,

				// В non-streaming режиме ждём HTTP response headers.
				HeaderTimeout: 60 * time.Second,

				// Оставляем для совместимости с текущим
				// OpenAIClient.
				IdleTimeout: cfg.HeavyIdle,

				// Максимальное время одной генерации.
				GenerationTimeout: cfg.HeavyTimeout,
			},
		),
	}
}

// Name implements providers.Provider.
func (c *Client) Name() string {
	return "heavy"
}

// Model implements providers.Provider.
func (c *Client) Model() string {
	return c.inner.Model()
}

// CompleteSync performs a non-streaming completion.
//
// Это основной метод worker'а текущего MVP.
func (c *Client) CompleteSync(
	ctx context.Context,
	req *providers.ChatRequest,
) (*protocol.ChatResponse, error) {
	return c.inner.CompleteSync(ctx, req)
}

// Complete оставляем для совместимости с существующим
// providers.Provider и Gateway.
//
// Gateway напрямую heavy worker не использует.
func (c *Client) Complete(
	ctx context.Context,
	req *providers.ChatRequest,
	events chan<- providers.ProviderEvent,
) error {
	return c.inner.Complete(ctx, req, events)
}
