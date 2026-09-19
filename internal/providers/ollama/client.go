// Package ollama implements the Ollama provider (chat + embeddings).
package ollama

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/kafka-llm-gateway/gateway/internal/config"
	"github.com/kafka-llm-gateway/gateway/internal/providers"
)

// Client is the Ollama provider.
type Client struct {
	cfg  config.Config
	http *http.Client
}

// New builds the Ollama provider client.
func New(cfg config.Config) *Client {
	return &Client{
		cfg:  cfg,
		http: &http.Client{Timeout: cfg.OllamaTimeout},
	}
}

// Name implements providers.Provider.
func (c *Client) Name() string { return "ollama" }

// Model implements providers.Provider.
func (c *Client) Model() string { return c.cfg.OllamaModel }

// Complete implements providers.Provider using Ollama's OpenAI-compatible
// /v1/chat/completions endpoint with raw SSE passthrough.
func (c *Client) Complete(ctx context.Context, req *providers.ChatRequest, events chan<- providers.ProviderEvent) error {
	inner := providers.NewOpenAIClient(providers.OpenAIConfig{
		BaseURL:           strings.TrimRight(c.cfg.OllamaURL, "/") + "/v1",
		Model:             c.cfg.OllamaModel,
		APIKey:            c.cfg.OllamaAPIKey,
		ConnectTimeout:    10 * time.Second,
		HeaderTimeout:     30 * time.Second,
		IdleTimeout:       2 * time.Minute,
		GenerationTimeout: c.cfg.OllamaTimeout,
	})
	return inner.Complete(ctx, req, events)
}

// Embedding is one embedding vector.
type Embedding struct {
	Object    string    `json:"object"`
	Index     int       `json:"index"`
	Embedding []float64 `json:"embedding"`
}

// EmbeddingsResponse is the OpenAI-compatible embeddings response.
type EmbeddingsResponse struct {
	Object string      `json:"object"`
	Data   []Embedding `json:"data"`
	Model  string      `json:"model"`
	Usage  *struct {
		PromptTokens int `json:"prompt_tokens"`
		TotalTokens  int `json:"total_tokens"`
	} `json:"usage,omitempty"`
}

// Embed computes embeddings for the given inputs.
func (c *Client) Embed(ctx context.Context, model string, inputs []string) (*EmbeddingsResponse, error) {
	if model == "" {
		model = c.cfg.OllamaEmbedModel
	}
	body, err := json.Marshal(map[string]any{"model": model, "input": inputs})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		strings.TrimRight(c.cfg.OllamaURL, "/")+"/api/embed", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if c.cfg.OllamaAPIKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.cfg.OllamaAPIKey)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("ollama embed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("ollama embed status %d: %s", resp.StatusCode, strings.TrimSpace(string(msg)))
	}
	var raw struct {
		Embeddings [][]float64 `json:"embeddings"`
		Model      string      `json:"model"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return nil, fmt.Errorf("ollama embed decode: %w", err)
	}
	out := &EmbeddingsResponse{Object: "list", Model: raw.Model, Data: make([]Embedding, 0, len(raw.Embeddings))}
	for i, e := range raw.Embeddings {
		out.Data = append(out.Data, Embedding{Object: "embedding", Index: i, Embedding: e})
	}
	return out, nil
}
