// Package ollama implements the Ollama provider.
//
// Chat completion uses Ollama's native endpoint:
//
//	POST /api/generate
//
// Request modes:
//
//	req.Stream == true:
//	    Ollama stream=true -> NDJSON ->
//	    OpenAI-compatible chat.completion.chunk events.
//
//	req.Stream == false:
//	    Ollama stream=false -> one JSON response ->
//	    one complete OpenAI-compatible chat.completion event.
//
// Embeddings use:
//
//	POST /api/embed
package ollama

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/kafka-llm-gateway/gateway/internal/config"
	"github.com/kafka-llm-gateway/gateway/internal/providers"
)

// Client is the Ollama provider.
type Client struct {
	cfg *config.Config

	// generateHTTP is used for chat generation.
	//
	// It intentionally has no http.Client.Timeout because model generation
	// may legitimately take longer than 10 minutes.
	generateHTTP *http.Client

	// http is used for short native API calls, such as embeddings.
	http *http.Client
}

// New builds the Ollama provider client.
func New(cfg config.Config) *Client {
	dialer := &net.Dialer{
		Timeout:   10 * time.Second,
		KeepAlive: 30 * time.Second,
	}

	transport := &http.Transport{
		DialContext: func(
			ctx context.Context,
			network string,
			addr string,
		) (net.Conn, error) {
			return dialer.DialContext(ctx, network, addr)
		},
		TLSClientConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
		},
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 2 * time.Minute,
		MaxIdleConns:          16,
		IdleConnTimeout:       90 * time.Second,
	}

	return &Client{
		cfg: &cfg,

		generateHTTP: &http.Client{
			Transport: transport,
		},

		http: &http.Client{
			Timeout: cfg.OllamaTimeout,
		},
	}
}

// Name implements providers.Provider.
func (c *Client) Name() string {
	return "ollama"
}

// Model implements providers.Provider.
func (c *Client) Model() string {
	return c.cfg.OllamaModel
}

// Complete implements providers.Provider.
func (c *Client) Complete(
	ctx context.Context,
	req *providers.ChatRequest,
	events chan<- providers.ProviderEvent,
) error {
	if req.Stream {
		return c.completeStreaming(ctx, req, events)
	}

	return c.completeNonStreaming(ctx, req, events)
}

// completeStreaming calls /api/generate with stream=true and converts
// native Ollama NDJSON into OpenAI-compatible chat.completion.chunk events.
func (c *Client) completeStreaming(
	ctx context.Context,
	req *providers.ChatRequest,
	events chan<- providers.ProviderEvent,
) error {
	body, err := buildGenerateBody(req, true)
	if err != nil {
		return fmt.Errorf("build ollama generate request: %w", err)
	}

	generationCtx, cancel := c.generationContext(ctx)
	defer cancel()

	httpReq, err := http.NewRequestWithContext(
		generationCtx,
		http.MethodPost,
		strings.TrimRight(c.cfg.OllamaURL, "/")+"/api/generate",
		bytes.NewReader(body),
	)
	if err != nil {
		return fmt.Errorf("create ollama generate request: %w", err)
	}

	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "application/x-ndjson")

	if c.cfg.OllamaAPIKey != "" {
		httpReq.Header.Set(
			"Authorization",
			"Bearer "+c.cfg.OllamaAPIKey,
		)
	}

	resp, err := c.generateHTTP.Do(httpReq)
	if err != nil {
		return fmt.Errorf("ollama generate request: %w", err)
	}

	defer resp.Body.Close()

	if err := ensureSuccess(resp); err != nil {
		events <- providers.ProviderEvent{
			Type: providers.EventError,
			Err:  err,
		}

		close(events)
		return err
	}

	completionID := newCompletionID()
	created := time.Now().Unix()
	sentRole := false

	reader := bufio.NewReaderSize(resp.Body, 64*1024)

	for {
		line, readErr := reader.ReadString('\n')
		line = strings.TrimSpace(line)

		if line != "" {
			var native struct {
				Model           string `json:"model"`
				Response        string `json:"response"`
				Done            bool   `json:"done"`
				DoneReason      string `json:"done_reason"`
				PromptEvalCount int    `json:"prompt_eval_count"`
				EvalCount       int    `json:"eval_count"`
			}

			if err := json.Unmarshal([]byte(line), &native); err != nil {
				eventErr := fmt.Errorf(
					"decode ollama generate chunk: %w",
					err,
				)

				events <- providers.ProviderEvent{
					Type: providers.EventError,
					Err:  eventErr,
				}

				close(events)
				return eventErr
			}

			if !sentRole {
				events <- providers.ProviderEvent{
					Type: providers.EventRawSSE,
					Raw: buildOpenAIChunk(
						completionID,
						created,
						native.Model,
						"",
						"assistant",
						"",
						nil,
					),
				}

				sentRole = true
			}

			if native.Response != "" {
				events <- providers.ProviderEvent{
					Type: providers.EventRawSSE,
					Raw: buildOpenAIChunk(
						completionID,
						created,
						native.Model,
						native.Response,
						"",
						"",
						nil,
					),
				}
			}

			if native.Done {
				var usage *openAIUsage

				if native.PromptEvalCount > 0 || native.EvalCount > 0 {
					usage = &openAIUsage{
						PromptTokens:     native.PromptEvalCount,
						CompletionTokens: native.EvalCount,
						TotalTokens:      native.PromptEvalCount + native.EvalCount,
					}
				}

				finishReason := native.DoneReason
				if finishReason == "" {
					finishReason = "stop"
				}

				events <- providers.ProviderEvent{
					Type: providers.EventRawSSE,
					Raw: buildOpenAIChunk(
						completionID,
						created,
						native.Model,
						"",
						"",
						finishReason,
						usage,
					),
				}

				break
			}
		}

		if readErr != nil {
			if readErr != io.EOF {
				eventErr := fmt.Errorf(
					"read ollama generate stream: %w",
					readErr,
				)

				events <- providers.ProviderEvent{
					Type: providers.EventError,
					Err:  eventErr,
				}

				close(events)
				return eventErr
			}

			break
		}
	}

	events <- providers.ProviderEvent{
		Type: providers.EventDone,
	}

	close(events)
	return nil
}

// completeNonStreaming calls /api/generate with stream=false and emits one
// complete OpenAI-compatible chat.completion object.
func (c *Client) completeNonStreaming(
	ctx context.Context,
	req *providers.ChatRequest,
	events chan<- providers.ProviderEvent,
) error {
	body, err := buildGenerateBody(req, false)
	if err != nil {
		return fmt.Errorf("build ollama generate request: %w", err)
	}

	generationCtx, cancel := c.generationContext(ctx)
	defer cancel()

	httpReq, err := http.NewRequestWithContext(
		generationCtx,
		http.MethodPost,
		strings.TrimRight(c.cfg.OllamaURL, "/")+"/api/generate",
		bytes.NewReader(body),
	)
	if err != nil {
		return fmt.Errorf("create ollama generate request: %w", err)
	}

	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "application/json")

	if c.cfg.OllamaAPIKey != "" {
		httpReq.Header.Set(
			"Authorization",
			"Bearer "+c.cfg.OllamaAPIKey,
		)
	}

	resp, err := c.generateHTTP.Do(httpReq)
	if err != nil {
		return fmt.Errorf("ollama generate request: %w", err)
	}

	defer resp.Body.Close()

	if err := ensureSuccess(resp); err != nil {
		events <- providers.ProviderEvent{
			Type: providers.EventError,
			Err:  err,
		}

		close(events)
		return err
	}

	var native struct {
		Model           string `json:"model"`
		Response        string `json:"response"`
		Done            bool   `json:"done"`
		DoneReason      string `json:"done_reason"`
		PromptEvalCount int    `json:"prompt_eval_count"`
		EvalCount       int    `json:"eval_count"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&native); err != nil {
		eventErr := fmt.Errorf(
			"decode ollama generate response: %w",
			err,
		)

		events <- providers.ProviderEvent{
			Type: providers.EventError,
			Err:  eventErr,
		}

		close(events)
		return eventErr
	}

	finishReason := native.DoneReason
	if finishReason == "" {
		finishReason = "stop"
	}

	completion := map[string]any{
		"id":      newCompletionID(),
		"object":  "chat.completion",
		"created": time.Now().Unix(),
		"model":   native.Model,
		"choices": []any{
			map[string]any{
				"index": 0,
				"message": map[string]any{
					"role":    "assistant",
					"content": native.Response,
				},
				"finish_reason": finishReason,
			},
		},
	}

	if native.PromptEvalCount > 0 || native.EvalCount > 0 {
		completion["usage"] = openAIUsage{
			PromptTokens:     native.PromptEvalCount,
			CompletionTokens: native.EvalCount,
			TotalTokens:      native.PromptEvalCount + native.EvalCount,
		}
	}

	encoded, err := json.Marshal(completion)
	if err != nil {
		eventErr := fmt.Errorf(
			"encode ollama completion response: %w",
			err,
		)

		events <- providers.ProviderEvent{
			Type: providers.EventError,
			Err:  eventErr,
		}

		close(events)
		return eventErr
	}

	// One complete OpenAI-compatible response is emitted as a single event.
	events <- providers.ProviderEvent{
		Type: providers.EventRawSSE,
		Raw:  string(encoded),
	}

	events <- providers.ProviderEvent{
		Type: providers.EventDone,
	}

	close(events)
	return nil
}

// buildGenerateBody builds an Ollama native /api/generate request.
func buildGenerateBody(
	req *providers.ChatRequest,
	stream bool,
) ([]byte, error) {
	requestBody := map[string]any{
		"model":  cModel(req),
		"prompt": buildOllamaPrompt(req.Messages),
		"stream": stream,
	}

	// Preserve Ollama generation options from the original request.
	if len(req.Extra) > 0 {
		var extra map[string]any

		if err := json.Unmarshal(req.Extra, &extra); err == nil {
			if options, ok := extra["options"].(map[string]any); ok {
				requestBody["options"] = options
			}
		}
	}

	return json.Marshal(requestBody)
}

// cModel returns the requested model or configured default model.
func cModel(req *providers.ChatRequest) string {
	if req.Model != "" {
		return req.Model
	}

	return ""
}

// generationContext returns a context with the configured generation timeout.
// A zero timeout means no additional deadline.
func (c *Client) generationContext(
	ctx context.Context,
) (context.Context, context.CancelFunc) {
	if c.cfg.OllamaTimeout <= 0 {
		return context.WithCancel(ctx)
	}

	return context.WithTimeout(ctx, c.cfg.OllamaTimeout)
}

// ensureSuccess converts a non-2xx HTTP response into an error.
func ensureSuccess(resp *http.Response) error {
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}

	msg, _ := io.ReadAll(
		io.LimitReader(resp.Body, 4096),
	)

	return fmt.Errorf(
		"ollama generate status %d: %s",
		resp.StatusCode,
		strings.TrimSpace(string(msg)),
	)
}

// openAIUsage is the OpenAI-compatible usage object.
type openAIUsage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

// buildOpenAIChunk builds an OpenAI-compatible chat.completion.chunk payload.
func buildOpenAIChunk(
	id string,
	created int64,
	model string,
	content string,
	role string,
	finishReason string,
	usage *openAIUsage,
) string {
	delta := map[string]any{}

	if role != "" {
		delta["role"] = role
	}

	if content != "" {
		delta["content"] = content
	}

	choice := map[string]any{
		"index": 0,
		"delta": delta,
	}

	if finishReason != "" {
		choice["finish_reason"] = finishReason
	} else {
		choice["finish_reason"] = nil
	}

	chunk := map[string]any{
		"id":      id,
		"object":  "chat.completion.chunk",
		"created": created,
		"model":   model,
		"choices": []any{choice},
	}

	if usage != nil {
		chunk["usage"] = usage
	}

	encoded, err := json.Marshal(chunk)
	if err != nil {
		return "{}"
	}

	return string(encoded)
}

// newCompletionID generates an OpenAI-compatible completion id.
func newCompletionID() string {
	return fmt.Sprintf(
		"chatcmpl-%d",
		time.Now().UnixNano(),
	)
}

// buildOllamaPrompt converts chat messages into an /api/generate prompt.
func buildOllamaPrompt(messages []providers.Message) string {
	var builder strings.Builder

	for _, message := range messages {
		switch strings.ToLower(message.Role) {
		case "system":
			builder.WriteString("System: ")
		case "assistant":
			builder.WriteString("Assistant: ")
		case "user":
			builder.WriteString("User: ")
		default:
			builder.WriteString(message.Role)
			builder.WriteString(": ")
		}

		builder.WriteString(message.Content)
		builder.WriteString("\n")
	}

	builder.WriteString("Assistant:")

	return builder.String()
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
func (c *Client) Embed(
	ctx context.Context,
	model string,
	inputs []string,
) (*EmbeddingsResponse, error) {
	if model == "" {
		model = c.cfg.OllamaEmbedModel
	}

	body, err := json.Marshal(map[string]any{
		"model": model,
		"input": inputs,
	})
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		strings.TrimRight(c.cfg.OllamaURL, "/")+"/api/embed",
		bytes.NewReader(body),
	)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")

	if c.cfg.OllamaAPIKey != "" {
		req.Header.Set(
			"Authorization",
			"Bearer "+c.cfg.OllamaAPIKey,
		)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("ollama embed: %w", err)
	}

	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		msg, _ := io.ReadAll(
			io.LimitReader(resp.Body, 4096),
		)

		return nil, fmt.Errorf(
			"ollama embed status %d: %s",
			resp.StatusCode,
			strings.TrimSpace(string(msg)),
		)
	}

	var raw struct {
		Embeddings [][]float64 `json:"embeddings"`
		Model      string      `json:"model"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return nil, fmt.Errorf(
			"ollama embed decode: %w",
			err,
		)
	}

	out := &EmbeddingsResponse{
		Object: "list",
		Model:  raw.Model,
		Data:   make([]Embedding, 0, len(raw.Embeddings)),
	}

	for i, embedding := range raw.Embeddings {
		out.Data = append(
			out.Data,
			Embedding{
				Object:    "embedding",
				Index:     i,
				Embedding: embedding,
			},
		)
	}

	return out, nil
}
