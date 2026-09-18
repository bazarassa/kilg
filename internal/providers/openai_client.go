package providers

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

	"github.com/kafka-llm-gateway/gateway/internal/protocol"
)

// OpenAIConfig configures an OpenAI-compatible provider.
type OpenAIConfig struct {
	BaseURL   string
	Model     string
	APIKey    string
	// ConnectTimeout bounds TCP connect + TLS handshake.
	ConnectTimeout time.Duration
	// HeaderTimeout bounds waiting for the response headers.
	HeaderTimeout time.Duration
	// IdleTimeout bounds the gap between two upstream chunks.
	IdleTimeout time.Duration
	// GenerationTimeout bounds the whole generation.
	GenerationTimeout time.Duration
}

// OpenAIClient is a streaming client for OpenAI-compatible endpoints
// (llama.cpp, LiteLLM, Ollama's OpenAI route).
type OpenAIClient struct {
	cfg OpenAIConfig
	http *http.Client
}

// NewOpenAIClient builds a client with separate connect/header/idle/overall
// timeouts. The http.Client.Timeout is intentionally left zero: the overall
// generation may run for hours.
func NewOpenAIClient(cfg OpenAIConfig) *OpenAIClient {
	if cfg.ConnectTimeout <= 0 {
		cfg.ConnectTimeout = 10 * time.Second
	}
	if cfg.HeaderTimeout <= 0 {
		cfg.HeaderTimeout = 60 * time.Second
	}
	if cfg.IdleTimeout <= 0 {
		cfg.IdleTimeout = 10 * time.Minute
	}
	if cfg.GenerationTimeout <= 0 {
		cfg.GenerationTimeout = 2 * time.Hour
	}
	dialer := &net.Dialer{Timeout: cfg.ConnectTimeout, KeepAlive: 30 * time.Second}
	transport := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return dialer.DialContext(ctx, network, addr)
		},
		TLSClientConfig:       &tls.Config{MinVersion: tls.VersionTLS12},
		TLSHandshakeTimeout:   cfg.ConnectTimeout,
		ResponseHeaderTimeout: cfg.HeaderTimeout,
		MaxIdleConns:          16,
		IdleConnTimeout:       90 * time.Second,
	}
	return &OpenAIClient{
		cfg:  cfg,
		http: &http.Client{Transport: transport},
	}
}

// Name implements Provider.
func (c *OpenAIClient) Name() string { return "openai" }

// Model implements Provider.
func (c *OpenAIClient) Model() string { return c.cfg.Model }

// buildBody constructs the upstream request body. If req.Extra holds the
// original request JSON it is used as the base (preserving unknown fields);
// otherwise the body is built from structured fields.
func buildBody(req *ChatRequest) ([]byte, error) {
	if len(req.Extra) > 0 {
		var obj map[string]json.RawMessage
		if err := json.Unmarshal(req.Extra, &obj); err == nil {
			model, _ := json.Marshal(req.Model)
			obj["model"] = model
			stream, _ := json.Marshal(true)
			obj["stream"] = stream
			return json.Marshal(obj)
		}
	}
	body := map[string]any{
		"model":    req.Model,
		"messages": make([]map[string]string, 0, len(req.Messages)),
		"stream":   true,
	}
	for _, m := range req.Messages {
		body["messages"] = append(body["messages"].([]map[string]string), map[string]string{
			"role":    m.Role,
			"content": m.Content,
		})
	}
	return json.Marshal(body)
}

// Complete streams a chat completion, emitting raw SSE events.
func (c *OpenAIClient) Complete(ctx context.Context, req *ChatRequest, events chan<- ProviderEvent) error {
	body, err := buildBody(req)
	if err != nil {
		return err
	}

	genCtx, cancel := context.WithTimeout(ctx, c.cfg.GenerationTimeout)
	defer cancel()

	httpReq, err := http.NewRequestWithContext(genCtx, http.MethodPost,
		strings.TrimRight(c.cfg.BaseURL, "/")+"/chat/completions", bytes.NewReader(body))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "text/event-stream")
	if c.cfg.APIKey != "" {
		httpReq.Header.Set("Authorization", "Bearer "+c.cfg.APIKey)
	}

	resp, err := c.http.Do(httpReq)
	if err != nil {
		return fmt.Errorf("provider request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		err := fmt.Errorf("provider status %d: %s", resp.StatusCode, strings.TrimSpace(string(msg)))
		if resp.StatusCode >= 400 && resp.StatusCode < 500 {
			err = fmt.Errorf("%w: %v", ErrNonRetryable, err)
		}
		events <- ProviderEvent{Type: EventError, Err: err}
		close(events)
		return err
	}

	// Idle timeout: reset on every chunk received.
	idleCtx, idleCancel := context.WithCancel(ctx)
	defer idleCancel()
	idleTimer := time.NewTimer(c.cfg.IdleTimeout)
	defer idleTimer.Stop()
	go func() {
		<-idleTimer.C
		idleCancel()
	}()

	reader := bufio.NewReaderSize(resp.Body, 64*1024)
	finished := false
	for {
		data, err := readSSEData(reader)
		if data != "" {
			idleReset(idleTimer, c.cfg.IdleTimeout)
			if data == "[DONE]" {
				finished = true
				break
			}
			events <- ProviderEvent{Type: EventRawSSE, Raw: data}
		}
		if err != nil {
			if idleCtx.Err() != nil {
				events <- ProviderEvent{Type: EventError, Err: fmt.Errorf("idle timeout after %s", c.cfg.IdleTimeout)}
			} else if genCtx.Err() != nil {
				events <- ProviderEvent{Type: EventError, Err: fmt.Errorf("generation timeout after %s", c.cfg.GenerationTimeout)}
			} else if err != io.EOF {
				events <- ProviderEvent{Type: EventError, Err: fmt.Errorf("read stream: %w", err)}
			}
			break
		}
	}
	if finished {
		events <- ProviderEvent{Type: EventDone}
	}
	close(events)
	return nil
}

func idleReset(t *time.Timer, d time.Duration) {
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
	t.Reset(d)
}

// readSSEData reads one SSE event and returns its data payload.
func readSSEData(r *bufio.Reader) (string, error) {
	var datas []string
	for {
		line, err := r.ReadString('\n')
		if len(line) > 0 {
			line = strings.TrimRight(line, "\r\n")
			if line == "" {
				if len(datas) > 0 {
					return strings.Join(datas, "\n"), err
				}
				if err != nil {
					return "", err
				}
				continue
			}
			if strings.HasPrefix(line, "data:") {
				d := strings.TrimPrefix(line, "data:")
				d = strings.TrimPrefix(d, " ")
				datas = append(datas, d)
			}
		}
		if err != nil {
			if len(datas) > 0 {
				return strings.Join(datas, "\n"), err
			}
			return "", err
		}
	}
}

// ParseChunk extracts finish_reason, content and reasoning deltas from a raw
// upstream SSE data payload. Used for aggregation; the raw payload itself is
// what gets stored.
type ChunkInfo struct {
	FinishReason string
	Content      string
	Reasoning    string
	Usage        json.RawMessage
}

// ParseChunk parses a raw OpenAI-compatible SSE data payload.
func ParseChunk(raw string) ChunkInfo {
	var obj struct {
		Choices []struct {
			Delta struct {
				Content      string          `json:"content"`
				Reasoning    string          `json:"reasoning_content"`
				ReasoningTxt string          `json:"reasoning"`
			} `json:"delta"`
			Message struct {
				Content      string `json:"content"`
				Reasoning    string `json:"reasoning_content"`
			} `json:"message"`
			FinishReason *string `json:"finish_reason"`
		} `json:"choices"`
		Usage *protocol.Usage `json:"usage"`
	}
	if err := json.Unmarshal([]byte(raw), &obj); err != nil {
		return ChunkInfo{}
	}
	var info ChunkInfo
	if len(obj.Choices) > 0 {
		c := obj.Choices[0]
		if c.FinishReason != nil {
			info.FinishReason = *c.FinishReason
		}
		info.Content = c.Delta.Content
		info.Reasoning = c.Delta.Reasoning
		if info.Reasoning == "" {
			info.Reasoning = c.Delta.ReasoningTxt
		}
		if info.Content == "" {
			info.Content = c.Message.Content
		}
		if info.Reasoning == "" {
			info.Reasoning = c.Message.Reasoning
		}
	}
	if obj.Usage != nil {
		info.Usage, _ = json.Marshal(obj.Usage)
	}
	return info
}
