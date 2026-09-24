package providers

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
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
	BaseURL string
	Model   string
	APIKey  string

	// ConnectTimeout bounds TCP connect + TLS handshake.
	ConnectTimeout time.Duration

	// HeaderTimeout bounds waiting for HTTP response headers.
	HeaderTimeout time.Duration

	// IdleTimeout bounds the maximum gap between upstream SSE events.
	IdleTimeout time.Duration

	// GenerationTimeout bounds the whole generation.
	GenerationTimeout time.Duration
}

// OpenAIClient is a client for OpenAI-compatible endpoints.
//
// Supported backends:
//
//   - llama.cpp
//   - LiteLLM
//   - Ollama /v1
//
// The provider is intentionally transport-agnostic. The only assumption is
// that the endpoint implements:
//
//	POST /chat/completions
//
// with OpenAI-compatible JSON/SSE semantics.
type OpenAIClient struct {
	cfg  OpenAIConfig
	http *http.Client
}

// NewOpenAIClient creates an OpenAI-compatible client.
//
// http.Client.Timeout is intentionally not configured because long-running
// reasoning models may generate for a long time. GenerationTimeout is handled
// through request context instead.
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

	dialer := &net.Dialer{
		Timeout:   cfg.ConnectTimeout,
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

		TLSHandshakeTimeout:   cfg.ConnectTimeout,
		ResponseHeaderTimeout: cfg.HeaderTimeout,

		MaxIdleConns:    16,
		IdleConnTimeout: 90 * time.Second,
	}

	return &OpenAIClient{
		cfg: cfg,
		http: &http.Client{
			Transport: transport,

			// IMPORTANT:
			// Do not set Timeout here.
			//
			// Long reasoning generations can legitimately take many minutes.
			// GenerationTimeout is handled with context.WithTimeout below.
		},
	}
}

// Name implements Provider.
func (c *OpenAIClient) Name() string {
	return "openai"
}

// Model implements Provider.
func (c *OpenAIClient) Model() string {
	return c.cfg.Model
}

// buildBody builds the request sent to the upstream provider.
//
// The original HTTP request JSON is stored in req.Extra.
//
// We use it as the base document so that fields unknown to the gateway are
// not lost:
//
//	temperature
//	top_p
//	max_tokens
//	max_completion_tokens
//	stop
//	response_format
//	stream_options
//	tools
//	tool_choice
//	etc.
//
// Only model and stream are controlled by the gateway.
//
// This is important because providers.ChatRequest intentionally contains
// only provider-agnostic fields:
//
//	Model
//	Messages
//	Stream
//	Extra
func buildBody(req *ChatRequest) ([]byte, error) {
	if req == nil {
		return nil, errors.New("nil chat request")
	}

	// Preferred path: preserve the original HTTP request.
	if len(req.Extra) > 0 {
		var obj map[string]json.RawMessage

		if err := json.Unmarshal(req.Extra, &obj); err == nil {
			model, err := json.Marshal(req.Model)
			if err != nil {
				return nil, fmt.Errorf("marshal model: %w", err)
			}

			stream, err := json.Marshal(req.Stream)
			if err != nil {
				return nil, fmt.Errorf("marshal stream: %w", err)
			}

			obj["model"] = model
			obj["stream"] = stream

			return json.Marshal(obj)
		}
	}

	// Fallback when Extra is unavailable.
	messages := make([]map[string]string, 0, len(req.Messages))

	for _, message := range req.Messages {
		messages = append(messages, map[string]string{
			"role":    message.Role,
			"content": message.Content,
		})
	}

	body := map[string]any{
		"model":    req.Model,
		"messages": messages,
		"stream":   req.Stream,
	}

	return json.Marshal(body)
}

// buildNonStreamingBody builds a request with stream=false.
//
// It is kept separately because CompleteSync is still used by the worker
// for providers such as heavy.
func buildNonStreamingBody(req *ChatRequest) ([]byte, error) {
	if req == nil {
		return nil, errors.New("nil chat request")
	}

	if len(req.Extra) > 0 {
		var obj map[string]json.RawMessage

		if err := json.Unmarshal(req.Extra, &obj); err == nil {
			model, err := json.Marshal(req.Model)
			if err != nil {
				return nil, fmt.Errorf("marshal model: %w", err)
			}

			stream, err := json.Marshal(false)
			if err != nil {
				return nil, fmt.Errorf("marshal stream: %w", err)
			}

			obj["model"] = model
			obj["stream"] = stream

			return json.Marshal(obj)
		}
	}

	messages := make([]map[string]string, 0, len(req.Messages))

	for _, message := range req.Messages {
		messages = append(messages, map[string]string{
			"role":    message.Role,
			"content": message.Content,
		})
	}

	body := map[string]any{
		"model":    req.Model,
		"messages": messages,
		"stream":   false,
	}

	return json.Marshal(body)
}

// CompleteSync executes a non-streaming OpenAI-compatible completion.
//
// This is used by the worker when it wants a complete JSON response.
//
// Flow:
//
//	Kafka request
//	    |
//	    v
//	CompleteSync()
//	    |
//	    v
//	POST /chat/completions
//	    |
//	    v
//	JSON
//	    |
//	    v
//	Worker -> Kafka
func (c *OpenAIClient) CompleteSync(
	ctx context.Context,
	req *ChatRequest,
) (*protocol.ChatResponse, error) {
	body, err := buildNonStreamingBody(req)
	if err != nil {
		return nil, fmt.Errorf(
			"build provider request: %w",
			err,
		)
	}

	genCtx, cancel := context.WithTimeout(
		ctx,
		c.cfg.GenerationTimeout,
	)
	defer cancel()

	httpReq, err := http.NewRequestWithContext(
		genCtx,
		http.MethodPost,
		strings.TrimRight(c.cfg.BaseURL, "/")+"/chat/completions",
		bytes.NewReader(body),
	)
	if err != nil {
		return nil, fmt.Errorf(
			"create provider request: %w",
			err,
		)
	}

	httpReq.Header.Set(
		"Content-Type",
		"application/json",
	)

	httpReq.Header.Set(
		"Accept",
		"application/json",
	)

	if c.cfg.APIKey != "" {
		httpReq.Header.Set(
			"Authorization",
			"Bearer "+c.cfg.APIKey,
		)
	}

	resp, err := c.http.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf(
			"provider request: %w",
			err,
		)
	}

	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, c.providerHTTPError(resp)
	}

	var result protocol.ChatResponse

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf(
			"decode provider response: %w",
			err,
		)
	}

	if len(result.Choices) == 0 {
		return nil, errors.New(
			"provider response contains no choices",
		)
	}

	return &result, nil
}

// Complete executes a completion.
//
// req.Stream controls the upstream transport:
//
//	stream=false
//	    provider returns JSON
//
//	stream=true
//	    provider returns SSE
//
// IMPORTANT:
//
// The Gateway itself exposes SSE to the client. Therefore a non-streaming
// provider response is converted into one or two OpenAI-compatible SSE
// chunks before being emitted as ProviderEvent values.
func (c *OpenAIClient) Complete(
	ctx context.Context,
	req *ChatRequest,
	events chan<- ProviderEvent,
) error {
	defer close(events)

	if req == nil {
		err := errors.New("nil chat request")

		events <- ProviderEvent{
			Type: EventError,
			Err:  err,
		}

		return err
	}

	body, err := buildBody(req)
	if err != nil {
		err = fmt.Errorf(
			"build provider request: %w",
			err,
		)

		events <- ProviderEvent{
			Type: EventError,
			Err:  err,
		}

		return err
	}

	genCtx, cancel := context.WithTimeout(
		ctx,
		c.cfg.GenerationTimeout,
	)
	defer cancel()

	httpReq, err := http.NewRequestWithContext(
		genCtx,
		http.MethodPost,
		strings.TrimRight(c.cfg.BaseURL, "/")+"/chat/completions",
		bytes.NewReader(body),
	)
	if err != nil {
		err = fmt.Errorf(
			"create provider request: %w",
			err,
		)

		events <- ProviderEvent{
			Type: EventError,
			Err:  err,
		}

		return err
	}

	httpReq.Header.Set(
		"Content-Type",
		"application/json",
	)

	if req.Stream {
		httpReq.Header.Set(
			"Accept",
			"text/event-stream",
		)
	} else {
		httpReq.Header.Set(
			"Accept",
			"application/json",
		)
	}

	if c.cfg.APIKey != "" {
		httpReq.Header.Set(
			"Authorization",
			"Bearer "+c.cfg.APIKey,
		)
	}

	resp, err := c.http.Do(httpReq)
	if err != nil {
		err = fmt.Errorf(
			"provider request: %w",
			err,
		)

		events <- ProviderEvent{
			Type: EventError,
			Err:  err,
		}

		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		err = c.providerHTTPError(resp)

		events <- ProviderEvent{
			Type: EventError,
			Err:  err,
		}

		return err
	}

	// ------------------------------------------------------------
	// NON-STREAMING UPSTREAM
	// ------------------------------------------------------------
	//
	// The upstream provider returns one JSON ChatResponse.
	//
	// We convert it into OpenAI-compatible SSE chunks so the rest of
	// the Gateway does not need a second response path.
	//
	if !req.Stream {
		return c.completeNonStreaming(resp, events)
	}

	// ------------------------------------------------------------
	// STREAMING UPSTREAM
	// ------------------------------------------------------------

	return c.completeStreaming(
		genCtx,
		resp,
		events,
	)
}

// completeNonStreaming converts a JSON completion into SSE-compatible
// ProviderEvent values.
func (c *OpenAIClient) completeNonStreaming(
	resp *http.Response,
	events chan<- ProviderEvent,
) error {
	var result protocol.ChatResponse

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		err = fmt.Errorf(
			"decode provider response: %w",
			err,
		)

		events <- ProviderEvent{
			Type: EventError,
			Err:  err,
		}

		return err
	}

	if len(result.Choices) == 0 {
		err := errors.New(
			"provider response contains no choices",
		)

		events <- ProviderEvent{
			Type: EventError,
			Err:  err,
		}

		return err
	}

	choice := result.Choices[0]

	// Extract content from protocol.ChatMessage.
	//
	// ChatMessage.Content is json.RawMessage, therefore it may contain:
	//
	//	"hello"
	//
	// or, depending on the provider:
	//
	//	{"type":"text","text":"hello"}
	//
	// The normal OpenAI-compatible response is a JSON string.
	content := rawMessageToString(choice.Message.Content)

	// Emit the actual assistant content.
	//
	// This is the critical part that ensures that a non-streaming
	// llama.cpp/LiteLLM/Ollama response is not reduced to only
	// finish_reason=stop.
	if content != "" {
		chunk := protocol.ChatChunk{
			ID:      result.ID,
			Object:  "chat.completion.chunk",
			Created: result.Created,
			Model:   result.Model,
			Choices: []protocol.ChunkChoice{
				{
					Index: 0,
					Delta: protocol.ChatMessage{
						Role:    "assistant",
						Content: mustJSONRaw(content),
					},
					FinishReason: nil,
				},
			},
		}

		data, err := json.Marshal(chunk)
		if err != nil {
			err = fmt.Errorf(
				"marshal content chunk: %w",
				err,
			)

			events <- ProviderEvent{
				Type: EventError,
				Err:  err,
			}

			return err
		}

		events <- ProviderEvent{
			Type: EventRawSSE,
			Raw:  string(data),
		}
	}

	// Emit final chunk.
	finishReason := "stop"

	if choice.FinishReason != nil &&
		*choice.FinishReason != "" {
		finishReason = *choice.FinishReason
	}

	finishChunk := protocol.ChatChunk{
		ID:      result.ID,
		Object:  "chat.completion.chunk",
		Created: result.Created,
		Model:   result.Model,
		Choices: []protocol.ChunkChoice{
			{
				Index: 0,
				Delta: protocol.ChatMessage{},
				FinishReason: &finishReason,
			},
		},
		Usage: result.Usage,
	}

	finishData, err := json.Marshal(finishChunk)
	if err != nil {
		err = fmt.Errorf(
			"marshal finish chunk: %w",
			err,
		)

		events <- ProviderEvent{
			Type: EventError,
			Err:  err,
		}

		return err
	}

	events <- ProviderEvent{
		Type: EventRawSSE,
		Raw:  string(finishData),
	}

	events <- ProviderEvent{
		Type: EventDone,
	}

	return nil
}

// completeStreaming reads upstream SSE and forwards each data payload
// without modifying it.
func (c *OpenAIClient) completeStreaming(
	genCtx context.Context,
	resp *http.Response,
	events chan<- ProviderEvent,
) error {
	// Idle timeout is different from GenerationTimeout.
	//
	// GenerationTimeout:
	//
	//	whole request may not exceed N
	//	seconds/hours.
	//
	// IdleTimeout:
	//
	//	there may not be a gap of more than N
	//	between two upstream events.
	idleCtx, idleCancel := context.WithCancel(genCtx)
	defer idleCancel()

	idleTimer := time.NewTimer(c.cfg.IdleTimeout)
	defer idleTimer.Stop()

	go func() {
		select {
		case <-idleTimer.C:
			idleCancel()

		case <-genCtx.Done():
		}
	}()

	reader := bufio.NewReaderSize(
		resp.Body,
		64*1024,
	)

	finished := false

	for {
		data, err := readSSEData(reader)

		if data != "" {
			idleReset(
				idleTimer,
				c.cfg.IdleTimeout,
			)

			if data == "[DONE]" {
				finished = true
				break
			}

			events <- ProviderEvent{
				Type: EventRawSSE,
				Raw:  data,
			}
		}

		if err != nil {
			if idleCtx.Err() != nil &&
				genCtx.Err() == nil {

				err = fmt.Errorf(
					"idle timeout after %s",
					c.cfg.IdleTimeout,
				)

				events <- ProviderEvent{
					Type: EventError,
					Err:  err,
				}

				return err
			}

			if genCtx.Err() != nil {
				err = fmt.Errorf(
					"generation timeout after %s",
					c.cfg.GenerationTimeout,
				)

				events <- ProviderEvent{
					Type: EventError,
					Err:  err,
				}

				return err
			}

			if err != io.EOF {
				err = fmt.Errorf(
					"read stream: %w",
					err,
				)

				events <- ProviderEvent{
					Type: EventError,
					Err:  err,
				}

				return err
			}

			break
		}
	}

	if finished {
		events <- ProviderEvent{
			Type: EventDone,
		}
	}

	return nil
}

// providerHTTPError converts an upstream HTTP error into a useful error.
//
// 4xx errors are marked non-retryable because they usually indicate:
//
//	invalid request
//	invalid model
//	authentication failure
//	unsupported parameter
func (c *OpenAIClient) providerHTTPError(
	resp *http.Response,
) error {
	msg, _ := io.ReadAll(
		io.LimitReader(resp.Body, 4096),
	)

	err := fmt.Errorf(
		"provider status %d: %s",
		resp.StatusCode,
		strings.TrimSpace(string(msg)),
	)

	if resp.StatusCode >= 400 &&
		resp.StatusCode < 500 {
		return fmt.Errorf(
			"%w: %v",
			ErrNonRetryable,
			err,
		)
	}

	return err
}

// idleReset safely resets an idle timer.
func idleReset(
	timer *time.Timer,
	duration time.Duration,
) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}

	timer.Reset(duration)
}

// readSSEData reads one SSE event and returns its data payload.
//
// Example:
//
//	id: 123
//	data: {"choices":[...]}
//	data: {"more":true}
//
//	<empty line>
//
// returns:
//
//	{"choices":[...]}
//	{"more":true}
func readSSEData(
	reader *bufio.Reader,
) (string, error) {
	var datas []string

	for {
		line, err := reader.ReadString('\n')

		if len(line) > 0 {
			line = strings.TrimRight(
				line,
				"\r\n",
			)

			if line == "" {
				if len(datas) > 0 {
					return strings.Join(
						datas,
						"\n",
					), err
				}

				if err != nil {
					return "", err
				}

				continue
			}

			if strings.HasPrefix(line, "data:") {
				data := strings.TrimPrefix(
					line,
					"data:",
				)

				data = strings.TrimPrefix(
					data,
					" ",
				)

				datas = append(
					datas,
					data,
				)
			}

			// id:
			// event:
			// retry:
			// comments:
			//
			// are intentionally ignored.
		}

		if err != nil {
			if len(datas) > 0 {
				return strings.Join(
					datas,
					"\n",
				), err
			}

			return "", err
		}
	}
}

// rawMessageToString converts json.RawMessage containing a JSON string
// into a normal Go string.
//
// If the value is not a JSON string, the raw JSON is returned as-is.
//
// This makes the client tolerant of providers returning either:
//
//	"hello"
//
// or:
//
//	{"type":"text","text":"hello"}
func rawMessageToString(
	raw json.RawMessage,
) string {
	if len(raw) == 0 {
		return ""
	}

	var value string

	if err := json.Unmarshal(
		raw,
		&value,
	); err == nil {
		return value
	}

	// Some providers may already give us a plain JSON-compatible
	// object/array. Preserve it rather than silently dropping it.
	return string(raw)
}

// mustJSONRaw converts a string to json.RawMessage.
//
// The function is intentionally small and only fails if json.Marshal
// somehow fails for a Go string, which cannot happen in practice.
func mustJSONRaw(value string) json.RawMessage {
	data, err := json.Marshal(value)
	if err != nil {
		return json.RawMessage(`""`)
	}

	return json.RawMessage(data)
}
