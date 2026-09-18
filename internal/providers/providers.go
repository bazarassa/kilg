// Package providers defines the common provider interface and the
// OpenAI-compatible client used by the heavy, LiteLLM and Ollama providers.
package providers

import (
	"context"
	"errors"
)

// Event types emitted by providers.
const (
	// EventRawSSE carries the original upstream SSE data payload, verbatim.
	EventRawSSE = "raw_sse"
	// EventDone marks the end of the upstream stream.
	EventDone = "done"
	// EventError marks a provider error.
	EventError = "error"
)

// ProviderEvent is one unit of provider output.
type ProviderEvent struct {
	Type string
	// Raw is the original upstream SSE data payload (JSON line) for
	// EventRawSSE events. It must be preserved byte-for-byte.
	Raw string
	Err error
}

// Provider is the common interface for LLM backends.
type Provider interface {
	// Name returns the provider name (e.g. "heavy", "litellm", "ollama").
	Name() string
	// Model returns the default model id.
	Model() string
	// Complete runs a chat completion, emitting raw SSE events on the
	// channel. The channel is closed when the stream ends or errors.
	Complete(ctx context.Context, req *ChatRequest, events chan<- ProviderEvent) error
}

// ChatRequest is the provider-agnostic chat request.
type ChatRequest struct {
	Model    string
	Messages []Message
	Stream   bool
	// Extra carries the full original request JSON for passthrough.
	Extra []byte
}

// Message is a chat message.
type Message struct {
	Role    string
	Content string
}

// ErrNonRetryable marks errors that must not be retried (4xx, bad request).
var ErrNonRetryable = errors.New("non-retryable provider error")

// IsRetryable reports whether err represents a transient failure.
func IsRetryable(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrNonRetryable) {
		return false
	}
	return true
}
