// Package protocol defines the durable event protocol shared by the
// gateway, the heavy worker and any future consumers of the Kafka event log.
package protocol

import (
	"encoding/json"
	"fmt"
	"time"
)

// Event types carried on the llm.events topic.
const (
	TypeQueued      = "queued"
	TypeStarted     = "started"
	TypeReasoning   = "reasoning"
	TypeContent     = "content"
	TypeToolCall    = "tool_call"
	TypeUsage       = "usage"
	TypeHeartbeat   = "heartbeat"
	TypeCompleted   = "completed"
	TypeFailed      = "failed"
	TypeCancelled   = "cancelled"
	TypeRawSSE      = "raw_sse"
	TypeUnknown     = "unknown"
)

// Job statuses.
const (
	StatusQueued      = "queued"
	StatusRunning     = "running"
	StatusCompleted   = "completed"
	StatusFailed      = "failed"
	StatusCancelled   = "cancelled"
)

// Event is the envelope for every record published to llm.events.
//
// The payload is kept as raw JSON so provider-specific fields (including
// reasoning_content) are never lost by round-tripping through Go structs.
type Event struct {
	EventID   string          `json:"event_id"`
	RequestID string          `json:"request_id"`
	Sequence  uint64          `json:"sequence"`
	Type      string          `json:"type"`
	Provider  string          `json:"provider,omitempty"`
	Model     string          `json:"model,omitempty"`
	Timestamp time.Time       `json:"timestamp"`
	Payload   json.RawMessage `json:"payload"`
}

// RawSSEPayload carries the original upstream SSE data line, verbatim.
type RawSSEPayload struct {
	Data string `json:"data"`
}

// StartedPayload is emitted when the worker begins generation.
type StartedPayload struct {
	Provider string `json:"provider"`
	Model    string `json:"model"`
}

// CompletedPayload is emitted after the final content event is durably stored.
type CompletedPayload struct {
	FinishReason string          `json:"finish_reason,omitempty"`
	Usage        json.RawMessage `json:"usage,omitempty"`
	Content      string          `json:"content,omitempty"`
	Reasoning    string          `json:"reasoning,omitempty"`
	ReasoningAvailable bool       `json:"reasoning_available"`
	Events       uint64          `json:"events"`
	DurationMS   int64           `json:"duration_ms"`
}

// FailedPayload is emitted when a job cannot be completed.
type FailedPayload struct {
	Error   string `json:"error"`
	Retryable bool `json:"retryable"`
	Attempt int    `json:"attempt,omitempty"`
}

// HeartbeatPayload is emitted periodically for long generations.
type HeartbeatPayload struct {
	LastSequence uint64 `json:"last_sequence"`
}

// Request is the job record published to llm.requests.
type Request struct {
	RequestID  string          `json:"request_id"`
	Provider   string          `json:"provider"`
	Model      string          `json:"model"`
	CreatedAt  time.Time       `json:"created_at"`
	Attempt    int             `json:"attempt"`
	IdempotencyKey string      `json:"idempotency_key,omitempty"`
	Payload    json.RawMessage `json:"payload"`
}

// Completion is the aggregate result published to llm.completed.
type Completion struct {
	RequestID  string          `json:"request_id"`
	Status     string          `json:"status"`
	Provider   string          `json:"provider"`
	Model      string          `json:"model"`
	Reasoning  string          `json:"reasoning,omitempty"`
	Content    string          `json:"content"`
	Usage      json.RawMessage `json:"usage,omitempty"`
	Events     uint64          `json:"events"`
	StartedAt  time.Time       `json:"started_at,omitempty"`
	CompletedAt time.Time      `json:"completed_at"`
}

// Failure is the record published to llm.failed.
type Failure struct {
	RequestID string    `json:"request_id"`
	Provider  string    `json:"provider"`
	Model     string    `json:"model"`
	Error     string    `json:"error"`
	Retryable bool      `json:"retryable"`
	Attempt   int       `json:"attempt"`
	Timestamp time.Time `json:"timestamp"`
}

// DLQRecord is the record published to llm.dlq after retries are exhausted.
type DLQRecord struct {
	RequestID string          `json:"request_id"`
	Attempt   int             `json:"attempt"`
	Error     string          `json:"error"`
	Timestamp time.Time       `json:"timestamp"`
	Original  json.RawMessage `json:"original"`
}

// Marshal serializes v into a JSON raw message.
func Marshal(v any) (json.RawMessage, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("marshal %T: %w", v, err)
	}
	return b, nil
}

// NewEvent builds an event envelope with a stable event id.
func NewEvent(requestID string, sequence uint64, typ string, payload any) (Event, error) {
	raw, err := Marshal(payload)
	if err != nil {
		return Event{}, err
	}
	return Event{
		EventID:   fmt.Sprintf("%s-%010d", requestID, sequence),
		RequestID: requestID,
		Sequence:  sequence,
		Type:      typ,
		Timestamp: time.Now().UTC(),
		Payload:   raw,
	}, nil
}
