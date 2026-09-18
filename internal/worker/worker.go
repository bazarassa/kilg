// Package worker implements the heavy LLM worker: it consumes jobs from
// llm.requests, runs the heavy provider, and publishes every chunk to
// llm.events as a durable raw SSE event.
package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/kafka-llm-gateway/gateway/internal/kafka"
	"github.com/kafka-llm-gateway/gateway/internal/observability"
	"github.com/kafka-llm-gateway/gateway/internal/protocol"
	"github.com/kafka-llm-gateway/gateway/internal/providers"
)

// Worker runs heavy generations.
type Worker struct {
	producer  kafka.EventProducer
	provider  providers.Provider
	topics    kafka.Topics
	metrics   *observability.Metrics
	log       *slog.Logger
	workers   int
	maxRetry  int
	heartbeat time.Duration
}

// New builds a worker.
func New(producer kafka.EventProducer, provider providers.Provider, topics kafka.Topics,
	metrics *observability.Metrics, log *slog.Logger, workers, maxRetry int, heartbeat time.Duration) *Worker {
	if workers <= 0 {
		workers = 2
	}
	if maxRetry <= 0 {
		maxRetry = 3
	}
	if heartbeat <= 0 {
		heartbeat = 30 * time.Second
	}
	return &Worker{
		producer:  producer,
		provider:  provider,
		topics:    topics,
		metrics:   metrics,
		log:       log,
		workers:   workers,
		maxRetry:  maxRetry,
		heartbeat: heartbeat,
	}
}

// Run consumes llm.requests until ctx is done.
func (w *Worker) Run(ctx context.Context, consumer kafka.EventConsumer) error {
	sem := make(chan struct{}, w.workers)
	var wg sync.WaitGroup

	err := consumer.Consume(ctx, func(rec kafka.Record) error {
		var req protocol.Request
		if err := json.Unmarshal(rec.Value, &req); err != nil {
			w.log.Error("invalid request record", "err", err)
			return nil
		}
		w.log.Info("job received", "request_id", req.RequestID, "provider", req.Provider, "model", req.Model, "attempt", req.Attempt)

		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			w.process(ctx, req)
		}()
		return nil
	})
	// Wait for in-flight generations to finish (graceful shutdown).
	wg.Wait()
	return err
}

// process runs one job with retries.
func (w *Worker) process(ctx context.Context, req protocol.Request) {
	attempt := req.Attempt
	lastSeq := uint64(0)
	for {
		attempt++
		var err error
		lastSeq, err = w.runOnce(ctx, req, attempt, lastSeq)
		if err == nil || ctx.Err() != nil || !providers.IsRetryable(err) || attempt >= w.maxRetry {
			if err != nil && ctx.Err() == nil {
				w.publishFailed(ctx, req, attempt, err, lastSeq)
				// Only route to DLQ when a retryable error exhausts retries.
				if providers.IsRetryable(err) && attempt >= w.maxRetry {
					w.publishDLQ(ctx, req, attempt, err)
				}
			}
			return
		}
		w.log.Warn("retrying job", "request_id", req.RequestID, "attempt", attempt, "err", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Duration(attempt) * 5 * time.Second):
		}
	}
}

// runOnce executes one generation attempt. It returns the last sequence
// published so a subsequent failure event continues the monotonic sequence.
func (w *Worker) runOnce(ctx context.Context, req protocol.Request, attempt int, startSeq uint64) (uint64, error) {
	start := time.Now()
	w.metrics.RequestsActive.Inc()
	defer w.metrics.RequestsActive.Dec()

	var chatReq protocol.ChatRequest
	if err := json.Unmarshal(req.Payload, &chatReq); err != nil {
		return startSeq, fmt.Errorf("decode payload: %w", err)
	}
	pReq := &providers.ChatRequest{
		Model:  req.Model,
		Stream: true,
		Extra:  req.Payload,
	}
	for _, m := range chatReq.Messages {
		content := string(m.Content)
		var s string
		if json.Unmarshal(m.Content, &s) == nil {
			content = s
		}
		pReq.Messages = append(pReq.Messages, providers.Message{Role: m.Role, Content: content})
	}

	events := make(chan providers.ProviderEvent, 64)
	// Run the provider in a goroutine so events are published as they arrive
	// (not buffered until the stream ends). runDone carries the final error
	// and is only read after the event channel is closed, avoiding a race.
	runDone := make(chan error, 1)
	go func() {
		runDone <- w.provider.Complete(ctx, pReq, events)
	}()

	seq := startSeq
	publish := func(typ string, payload any) error {
		seq++
		ev, err := protocol.NewEvent(req.RequestID, seq, typ, payload)
		if err != nil {
			return err
		}
		ev.Provider = req.Provider
		ev.Model = req.Model
		b, err := json.Marshal(ev)
		if err != nil {
			return err
		}
		return w.publish(ctx, w.topics.Events, req.RequestID, b)
	}

	if err := publish(protocol.TypeStarted, protocol.StartedPayload{Provider: req.Provider, Model: req.Model}); err != nil {
		return seq, fmt.Errorf("publish started: %w", err)
	}

	var content, reasoning string
	reasoningAvailable := false
	var finishReason string
	var usage json.RawMessage
	var lastSeq uint64
	heartbeat := time.NewTicker(w.heartbeat)
	defer heartbeat.Stop()

	var streamErr error
eventLoop:
	for {
		select {
		case ev, ok := <-events:
			if !ok {
				break eventLoop
			}
			switch ev.Type {
			case providers.EventRawSSE:
				info := providers.ParseChunk(ev.Raw)
				if info.Content != "" {
					content += info.Content
				}
				if info.Reasoning != "" {
					reasoning += info.Reasoning
					reasoningAvailable = true
				}
				if info.FinishReason != "" {
					finishReason = info.FinishReason
				}
				if info.Usage != nil {
					usage = info.Usage
				}
				// Preserve the original upstream SSE event verbatim.
				if err := publish(protocol.TypeRawSSE, protocol.RawSSEPayload{Data: ev.Raw}); err != nil {
					return seq, fmt.Errorf("publish raw event: %w", err)
				}
				lastSeq = seq
			case providers.EventDone:
				break eventLoop
			case providers.EventError:
				streamErr = ev.Err
				break eventLoop
			}
		case <-heartbeat.C:
			_ = publish(protocol.TypeHeartbeat, protocol.HeartbeatPayload{LastSequence: lastSeq})
		case <-ctx.Done():
			return seq, ctx.Err()
		}
	}
	// The event channel is closed, so the provider has returned.
	runErr := <-runDone
	_ = runErr

	if streamErr != nil {
		return seq, streamErr
	}

	// The completed event is published only after the last content event is
	// durably stored (see PLAN section 31).
	if err := publish(protocol.TypeCompleted, protocol.CompletedPayload{
		FinishReason:       finishReason,
		Usage:              usage,
		Content:            content,
		Reasoning:          reasoning,
		ReasoningAvailable: reasoningAvailable,
		Events:             seq,
		DurationMS:         time.Since(start).Milliseconds(),
	}); err != nil {
		return seq, fmt.Errorf("publish completed: %w", err)
	}

	comp := protocol.Completion{
		RequestID:   req.RequestID,
		Status:      protocol.StatusCompleted,
		Provider:    req.Provider,
		Model:       req.Model,
		Reasoning:   reasoning,
		Content:     content,
		Usage:       usage,
		Events:      seq,
		StartedAt:   start,
		CompletedAt: time.Now().UTC(),
	}
	b, err := json.Marshal(comp)
	if err != nil {
		return seq, err
	}
	if err := w.publish(ctx, w.topics.Completed, req.RequestID, b); err != nil {
		return seq, fmt.Errorf("publish completion: %w", err)
	}

	w.metrics.GenerationDur.WithLabelValues(req.Provider).Observe(time.Since(start).Seconds())
	w.metrics.RequestsTotal.WithLabelValues(req.Provider, "completed").Inc()
	w.log.Info("job completed",
		"request_id", req.RequestID, "provider", req.Provider, "model", req.Model,
		"duration", time.Since(start).String(), "events", seq,
		"reasoning_available", reasoningAvailable)
	return seq, nil
}

func (w *Worker) publishFailed(ctx context.Context, req protocol.Request, attempt int, err error, lastSeq uint64) {
	w.metrics.RequestsFailed.WithLabelValues(req.Provider).Inc()
	w.metrics.RequestsTotal.WithLabelValues(req.Provider, "failed").Inc()

	ev, _ := protocol.NewEvent(req.RequestID, lastSeq+1, protocol.TypeFailed, protocol.FailedPayload{
		Error:     err.Error(),
		Retryable: providers.IsRetryable(err),
		Attempt:   attempt,
	})
	ev.Provider = req.Provider
	ev.Model = req.Model
	if b, merr := json.Marshal(ev); merr == nil {
		_ = w.publish(ctx, w.topics.Events, req.RequestID, b)
	}

	f := protocol.Failure{
		RequestID: req.RequestID,
		Provider:  req.Provider,
		Model:     req.Model,
		Error:     err.Error(),
		Retryable: providers.IsRetryable(err),
		Attempt:   attempt,
		Timestamp: time.Now().UTC(),
	}
	if b, merr := json.Marshal(f); merr == nil {
		_ = w.publish(ctx, w.topics.Failed, req.RequestID, b)
	}
	w.log.Error("job failed", "request_id", req.RequestID, "attempt", attempt, "err", err)
}

func (w *Worker) publishDLQ(ctx context.Context, req protocol.Request, attempt int, err error) {
	rec := protocol.DLQRecord{
		RequestID: req.RequestID,
		Attempt:   attempt,
		Error:     err.Error(),
		Timestamp: time.Now().UTC(),
		Original:  req.Payload,
	}
	if b, merr := json.Marshal(rec); merr == nil {
		if perr := w.publish(ctx, w.topics.DLQ, req.RequestID, b); perr != nil {
			w.log.Error("dlq publish failed", "request_id", req.RequestID, "err", perr)
		}
	}
}

func (w *Worker) publish(ctx context.Context, topic, key string, value []byte) error {
	if w.metrics != nil {
		if err := w.producer.Publish(ctx, topic, key, value); err != nil {
			w.metrics.KafkaPublish.WithLabelValues(topic, "error").Inc()
			w.metrics.KafkaPublishErr.WithLabelValues(topic).Inc()
			return err
		}
		w.metrics.KafkaPublish.WithLabelValues(topic, "ok").Inc()
		return nil
	}
	return w.producer.Publish(ctx, topic, key, value)
}
