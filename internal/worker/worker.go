// Package worker implements the heavy LLM worker.
//
// Текущий MVP намеренно простой:
//
//     llm.requests
//          |
//          v
//     Kafka consumer
//          |
//          v
//     HTTP POST /chat/completions
//          |
//          | stream=false
//          v
//     JSON response
//          |
//          +----> llm.completed
//          |
//          +----> llm.events (terminal event для Gateway)
//          |
//          v
//     MarkMessage()
//
// В этой версии НЕТ:
//
//   - streaming;
//   - reasoning aggregation;
//   - raw SSE;
//   - heartbeat events;
//   - tool calls;
//   - сложной event choreography.
//
// Всё это можно вернуть следующим этапом.
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/kafka-llm-gateway/gateway/internal/kafka"
	"github.com/kafka-llm-gateway/gateway/internal/observability"
	"github.com/kafka-llm-gateway/gateway/internal/protocol"
	"github.com/kafka-llm-gateway/gateway/internal/providers"
)

// SyncProvider is the minimal interface required by this worker.
//
// Worker не должен знать, как именно устроен HTTP client.
//
// Ему достаточно:
//
//     CompleteSync(ctx, request) -> response
//
// Сейчас реализация — heavy.Client.
type SyncProvider interface {
	CompleteSync(
		context.Context,
		*providers.ChatRequest,
	) (*protocol.ChatResponse, error)
}

// Worker processes durable Kafka requests.
type Worker struct {
	producer kafka.EventProducer
	provider SyncProvider
	topics   kafka.Topics

	metrics *observability.Metrics
	log     *slog.Logger

	// Максимальное количество одновременных LLM generation.
	workers int

	// Максимальное количество попыток одной Kafka request.
	maxRetry int
}

// New builds a worker.
func New(
	producer kafka.EventProducer,
	provider SyncProvider,
	topics kafka.Topics,
	metrics *observability.Metrics,
	log *slog.Logger,
	workers int,
	maxRetry int,
	_ time.Duration, // heartbeat больше не нужен в non-streaming MVP
) *Worker {
	if workers <= 0 {
		workers = 2
	}

	if maxRetry <= 0 {
		maxRetry = 3
	}

	return &Worker{
		producer: producer,
		provider: provider,
		topics:   topics,

		metrics: metrics,
		log:     log,

		workers:  workers,
		maxRetry: maxRetry,
	}
}

// Run consumes llm.requests.
//
// Важнейшее отличие от старой реализации:
//
//     handler НЕ запускает process() в detached goroutine.
//
// Он ждёт завершения process().
//
// Поэтому consumer не делает MarkMessage до завершения обработки.
//
// При этом semaphore всё равно позволяет обрабатывать несколько
// partition параллельно:
//
//     partition 0 -> worker slot 1
//     partition 1 -> worker slot 2
//     partition 2 -> ждёт свободный slot
//
// Для текущих 3 Kafka partitions и HEAVY_WORKERS=2 это именно
// ожидаемая модель.
func (w *Worker) Run(
	ctx context.Context,
	consumer kafka.EventConsumer,
) error {
	sem := make(chan struct{}, w.workers)

	return consumer.Consume(
		ctx,
		func(sessionCtx context.Context, rec kafka.Record) error {
			var req protocol.Request

			if err := json.Unmarshal(
				rec.Value,
				&req,
			); err != nil {
				// Невалидный Kafka message — это poison message.
				//
				// Нельзя просто вернуть ошибку бесконечно:
				// иначе consumer будет постоянно получать один и тот
				// же битый message.
				//
				// Поэтому для malformed request пытаемся записать
				// failure/DLQ и после этого позволяем MarkMessage.
				w.log.Error(
					"invalid Kafka request",
					"topic", rec.Topic,
					"partition", rec.Partition,
					"offset", rec.Offset,
					"err", err,
				)

				return w.handleInvalidRequest(
					sessionCtx,
					rec,
					err,
				)
			}

			if req.RequestID == "" {
				return w.handleInvalidRequest(
					sessionCtx,
					rec,
					errors.New("request_id is empty"),
				)
			}

			w.log.Info(
				"job received",
				"request_id", req.RequestID,
				"provider", req.Provider,
				"model", req.Model,
				"attempt", req.Attempt,
				"partition", rec.Partition,
				"offset", rec.Offset,
			)

			// Получаем worker slot.
			//
			// Используем select, а не простой:
			//
			//     sem <- struct{}{}
			//
			// чтобы rebalance/shutdown мог отменить ожидание.
			select {
			case sem <- struct{}{}:

			case <-sessionCtx.Done():
				return sessionCtx.Err()
			}

			defer func() {
				<-sem
			}()

			// КРИТИЧЕСКИ ВАЖНО:
			//
			// process() выполняется синхронно.
			//
			// Только после его успешного завершения consumer сможет
			// сделать MarkMessage().
			err := w.process(
				sessionCtx,
				req,
			)

			if err != nil {
				// Если session была отменена из-за rebalance/shutdown,
				// message НЕ помечаем.
				//
				// Новый owner partition получит его снова.
				if sessionCtx.Err() != nil {
					return sessionCtx.Err()
				}

				return err
			}

			return nil
		},
	)
}

// process executes one request with retries.
//
// Семантика:
//
// provider success
//     -> completed
//     -> nil
//
// provider failure
//     -> retry if retryable
//     -> failed + optional DLQ after maxRetry
//     -> nil, если terminal failure успешно опубликован
//
// Ошибка публикации terminal result
//     -> error
//     -> offset НЕ MarkMessage
//     -> Kafka retry.
func (w *Worker) process(
	ctx context.Context,
	req protocol.Request,
) error {
	start := time.Now()

	attempt := req.Attempt

	for {
		attempt++

		//response, err := w.runOnce(
                _, err := w.runOnce(
			ctx,
			req,
			attempt,
		)

		if err == nil {
			w.metricsCompleted(req.Provider, start)

			w.log.Info(
				"job completed",
				"request_id", req.RequestID,
				"provider", req.Provider,
				"model", req.Model,
				"attempt", attempt,
				"duration", time.Since(start).String(),
			)

			return nil
		}

		// Kafka session cancellation.
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// Ошибка, которую нельзя retry.
		if !providers.IsRetryable(err) {
			w.log.Error(
				"non-retryable job failure",
				"request_id", req.RequestID,
				"attempt", attempt,
				"err", err,
			)

			return w.publishFailed(
				ctx,
				req,
				attempt,
				err,
			)
		}

		// Retry exhausted.
		if attempt >= w.maxRetry {
			w.log.Error(
				"job retries exhausted",
				"request_id", req.RequestID,
				"attempt", attempt,
				"err", err,
			)

			return w.publishFailed(
				ctx,
				req,
				attempt,
				err,
			)
		}

		w.log.Warn(
			"retrying job",
			"request_id", req.RequestID,
			"attempt", attempt,
			"max_retry", w.maxRetry,
			"err", err,
		)

		// Простой exponential-ish backoff для MVP.
		backoff := time.Duration(attempt) * 2 * time.Second

		timer := time.NewTimer(backoff)

		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()

		case <-timer.C:
		}
	}
}

// runOnce performs one non-streaming LLM request.
func (w *Worker) runOnce(
	ctx context.Context,
	req protocol.Request,
	attempt int,
) (*protocol.ChatResponse, error) {
	var chatReq protocol.ChatRequest

	if err := json.Unmarshal(
		req.Payload,
		&chatReq,
	); err != nil {
		return nil, fmt.Errorf(
			"decode chat payload: %w",
			err,
		)
	}

	// Kafka envelope содержит authoritative model.
	//
	// Не доверяем model из payload, если Gateway уже выбрал
	// конкретную модель.
	chatReq.Model = req.Model

	// Для текущего MVP worker всегда использует non-streaming.
	chatReq.Stream = false

	// providers.ChatRequest используется только как internal DTO.
	// Extra сохраняет оригинальный JSON request, включая:
	//
	// temperature
	// max_tokens
	// top_p
	// и т.д.
	providerReq := &providers.ChatRequest{
		Model:    req.Model,
		Stream:   false,
		Extra:    req.Payload,
	}

	for _, message := range chatReq.Messages {
		content := string(message.Content)

		// Content может быть JSON string.
		var text string
		if json.Unmarshal(
			message.Content,
			&text,
		) == nil {
			content = text
		}

		providerReq.Messages = append(
			providerReq.Messages,
			providers.Message{
				Role:    message.Role,
				Content: content,
			},
		)
	}

	if w.metrics != nil {
		w.metrics.RequestsActive.Inc()
		defer w.metrics.RequestsActive.Dec()
	}

	startedAt := time.Now().UTC()

	// Публикуем started event.
	//
	// Gateway может перевести job:
	//
	// queued -> running
	//
	// Event sequence 0 оставляем за queued,
	// поэтому started получает sequence 1,
	// completed получает sequence 2.
	if err := w.publishEvent(
		ctx,
		req,
		1,
		protocol.TypeStarted,
		protocol.StartedPayload{
			Provider: req.Provider,
			Model:    req.Model,
		},
	); err != nil {
		return nil, fmt.Errorf(
			"publish started event: %w",
			err,
		)
	}

	// Основной HTTP вызов к heavy LLM.
	response, err := w.provider.CompleteSync(
		ctx,
		providerReq,
	)
	if err != nil {
		return nil, err
	}

	if len(response.Choices) == 0 {
		return nil, errors.New(
			"LLM response contains no choices",
		)
	}

	choice := response.Choices[0]

	content := ""

	// protocol.ChatMessage.Content — json.RawMessage.
	//
	// В стандартном OpenAI-compatible response это:
	//
	//     "content": "text"
	//
	// Но некоторые серверы могут вернуть null/object.
	if len(choice.Message.Content) > 0 &&
		string(choice.Message.Content) != "null" {

		var text string

		if err := json.Unmarshal(
			choice.Message.Content,
			&text,
		); err == nil {
			content = text
		} else {
			// Если content не string, сохраняем JSON как есть.
			content = string(choice.Message.Content)
		}
	}

	finishReason := ""

	if choice.FinishReason != nil {
		finishReason = *choice.FinishReason
	}

	if finishReason == "" {
		finishReason = "stop"
	}

	var usage json.RawMessage

	if response.Usage != nil {
		b, err := json.Marshal(response.Usage)
		if err != nil {
			return nil, fmt.Errorf(
				"marshal usage: %w",
				err,
			)
		}

		usage = b
	}

	completedAt := time.Now().UTC()

	payload := protocol.CompletedPayload{
		FinishReason:       finishReason,
		Usage:              usage,
		Content:            content,
		Reasoning:          "",
		ReasoningAvailable: false,
		Events:             2,
		DurationMS:         time.Since(startedAt).Milliseconds(),
	}

	// Terminal event в llm.events.
	//
	// Именно его сейчас читает Gateway.
	//
	// Поэтому Gateway не обязан читать llm.completed для live response.
	if err := w.publishEvent(
		ctx,
		req,
		2,
		protocol.TypeCompleted,
		payload,
	); err != nil {
		return nil, fmt.Errorf(
			"publish completed event: %w",
			err,
		)
	}

	// Aggregate result в llm.completed.
	//
	// Это уже не event log, а готовый результат для downstream services.
	completion := protocol.Completion{
		RequestID:   req.RequestID,
		Status:      protocol.StatusCompleted,
		Provider:    req.Provider,
		Model:       req.Model,
		Content:     content,
		Reasoning:   "",
		Usage:       usage,
		Events:      2,
		StartedAt:   startedAt,
		CompletedAt: completedAt,
	}

	data, err := json.Marshal(completion)
	if err != nil {
		return nil, fmt.Errorf(
			"marshal completion: %w",
			err,
		)
	}

	if err := w.publish(
		ctx,
		w.topics.Completed,
		req.RequestID,
		data,
	); err != nil {
		return nil, fmt.Errorf(
			"publish completion: %w",
			err,
		)
	}

	return response, nil
}

// publishEvent writes an event to llm.events.
func (w *Worker) publishEvent(
	ctx context.Context,
	req protocol.Request,
	sequence uint64,
	eventType string,
	payload any,
) error {
	ev, err := protocol.NewEvent(
		req.RequestID,
		sequence,
		eventType,
		payload,
	)
	if err != nil {
		return err
	}

	ev.Provider = req.Provider
	ev.Model = req.Model

	data, err := json.Marshal(ev)
	if err != nil {
		return err
	}

	return w.publish(
		ctx,
		w.topics.Events,
		req.RequestID,
		data,
	)
}

// publishFailed writes terminal failure information.
//
// Важно:
//
// Если llm.failed не удалось записать, мы возвращаем ошибку.
//
// Тогда consumer НЕ делает MarkMessage.
//
// Kafka сможет отдать request повторно.
//
// Это лучше, чем потерять request.
func (w *Worker) publishFailed(
	ctx context.Context,
	req protocol.Request,
	attempt int,
	providerErr error,
) error {
	retryable := providers.IsRetryable(
		providerErr,
	)

	payload := protocol.FailedPayload{
		Error:     providerErr.Error(),
		Retryable: retryable,
		Attempt:   attempt,
	}

	// Sequence 2 соответствует terminal event после started.
	if err := w.publishEvent(
		ctx,
		req,
		2,
		protocol.TypeFailed,
		payload,
	); err != nil {
		return fmt.Errorf(
			"publish failed event: %w",
			err,
		)
	}

	failure := protocol.Failure{
		RequestID: req.RequestID,
		Provider:  req.Provider,
		Model:     req.Model,
		Error:     providerErr.Error(),
		Retryable: retryable,
		Attempt:   attempt,
		Timestamp: time.Now().UTC(),
	}

	data, err := json.Marshal(failure)
	if err != nil {
		return fmt.Errorf(
			"marshal failure: %w",
			err,
		)
	}

	if err := w.publish(
		ctx,
		w.topics.Failed,
		req.RequestID,
		data,
	); err != nil {
		return fmt.Errorf(
			"publish failure: %w",
			err,
		)
	}

	// После исчерпания retry дополнительно сохраняем исходный request
	// в DLQ.
	//
	// DLQ является secondary durable storage.
	//
	// Даже если DLQ publish не удался, llm.failed уже записан,
	// поэтому request не должен теряться.
	if attempt >= w.maxRetry &&
		retryable {
		w.publishDLQ(
			ctx,
			req,
			attempt,
			providerErr,
		)
	}

	if w.metrics != nil {
		w.metrics.RequestsFailed.
			WithLabelValues(req.Provider).
			Inc()

		w.metrics.RequestsTotal.
			WithLabelValues(req.Provider, "failed").
			Inc()
	}

	return nil
}

// publishDLQ writes an exhausted request to llm.dlq.
//
// Ошибка DLQ здесь логируется, но не возвращается.
//
// Причина:
//
// llm.failed уже является terminal result.
//
// Иначе временная недоступность DLQ заставит заново запускать
// потенциально дорогую LLM generation.
func (w *Worker) publishDLQ(
	ctx context.Context,
	req protocol.Request,
	attempt int,
	providerErr error,
) {
	record := protocol.DLQRecord{
		RequestID: req.RequestID,
		Attempt:   attempt,
		Error:     providerErr.Error(),
		Timestamp: time.Now().UTC(),
		Original:  req.Payload,
	}

	data, err := json.Marshal(record)
	if err != nil {
		w.log.Error(
			"marshal DLQ record failed",
			"request_id", req.RequestID,
			"err", err,
		)
		return
	}

	if err := w.publish(
		ctx,
		w.topics.DLQ,
		req.RequestID,
		data,
	); err != nil {
		w.log.Error(
			"publish DLQ failed",
			"request_id", req.RequestID,
			"err", err,
		)
	}
}

// handleInvalidRequest handles malformed llm.requests records.
//
// Мы не хотим бесконечно retry'ить JSON, который невозможно распарсить.
//
// Поэтому отправляем его в llm.dlq и возвращаем nil только если
// DLQ удалось записать.
func (w *Worker) handleInvalidRequest(
	ctx context.Context,
	rec kafka.Record,
	parseErr error,
) error {
	reqID := string(rec.Key)

	if reqID == "" {
		reqID = fmt.Sprintf(
			"%s/%d/%d",
			rec.Topic,
			rec.Partition,
			rec.Offset,
		)
	}

	dlq := protocol.DLQRecord{
		RequestID: reqID,
		Attempt:   0,
		Error:     parseErr.Error(),
		Timestamp: time.Now().UTC(),
		Original:  rec.Value,
	}

	data, err := json.Marshal(dlq)
	if err != nil {
		return fmt.Errorf(
			"marshal invalid request DLQ: %w",
			err,
		)
	}

	if err := w.publish(
		ctx,
		w.topics.DLQ,
		string(rec.Key),
		data,
	); err != nil {
		return fmt.Errorf(
			"publish invalid request DLQ: %w",
			err,
		)
	}

	return nil
}

func (w *Worker) publish(
	ctx context.Context,
	topic string,
	key string,
	value []byte,
) error {
	if w.metrics != nil {
		if err := w.producer.Publish(
			ctx,
			topic,
			key,
			value,
		); err != nil {
			w.metrics.KafkaPublish.
				WithLabelValues(topic, "error").
				Inc()

			w.metrics.KafkaPublishErr.
				WithLabelValues(topic).
				Inc()

			return err
		}

		w.metrics.KafkaPublish.
			WithLabelValues(topic, "ok").
			Inc()

		return nil
	}

	return w.producer.Publish(
		ctx,
		topic,
		key,
		value,
	)
}

func (w *Worker) metricsCompleted(
	provider string,
	start time.Time,
) {
	if w.metrics == nil {
		return
	}

	w.metrics.GenerationDur.
		WithLabelValues(provider).
		Observe(time.Since(start).Seconds())

	w.metrics.RequestsTotal.
		WithLabelValues(provider, "completed").
		Inc()
}

// Compile-time assertion.
//
// Если heavy.Client перестанет реализовывать SyncProvider,
// проект не соберётся.
var _ SyncProvider = (*syncProviderCompileCheck)(nil)

// syncProviderCompileCheck существует только для документации интерфейса.
// Реальный provider передаётся из cmd/worker.
type syncProviderCompileCheck struct{}

func (*syncProviderCompileCheck) CompleteSync(
	context.Context,
	*providers.ChatRequest,
) (*protocol.ChatResponse, error) {
	return nil, nil
}

// Защищаемся от случайного удаления sync import'ов в будущем.
var _ = sync.Mutex{}
