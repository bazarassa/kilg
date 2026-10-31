// Package worker implements the heavy LLM worker.
//
// Модель (PLAN §8/§9/§14/§21/§55):
//
//	llm.requests
//	     |
//	     v
//	Kafka consumer
//	     |
//	     v
//	HTTP POST /chat/completions (stream=true)
//	     |
//	     v
//	SSE stream от provider'а
//	     |
//	     +----> каждый чанк -> raw_sse event -> llm.events
//	     +----> heartbeat (пока provider молчит) -> llm.events
//	     |
//	     v
//	completed (после сохранённого последнего чанка, PLAN §31)
//	     |
//	     +----> llm.completed (агрегированный результат, PLAN §32)
//	     |
//	     v
//	MarkMessage()
//
// Последствия:
//
//   - 30-минутная генерация выживает: события живут в Kafka,
//     отключение клиента не останавливает генерацию (PLAN §21);
//   - reasoning_content сохраняется как отдаёт provider (PLAN §8/§33);
//   - оригинальные SSE-события не реконструируются (PLAN §9/§55) —
//     llm.events является durable transcript генерации;
//   - sequence монотонно растёт внутри request, в т.ч. между retries
//     (PLAN §10);
//   - heartbeat каждые HEAVY_HEARTBEAT секунд (PLAN §28, INV-005).
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/kafka-llm-gateway/gateway/internal/kafka"
	"github.com/kafka-llm-gateway/gateway/internal/observability"
	"github.com/kafka-llm-gateway/gateway/internal/protocol"
	"github.com/kafka-llm-gateway/gateway/internal/providers"
)

// StreamProvider is the minimal interface required by this worker.
//
// Worker не должен знать, как именно устроен HTTP client.
//
// Ему достаточно:
//
//	Complete(ctx, request, events) -> error
//
// где events — канал provider-событий (raw SSE-чанки), который
// закрывается по завершении, а Complete возвращает итоговую ошибку.
//
// Сейчас реализация — heavy.Client.
type StreamProvider interface {
	Complete(
		context.Context,
		*providers.ChatRequest,
		chan<- providers.ProviderEvent,
	) error
}

// Worker processes durable Kafka requests.
type Worker struct {
	producer kafka.EventProducer
	provider StreamProvider
	topics   kafka.Topics

	metrics *observability.Metrics
	log     *slog.Logger

	// Максимальное количество одновременных LLM generation.
	workers int

	// Максимальное количество попыток одной Kafka request.
	maxRetry int

	// Период heartbeat-событий во время долгой генерации.
	//
	// Ноль/отрицательное значение отключает heartbeat.
	heartbeat time.Duration
}

// New builds a worker.
func New(
	producer kafka.EventProducer,
	provider StreamProvider,
	topics kafka.Topics,
	metrics *observability.Metrics,
	log *slog.Logger,
	workers int,
	maxRetry int,
	heartbeat time.Duration,
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

		workers:   workers,
		maxRetry:  maxRetry,
		heartbeat: heartbeat,
	}
}

// Run consumes llm.requests.
//
// Важнейшее отличие от старой реализации:
//
//	handler НЕ запускает process() в detached goroutine.
//
// Он ждёт завершения process().
//
// Поэтому consumer не делает MarkMessage до завершения обработки.
//
// При этом semaphore всё равно позволяет обрабатывать несколько
// partition параллельно:
//
//	partition 0 -> worker slot 1
//	partition 1 -> worker slot 2
//	partition 2 -> ждёт свободный slot
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
//
//	-> completed
//	-> nil
//
// provider failure
//
//	-> retry if retryable
//	-> failed + optional DLQ after maxRetry
//	-> nil, если terminal failure успешно опубликован
//
// Ошибка публикации terminal result
//
//	-> error
//	-> offset НЕ MarkMessage
//	-> Kafka retry.
//
// seqCounter выдаёт монотонные sequence-номера для одного request.
//
// Sequence 0 зарезервирован за queued-событием Gateway.
//
// Счётчик живёт дольше одной попытки: при retry номера продолжаются,
// поэтому llm.events остаётся монотонным (PLAN §10), а Gateway
// корректно дедуплицирует redelivery.
type seqCounter struct {
	n uint64
}

func (c *seqCounter) next() uint64 {
	c.n++

	return c.n
}

func (c *seqCounter) current() uint64 {
	return c.n
}

func (w *Worker) process(
	ctx context.Context,
	req protocol.Request,
) error {
	start := time.Now()

	attempt := req.Attempt

	seq := &seqCounter{}

	for {
		attempt++

		err := w.runOnce(
			ctx,
			req,
			attempt,
			seq,
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
				seq,
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
				seq,
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

// buildProviderRequest строит provider-запрос из Kafka-огибающего.
func (w *Worker) buildProviderRequest(
	req protocol.Request,
) (*providers.ChatRequest, error) {
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

	// providers.ChatRequest используется только как internal DTO.
	//
	// Extra сохраняет оригинальный JSON request, включая:
	//
	// temperature
	// max_tokens
	// top_p
	// и т.д.
	providerReq := &providers.ChatRequest{
		Model:  req.Model,
		Stream: true,
		Extra:  req.Payload,
	}

	// Kafka envelope содержит authoritative model.
	//
	// Не доверяем model из payload, если Gateway уже выбрал
	// конкретную модель.
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

	return providerReq, nil
}

// streamAggregation incrementally строит итоговый результат из
// raw-чанков provider'а (PLAN §32/§33).
//
// Raw-чанки при этом НЕ реконструируются: они сохраняются в
// llm.events как есть (PLAN §9/§55), агрегат — служебный.
type streamAggregation struct {
	content   strings.Builder
	reasoning strings.Builder
	usage     json.RawMessage
	finish    string
	rawEvents uint64
}

// providerChunk — минимальный срез OpenAI-compatible чанка для
// агрегации content/reasoning/usage/finish_reason.
type providerChunk struct {
	Choices []struct {
		Delta struct {
			Content          *string `json:"content"`
			ReasoningContent *string `json:"reasoning_content"`
		} `json:"delta"`
		FinishReason *string `json:"finish_reason"`
	} `json:"choices"`
	Usage *protocol.Usage `json:"usage"`
}

// absorb обновляет агрегат по одному raw-чанку.
//
// Непарсируемый чанк не ломает поток: он всё равно сохранён в
// llm.events, агрегация просто пропускает его.
func (a *streamAggregation) absorb(raw string) {
	a.rawEvents++

	var chunk providerChunk

	if err := json.Unmarshal([]byte(raw), &chunk); err != nil {
		return
	}

	for i := range chunk.Choices {
		c := &chunk.Choices[i]

		if c.Delta.Content != nil {
			a.content.WriteString(*c.Delta.Content)
		}

		// reasoning_content сохраняется как provider-provided field
		// (PLAN §33): Gateway не «извлекает мысли» из текста сам.
		if c.Delta.ReasoningContent != nil {
			a.reasoning.WriteString(*c.Delta.ReasoningContent)
		}

		if c.FinishReason != nil && *c.FinishReason != "" {
			a.finish = *c.FinishReason
		}
	}

	// usage приходит в финальном чанке (stream_options include_usage)
	// или не приходит вовсе — тогда остаётся пустым.
	if chunk.Usage != nil {
		a.usage, _ = json.Marshal(chunk.Usage)
	}
}

// runOnce выполняет одну попытку streaming-генерации.
//
// Публикует в llm.events:
//
//	started
//	raw_sse    (по одному на каждый чанк provider'а, в порядке provider'а)
//	heartbeat  (если provider молчит дольше w.heartbeat)
//	completed  (после сохранённого последнего чанка, PLAN §31)
//
// Sequence-номера продолжают расти между попытками (PLAN §10).
func (w *Worker) runOnce(
	ctx context.Context,
	req protocol.Request,
	attempt int,
	seq *seqCounter,
) error {
	providerReq, err := w.buildProviderRequest(req)
	if err != nil {
		return err
	}

	if w.metrics != nil {
		w.metrics.RequestsActive.Inc()
		defer w.metrics.RequestsActive.Dec()
	}

	startedAt := time.Now().UTC()

	// Публикуем started event на каждой попытке.
	//
	// Gateway может перевести job:
	//
	// queued -> running
	//
	// Sequence 0 за queued оставляет Gateway, started получает
	// следующий номер (обычно 1).
	if err := w.publishEvent(
		ctx,
		req,
		seq.next(),
		protocol.TypeStarted,
		protocol.StartedPayload{
			Provider: req.Provider,
			Model:    req.Model,
		},
	); err != nil {
		return fmt.Errorf(
			"publish started event: %w",
			err,
		)
	}

	events := make(chan providers.ProviderEvent, 64)
	providerErr := make(chan error, 1)

	// Provider живёт в отдельной goroutine: Complete возвращает
	// только после конца потока и гарантированно закрывает канал.
	//
	// При раннем выходе (ошибка публикации/Kafka) goroutine живёт
	// до конца HTTP-запроса provider'а, который ограничен
	// GenerationTimeout — это допустимая утечка на редкий сбой.
	go func() {
		providerErr <- w.provider.Complete(
			ctx,
			providerReq,
			events,
		)
	}()

	agg := &streamAggregation{}

	var heartbeatC <-chan time.Time

	if w.heartbeat > 0 {
		ticker := time.NewTicker(w.heartbeat)
		defer ticker.Stop()

		heartbeatC = ticker.C
	}

	if err := w.consumeStream(
		ctx,
		req,
		seq,
		agg,
		events,
		heartbeatC,
	); err != nil {
		return err
	}

	// Ожидаем финальную ошибку provider'а.
	if err := <-providerErr; err != nil {
		return fmt.Errorf("provider stream: %w", err)
	}

	if agg.rawEvents == 0 {
		// Пустой поток — та же семантика, что и legacy
		// "response contains no choices": retryable ошибка.
		return errors.New("provider returned empty stream")
	}

	if err := w.publishCompletion(
		ctx,
		req,
		seq,
		startedAt,
		agg,
	); err != nil {
		return err
	}

	w.observeTokens(agg.usage)

	return nil
}

// consumeStream перекачивает provider-события в llm.events до конца
// потока, публикуя heartbeat во время пауз (INV-005).
func (w *Worker) consumeStream(
	ctx context.Context,
	req protocol.Request,
	seq *seqCounter,
	agg *streamAggregation,
	events <-chan providers.ProviderEvent,
	heartbeatC <-chan time.Time,
) error {
loop:
	for {
		select {
		case ev, ok := <-events:
			if !ok {
				break loop
			}

			if err := w.handleProviderEvent(
				ctx,
				req,
				seq,
				agg,
				ev,
			); err != nil {
				return err
			}

		case <-heartbeatC:
			// Heartbeat: job жив, provider молчит. Держит живыми
			// SSE-соединения и прокси с таймаутами.
			//
			// LastSequence — последнее СОДЕРЖАНИЕ-событие до этого
			// heartbeat (heartbeat получает следующий номер сам).
			last := seq.current()

			if err := w.publishEvent(
				ctx,
				req,
				last+1,
				protocol.TypeHeartbeat,
				protocol.HeartbeatPayload{
					LastSequence: last,
				},
			); err != nil {
				return err
			}

		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

// publishCompletion пишет terminal completed event и агрегат в
// llm.completed (PLAN §31/§32).
func (w *Worker) publishCompletion(
	ctx context.Context,
	req protocol.Request,
	seq *seqCounter,
	startedAt time.Time,
	agg *streamAggregation,
) error {
	completedAt := time.Now().UTC()

	payload := protocol.CompletedPayload{
		FinishReason:       agg.finish,
		Usage:              agg.usage,
		Content:            agg.content.String(),
		Reasoning:          agg.reasoning.String(),
		ReasoningAvailable: agg.reasoning.Len() > 0,
		Stream:             true,
		Events:             agg.rawEvents,
		DurationMS:         completedAt.Sub(startedAt).Milliseconds(),
	}

	// Terminal event в llm.events.
	//
	// Публикуется ПОСЛЕ того, как последний чанк сохранён в Kafka
	// (PLAN §31), поэтому Gateway не обязан читать llm.completed
	// для live response.
	if err := w.publishEvent(
		ctx,
		req,
		seq.next(),
		protocol.TypeCompleted,
		payload,
	); err != nil {
		return fmt.Errorf(
			"publish completed event: %w",
			err,
		)
	}

	// Aggregate result в llm.completed — готовый результат
	// для downstream services (PLAN §32).
	completion := protocol.Completion{
		RequestID:   req.RequestID,
		Status:      protocol.StatusCompleted,
		Provider:    req.Provider,
		Model:       req.Model,
		Content:     agg.content.String(),
		Reasoning:   agg.reasoning.String(),
		Usage:       agg.usage,
		Events:      agg.rawEvents,
		StartedAt:   startedAt,
		CompletedAt: completedAt,
	}

	data, err := json.Marshal(completion)
	if err != nil {
		return fmt.Errorf(
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
		return fmt.Errorf(
			"publish completion: %w",
			err,
		)
	}

	return nil
}

// handleProviderEvent обрабатывает одно provider-событие:
// сохраняет его в llm.events и обновляет агрегат.
func (w *Worker) handleProviderEvent(
	ctx context.Context,
	req protocol.Request,
	seq *seqCounter,
	agg *streamAggregation,
	ev providers.ProviderEvent,
) error {
	switch ev.Type {
	case providers.EventRawSSE:
		if err := w.publishRaw(ctx, req, seq, ev.Raw); err != nil {
			return err
		}

		agg.absorb(ev.Raw)

	case providers.EventDone:
		// Provider сигнализировал конец потока;
		// итоговая ошибка (если есть) придёт от Complete.

	case providers.EventError:
		// Provider уже вернул (или вернёт) эту ошибку;
		// обработка — после цикла.

	default:
		// Неизвестное provider-specific событие: сохраняем его raw,
		// чтобы llm.events оставался полным транскриптом (PLAN §28).
		if ev.Raw == "" {
			return nil
		}

		if err := w.publishRaw(ctx, req, seq, ev.Raw); err != nil {
			return err
		}

		agg.absorb(ev.Raw)
	}

	return nil
}

// publishRaw сохраняет raw-чанк provider'а в llm.events как есть.
func (w *Worker) publishRaw(
	ctx context.Context,
	req protocol.Request,
	seq *seqCounter,
	raw string,
) error {
	return w.publishEvent(
		ctx,
		req,
		seq.next(),
		protocol.TypeRawSSE,
		protocol.RawSSEPayload{Data: raw},
	)
}

// observeTokens обновляет token-счётчики по usage от provider'а.
func (w *Worker) observeTokens(usage json.RawMessage) {
	if w.metrics == nil || len(usage) == 0 {
		return
	}

	var u protocol.Usage

	if err := json.Unmarshal(usage, &u); err != nil {
		return
	}

	w.metrics.TokensTotal.WithLabelValues("prompt").
		Add(float64(u.PromptTokens))
	w.metrics.TokensTotal.WithLabelValues("completion").
		Add(float64(u.CompletionTokens))
	w.metrics.TokensTotal.WithLabelValues("total").
		Add(float64(u.TotalTokens))

	w.metrics.OutputTokens.Add(float64(u.CompletionTokens))
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
	seq *seqCounter,
) error {
	retryable := providers.IsRetryable(
		providerErr,
	)

	payload := protocol.FailedPayload{
		Error:     providerErr.Error(),
		Retryable: retryable,
		Attempt:   attempt,
	}

	// Terminal event получает следующий номер: после started и всех
	// raw/heartbeat-событий последней попытки (PLAN §10).
	if err := w.publishEvent(
		ctx,
		req,
		seq.next(),
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
		Original:  validRawOrString(rec.Value),
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

// validRawOrString returns b as a json.RawMessage that always survives
// json.Marshal.
//
// If b is not valid JSON (poison message), it is stored as a JSON
// string.
//
// Otherwise the whole DLQRecord would be unmarshallable and the
// message could never be quarantined.
func validRawOrString(b []byte) json.RawMessage {
	var probe json.RawMessage

	if json.Unmarshal(b, &probe) == nil {
		return json.RawMessage(b)
	}

	data, err := json.Marshal(string(b))
	if err != nil {
		return json.RawMessage("null")
	}

	return data
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
// Если heavy.Client перестанет реализовывать StreamProvider,
// проект не соберётся.
var _ StreamProvider = (*streamProviderCompileCheck)(nil)

// streamProviderCompileCheck существует только для документации
// интерфейса. Реальный provider передаётся из cmd/worker.
type streamProviderCompileCheck struct{}

func (*streamProviderCompileCheck) Complete(
	context.Context,
	*providers.ChatRequest,
	chan<- providers.ProviderEvent,
) error {
	return nil
}
