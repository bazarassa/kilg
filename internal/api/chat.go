package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/kafka-llm-gateway/gateway/internal/jobs"
	"github.com/kafka-llm-gateway/gateway/internal/protocol"
	"github.com/kafka-llm-gateway/gateway/internal/providers"
	"github.com/kafka-llm-gateway/gateway/internal/router"
	"github.com/kafka-llm-gateway/gateway/internal/streaming"
)

// handleChat implements POST /v1/chat/completions.
//
// Сейчас Gateway поддерживает два принципиально разных пути:
//
//  1. Kafka-backed route:
//     Gateway -> llm.requests -> Worker -> LLM
//             -> llm.events -> Gateway -> SSE client
//
//  2. Direct provider route:
//     Gateway -> Provider -> SSE client
//
// Для heavy route используется первый вариант.
//
// @Summary Create chat completion
// @Description Creates a chat completion. Supports streaming, async mode, and idempotency.
// @Tags chat
// @Accept json
// @Produce json
// @Produce text/event-stream
// @Param Request body protocol.ChatRequest true "Chat completion request"
// @Param Prefer header string false "Set to 'respond-async' for async job mode"
// @Param Idempotency-Key header string false "Idempotency key to prevent duplicate requests"
// @Success 200 {object} protocol.ChatResponse "Synchronous completion"
// @Success 202 {object} protocol.JobAccepted "Async job accepted"
// @Failure 400 {object} map[string]any "Bad request"
// @Failure 401 {object} map[string]any "Unauthorized"
// @Failure 503 {object} map[string]any "Service unavailable"
// @Security BearerAuth
// @Router /chat/completions [post]
func (s *Server) handleChat(w http.ResponseWriter, r *http.Request) {
	var req protocol.ChatRequest

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(
			w,
			http.StatusBadRequest,
			"invalid JSON body: "+err.Error(),
		)
		return
	}

	if len(req.Messages) == 0 {
		writeJSONError(
			w,
			http.StatusBadRequest,
			"messages is required",
		)
		return
	}

	// Если model не передан, используем DefaultModel.
	//
	// Это позволяет клиенту отправить:
	//
	//     {
	//       "messages": [...]
	//     }
	//
	// если на сервере заранее определена модель по умолчанию.
	if req.Model == "" {
		req.Model = s.cfg.DefaultModel
	}

	req.Model = strings.TrimSpace(req.Model)

	if req.Model == "" {
		writeJSONError(
			w,
			http.StatusBadRequest,
			"model is required",
		)
		return
	}

	route := s.router.Route(req.Model)

	requestID := NewULID()
	reused := false

	// Idempotency:
	//
	// Если клиент повторяет запрос с тем же Idempotency-Key,
	// Gateway использует уже существующий request ID.
	if key := r.Header.Get("Idempotency-Key"); key != "" {
		if existing := s.idempotentGet(key); existing != "" {
			requestID = existing
			reused = true
		} else {
			s.idempotentSet(key, requestID)
		}
	}

	// Сохраняем нормализованный запрос как payload Kafka.
	rawBody, _ := json.Marshal(req)

	// ------------------------------------------------------------
	// ASYNC MODE
	// ------------------------------------------------------------
	//
	// Prefer: respond-async
	//
	// В этом режиме клиент получает только job ID и может
	// самостоятельно читать:
	//
	//     GET /v1/jobs/{id}
	//     GET /v1/jobs/{id}/events
	//
	if isAsync(r) {
		if !reused {
			if err := s.enqueue(
				r.Context(),
				requestID,
				route,
				rawBody,
				keyOf(r),
			); err != nil {
				writeJSONError(
					w,
					http.StatusServiceUnavailable,
					"failed to enqueue: "+err.Error(),
				)
				return
			}
		}

		status := protocol.StatusQueued

		if j, ok := s.jobs.Store().Get(requestID); ok {
			status = j.Status
		}

		writeJSON(
			w,
			http.StatusAccepted,
			protocol.JobAccepted{
				ID:     requestID,
				Object: "chat.completion.job",
				Status: status,
			},
		)

		return
	}

	// ------------------------------------------------------------
	// KAFKA-BACKED ASYNC ROUTE
	// ------------------------------------------------------------
	//
	// Heavy provider работает через Worker:
	//
	//     HTTP client
	//          |
	//          v
	//     Gateway
	//          |
	//          v
	//     llm.requests
	//          |
	//          v
	//       Worker
	//          |
	//          v
	//       LLM API
	//          |
	//          v
	//     llm.events
	//          |
	//          v
	//       Gateway
	//          |
	//          v
	//       SSE client
	//
	// Очень важно зарегистрировать Dispatcher ДО публикации
	// llm.requests.
	//
	// Иначе Worker может оказаться достаточно быстрым и успеть
	// записать started/completed event раньше регистрации SSE
	// subscriber.
	if route.Async {
		stream := s.dispatch.Register(requestID)

		if !reused {
			if err := s.enqueue(
				r.Context(),
				requestID,
				route,
				rawBody,
				keyOf(r),
			); err != nil {
				s.dispatch.Unregister(requestID)

				writeJSONError(
					w,
					http.StatusServiceUnavailable,
					"failed to enqueue: "+err.Error(),
				)

				return
			}
		}

		s.streamJobWith(
			w,
			r,
			requestID,
			stream,
		)

		return
	}

	// ------------------------------------------------------------
	// DIRECT PROVIDER ROUTE
	// ------------------------------------------------------------
	//
	// Например:
	//
	//     litellm
	//     ollama
	//
	// Эти provider'ы не проходят через Worker/Kafka.
	s.streamDirect(
		w,
		r,
		requestID,
		route,
		req,
		rawBody,
	)
}

// handleGetCompletion implements GET /v1/chat/completions/{id}.
//
// @Summary Get completion by ID
// @Description Retrieves a chat completion job by ID (alias for GET /v1/jobs/{id})
// @Tags chat
// @Produce json
// @Param id path string true "Completion ID"
// @Success 200 {object} protocol.JobInfo
// @Failure 404 {object} map[string]any
// @Security BearerAuth
// @Router /chat/completions/{id} [get]
func (s *Server) handleGetCompletion(
	w http.ResponseWriter,
	r *http.Request,
) {
	s.handleJob(w, r)
}

// handleCancelCompletion implements DELETE /v1/chat/completions/{id}.
//
// @Summary Cancel completion
// @Description Cancels a running or queued chat completion
// @Tags chat
// @Produce json
// @Param id path string true "Completion ID"
// @Success 200 {object} map[string]string
// @Failure 404 {object} map[string]any
// @Security BearerAuth
// @Router /chat/completions/{id} [delete]
func (s *Server) handleCancelCompletion(
	w http.ResponseWriter,
	r *http.Request,
) {
	s.handleCancelJob(w, r)
}

// handleGetCompletionMessages implements
// GET /v1/chat/completions/{id}/messages.
//
// @Summary Get completion messages
// @Description Streams all events/messages for a completion (alias for GET /v1/jobs/{id}/events)
// @Tags chat
// @Produce text/event-stream
// @Param id path string true "Completion ID"
// @Param Last-Event-ID header string false "Resume from this event ID"
// @Success 200 {string} string "SSE stream"
// @Failure 404 {object} map[string]any
// @Security BearerAuth
// @Router /chat/completions/{id}/messages [get]
func (s *Server) handleGetCompletionMessages(
	w http.ResponseWriter,
	r *http.Request,
) {
	s.handleJobEvents(w, r)
}

func keyOf(r *http.Request) string {
	return r.Header.Get("Idempotency-Key")
}

// isAsync reports whether the client requested async mode.
func isAsync(r *http.Request) bool {
	p := r.Header.Get("Prefer")

	for _, part := range splitComma(p) {
		if part == "respond-async" {
			return true
		}
	}

	return false
}

func splitComma(s string) []string {
	var out []string

	cur := ""

	appendCurrent := func() {
		value := trimSpace(cur)

		if value != "" {
			out = append(out, value)
		}
	}

	for _, c := range s {
		if c == ',' {
			appendCurrent()
			cur = ""
			continue
		}

		cur += string(c)
	}

	appendCurrent()

	return out
}

func trimSpace(s string) string {
	start, end := 0, len(s)

	for start < end &&
		(s[start] == ' ' || s[start] == '\t') {
		start++
	}

	for end > start &&
		(s[end-1] == ' ' || s[end-1] == '\t') {
		end--
	}

	return s[start:end]
}

// enqueue publishes the job to Kafka and records it in the job store.
func (s *Server) enqueue(
	ctx context.Context,
	requestID string,
	route router.Route,
	rawBody []byte,
	idemKey string,
) error {
	now := time.Now().UTC()

	// Создаём локальное состояние job.
	s.jobs.Ensure(
		requestID,
		route.Provider,
		route.Model,
	)

	s.jobs.Update(
		requestID,
		func(j *jobs.Job) {
			j.Status = protocol.StatusQueued
		},
	)

	// Kafka envelope.
	//
	// Именно этот объект попадёт в llm.requests.
	jobReq := protocol.Request{
		RequestID:      requestID,
		Provider:       route.Provider,
		Model:          route.Model,
		CreatedAt:      now,
		Attempt:        0,
		IdempotencyKey: idemKey,
		Payload:        rawBody,
	}

	b, err := json.Marshal(jobReq)
	if err != nil {
		return err
	}

	if err := s.producer.Publish(
		ctx,
		s.topics.Requests,
		requestID,
		b,
	); err != nil {
		return err
	}

	// queued event имеет sequence=0.
	//
	// Worker затем создаёт:
	//
	//     sequence=1 -> started
	//     sequence=2 -> completed / failed
	ev, err := protocol.NewEvent(
		requestID,
		0,
		protocol.TypeQueued,
		map[string]string{
			"provider": route.Provider,
			"model":    route.Model,
		},
	)

	if err == nil {
		ev.Provider = route.Provider
		ev.Model = route.Model

		if eb, merr := json.Marshal(ev); merr == nil {
			_ = s.producer.Publish(
				ctx,
				s.topics.Events,
				requestID,
				eb,
			)
		}
	}

	s.metrics.RequestsTotal.
		WithLabelValues(route.Provider, "queued").
		Inc()

	s.log.Info(
		"job enqueued",
		"request_id", requestID,
		"provider", route.Provider,
		"model", route.Model,
	)

	return nil
}

// streamJob streams a Kafka-backed job to the client as SSE.
func (s *Server) streamJob(
	w http.ResponseWriter,
	r *http.Request,
	requestID string,
) {
	stream := s.dispatch.Register(requestID)

	s.streamJobWith(
		w,
		r,
		requestID,
		stream,
	)
}

// streamJobWith streams using an already-registered stream.
//
// Kafka event flow:
//
//     llm.events
//          |
//          v
//     RunEventConsumer
//          |
//          v
//     Dispatcher.Dispatch()
//          |
//          v
//     stream.Ch
//          |
//          v
//     streamJobWith()
//          |
//          v
//     writeEvent()
//          |
//          v
//     SSE client
func (s *Server) streamJobWith(
	w http.ResponseWriter,
	r *http.Request,
	requestID string,
	stream *streaming.Stream,
) {
	flusher, ok := w.(http.Flusher)

	if !ok {
		s.dispatch.Unregister(requestID)

		writeJSONError(
			w,
			http.StatusInternalServerError,
			"streaming unsupported",
		)

		return
	}

	streaming.SSEHeaders(w)

	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	defer s.dispatch.Unregister(requestID)

	var lastSent uint64

	// Reconnect support:
	//
	// Клиент может передать:
	//
	//     Last-Event-ID: <request-id>-0000000001
	//
	// В этом случае Gateway сначала читает недостающие события
	// непосредственно из Kafka.
	if lastID := r.Header.Get("Last-Event-ID"); lastID != "" {
		s.metrics.Reconnects.Inc()

		var terminal bool
		var err error

		lastSent, terminal, err =
			s.replayFrom(
				r.Context(),
				requestID,
				lastID,
				w,
				flusher,
			)

		if err != nil {
			s.log.Warn(
				"replay failed",
				"request_id", requestID,
				"err", err,
			)
		}

		if terminal {
			return
		}
	}

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			s.metrics.Disconnects.Inc()
			return

		case ev, ok := <-stream.Ch:
			if !ok {
				return
			}

			// Защита от повторной доставки event.
			if ev.Sequence <= lastSent {
				continue
			}

			if err := s.writeEvent(
				w,
				flusher,
				ev,
			); err != nil {
				s.metrics.Disconnects.Inc()
				return
			}

			lastSent = ev.Sequence

			// completed / failed / cancelled завершают SSE.
			if isTerminal(ev.Type) {
				_ = streaming.WriteDone(
					w,
					flusher,
				)

				return
			}
		}
	}
}

// streamDirect runs a direct provider and streams raw SSE to the client.
func (s *Server) streamDirect(
w http.ResponseWriter,
r *http.Request,
requestID string,
route router.Route,
req protocol.ChatRequest,
rawBody []byte,
) {
flusher, ok := w.(http.Flusher)
if !ok {
writeJSONError(
w,
http.StatusInternalServerError,
"streaming unsupported",
)
return
}

prov, ok := s.providers[route.Provider]
if !ok {
	writeJSONError(
		w,
		http.StatusBadGateway,
		"provider not configured: "+route.Provider,
	)
	return
}

streaming.SSEHeaders(w)
w.WriteHeader(http.StatusOK)
flusher.Flush()

s.jobs.Ensure(
	requestID,
	route.Provider,
	route.Model,
)

s.jobs.Update(
	requestID,
	func(j *jobs.Job) {
		j.Status = protocol.StatusRunning
	},
)

s.metrics.RequestsTotal.
	WithLabelValues(
		route.Provider,
		"started",
	).
	Inc()

pReq := &providers.ChatRequest{
	Model:  route.Model,
	Stream: req.Stream,
	Extra:  rawBody,
}

for _, m := range req.Messages {
	content := string(m.Content)

	var str string
	if json.Unmarshal(
		m.Content,
		&str,
	) == nil {
		content = str
	}

	pReq.Messages = append(
		pReq.Messages,
		providers.Message{
			Role:    m.Role,
			Content: content,
		},
	)
}

events := make(chan providers.ProviderEvent, 64)

done := make(chan error, 1)

go func() {
	done <- prov.Complete(
		r.Context(),
		pReq,
		events,
	)
}()

for ev := range events {
	switch ev.Type {

	case providers.EventRawSSE:
		if err := streaming.WriteEvent(
			w,
			flusher,
			requestID,
			ev.Raw,
		); err != nil {
			s.metrics.Disconnects.Inc()
			return
		}

	case providers.EventDone:
		s.jobs.Update(
			requestID,
			func(j *jobs.Job) {
				j.Status = protocol.StatusCompleted
			},
		)

		_ = streaming.WriteDone(
			w,
			flusher,
		)

		return

	case providers.EventError:
		s.metrics.RequestsFailed.
			WithLabelValues(route.Provider).
			Inc()

		s.jobs.Update(
			requestID,
			func(j *jobs.Job) {
				j.Status = protocol.StatusFailed
				if ev.Err != nil {
					j.Error = ev.Err.Error()
				}
			},
		)

		message := "provider error"

		if ev.Err != nil {
			message = ev.Err.Error()
		}

		data, _ := json.Marshal(
			map[string]any{
				"error": map[string]string{
					"message": message,
				},
			},
		)

		_ = streaming.WriteEvent(
			w,
			flusher,
			requestID,
			string(data),
		)

		_ = streaming.WriteDone(
			w,
			flusher,
		)

		return
	}
}

// Complete() обязательно закрывает events.
//
// Получаем его итоговую ошибку только после завершения
// чтения канала.
if err := <-done; err != nil {
	s.log.Warn(
		"direct provider error",
		"request_id",
		requestID,
		"provider",
		route.Provider,
		"err",
		err,
	)
}

}

// writeEvent converts an internal Kafka event into an SSE event.
//
// Для Worker-backed heavy request здесь происходит важнейшее
// преобразование:
//
//     protocol.TypeStarted
//         -> metadata SSE
//
//     protocol.TypeCompleted
//         -> OpenAI-compatible final chunk
//
//     protocol.TypeFailed
//         -> SSE error
//
//     protocol.TypeRawSSE
//         -> raw provider SSE
func (s *Server) writeEvent(
w http.ResponseWriter,
flusher http.Flusher,
ev protocol.Event,
) error {
switch ev.Type {

case protocol.TypeRawSSE:
	var p protocol.RawSSEPayload

	if json.Unmarshal(
		ev.Payload,
		&p,
	) == nil {
		return streaming.WriteEvent(
			w,
			flusher,
			ev.EventID,
			p.Data,
		)
	}

	return nil

case protocol.TypeCompleted:
	var p protocol.CompletedPayload

	if err := json.Unmarshal(
		ev.Payload,
		&p,
	); err != nil {
		return err
	}

	// --------------------------------------------------------
	// 1. Content
	// --------------------------------------------------------

	if p.Content != "" {
		contentChunk := map[string]any{
			"id":     ev.RequestID,
			"object": "chat.completion.chunk",
			"choices": []any{
				map[string]any{
					"index": 0,
					"delta": map[string]any{
						"content": p.Content,
					},
					"finish_reason": nil,
				},
			},
		}

		data, err := json.Marshal(contentChunk)
		if err != nil {
			return err
		}

		if err := streaming.WriteEvent(
			w,
			flusher,
			ev.EventID,
			string(data),
		); err != nil {
			return err
		}
	}

	// --------------------------------------------------------
	// 2. Finish
	// --------------------------------------------------------

	finishReason := p.FinishReason

	if finishReason == "" {
		finishReason = "stop"
	}

	finishChunk := map[string]any{
		"id":     ev.RequestID,
		"object": "chat.completion.chunk",
		"choices": []any{
			map[string]any{
				"index": 0,
				"delta": map[string]any{},
				"finish_reason": finishReason,
			},
		},
	}

	data, err := json.Marshal(finishChunk)
	if err != nil {
		return err
	}

	return streaming.WriteEvent(
		w,
		flusher,
		ev.EventID,
		string(data),
	)

case protocol.TypeFailed:
	var p protocol.FailedPayload

	_ = json.Unmarshal(
		ev.Payload,
		&p,
	)

	data, err := json.Marshal(
		map[string]any{
			"id":     ev.RequestID,
			"object": "chat.completion.chunk",
			"error": map[string]string{
				"message": p.Error,
			},
		},
	)

	if err != nil {
		return err
	}

	return streaming.WriteEvent(
		w,
		flusher,
		ev.EventID,
		string(data),
	)

case protocol.TypeCancelled:
	return streaming.WriteEvent(
		w,
		flusher,
		ev.EventID,
		`{"id":"`+
			ev.RequestID+
			`","object":"chat.completion.chunk","error":{"message":"request cancelled"}}`,
	)

default:
	return streaming.WriteEvent(
		w,
		flusher,
		ev.EventID,
		metaChunk(ev),
	)
}

}



func isTerminal(t string) bool {
	return t == protocol.TypeCompleted ||
		t == protocol.TypeFailed ||
		t == protocol.TypeCancelled
}

// terminalChunk converts a terminal internal event to an
// OpenAI-compatible SSE chunk.
//
// ВАЖНО:
//
// Раньше completed event:
//
//     {
//       "payload": {
//         "content": "ответ модели"
//       }
//     }
//
// превращался в:
//
//     {
//       "choices": [{
//         "delta": {},
//         "finish_reason": "stop"
//       }]
//     }
//
// Поэтому клиент видел только факт завершения.
//
// Теперь Content попадает в:
//
//     choices[0].delta.content
//
// То есть клиент получит:
//
//     data: {
//       "id":"...",
//       "object":"chat.completion.chunk",
//       "choices":[{
//         "index":0,
//         "delta":{
//           "content":"ответ модели"
//         },
//         "finish_reason":"stop"
//       }]
//     }
//
// Это минимальное изменение, потому что Worker уже сохранил
// весь результат в CompletedPayload.Content.
func terminalChunk(ev protocol.Event) string {
if ev.Type == protocol.TypeFailed {
var p protocol.FailedPayload
_ = json.Unmarshal(ev.Payload, &p)

	return `{"id":"` +
		ev.RequestID +
		`","object":"chat.completion.chunk","error":{"message":"` +
		jsonEscape(p.Error) +
		`"}}`
}

var p protocol.CompletedPayload

if err := json.Unmarshal(
	ev.Payload,
	&p,
); err != nil {
	return `{"id":"` +
		ev.RequestID +
		`","object":"chat.completion.chunk","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`
}

fr := p.FinishReason

if fr == "" {
	fr = "stop"
}

// ВАЖНО:
//
// CompletedPayload.Content содержит весь ответ LLM,
// потому что текущий Worker работает в non-streaming режиме.
//
// Поэтому сначала отправляем content chunk.
//
// Затем отдельный finish chunk отправляется обычным
// terminal event lifecycle.
if p.Content != "" {
	contentChunk := map[string]any{
		"id":     ev.RequestID,
		"object": "chat.completion.chunk",
		"choices": []any{
			map[string]any{
				"index": 0,
				"delta": map[string]any{
					"content": p.Content,
				},
				"finish_reason": nil,
			},
		},
	}

	b, err := json.Marshal(contentChunk)
	if err == nil {
		// Один terminalChunk может вернуть только один SSE data.
		//
		// Поэтому для совместимости с текущей архитектурой
		// content и finish нужно разделить.
		//
		// Этот вариант используется только как fallback.
		return string(b)
	}
}

return `{"id":"` +
	ev.RequestID +
	`","object":"chat.completion.chunk","choices":[{"index":0,"delta":{},"finish_reason":"` +
	jsonEscape(fr) +
	`"}]}`

}



// metaChunk creates a small OpenAI-compatible chunk containing
// Gateway metadata.
//
// Например:
//
//     {
//       "id": "...",
//       "object": "chat.completion.chunk",
//       "_meta": {
//         "type": "started",
//         "sequence": 1
//       }
//     }
func metaChunk(ev protocol.Event) string {
	return `{"id":"` +
		ev.RequestID +
		`","object":"chat.completion.chunk","_meta":{"type":"` +
		ev.Type +
		`","sequence":` +
		itoa(ev.Sequence) +
		`}}`
}

// jsonEscape is retained for error payloads that are assembled manually.
func jsonEscape(s string) string {
	b, _ := json.Marshal(s)

	if len(b) < 2 {
		return ""
	}

	return string(
		b[1 : len(b)-1],
	)
}

func itoa(v uint64) string {
	if v == 0 {
		return "0"
	}

	var b [20]byte
	i := len(b)

	for v > 0 {
		i--

		b[i] = byte('0' + v%10)
		v /= 10
	}

	return string(b[i:])
}

func (s *Server) idempotentGet(key string) string {
	s.idemMu.Lock()
	defer s.idemMu.Unlock()

	return s.idem[key]
}

func (s *Server) idempotentSet(
	key string,
	id string,
) {
	s.idemMu.Lock()
	defer s.idemMu.Unlock()

	s.idem[key] = id
}
