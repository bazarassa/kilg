package api

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/kafka-llm-gateway/gateway/internal/jobs"
	"github.com/kafka-llm-gateway/gateway/internal/protocol"
	"github.com/kafka-llm-gateway/gateway/internal/providers"
	"github.com/kafka-llm-gateway/gateway/internal/router"
)

// JSON-поля provider-ответов (OpenAI-compatible format).
const (
	fieldID               = "id"
	fieldModel            = "model"
	fieldCreated          = "created"
	fieldUsage            = "usage"
	fieldChoices          = "choices"
	fieldDelta            = "delta"
	fieldMessage          = "message"
	fieldContent          = "content"
	fieldFinishReason     = "finish_reason"
	fieldReasoningContent = "reasoning_content"

	objChatCompletion = "chat.completion"
	roleAssistant     = "assistant"
	finishReasonStop  = "stop"
)

// respondNonStreaming отвечает на запрос со stream:false ОДНИМ JSON-
// объектом chat.completion согласно OpenAI-спеке:
//
//   - Kafka-маршрут (heavy): дожидаемся terminal-события job и
//     собираем ответ из CompletedPayload;
//   - direct-маршрут (litellm/ollama): запускаем provider напрямую
//     и агрегируем ответ/чанки в итоговый объект.
func (s *Server) respondNonStreaming(
	w http.ResponseWriter,
	r *http.Request,
	requestID string,
	route router.Route,
	req protocol.ChatRequest,
	rawBody []byte,
	reused bool,
) {
	if route.Async {
		s.completeJobJSON(
			w,
			r,
			requestID,
			route,
			rawBody,
			reused,
		)
		return
	}

	s.completeDirectJSON(
		w,
		r,
		requestID,
		route,
		req,
		rawBody,
	)
}

// chatCompletion строит объект chat.completion по OpenAI-спеке
// (один JSON-ответ для stream:false).
func chatCompletion(
	id string,
	model string,
	created int64,
	content string,
	finishReason string,
	usage json.RawMessage,
) protocol.ChatResponse {
	if created == 0 {
		created = time.Now().Unix()
	}

	if finishReason == "" {
		finishReason = finishReasonStop
	}

	contentRaw := json.RawMessage("null")

	if content != "" {
		if b, err := json.Marshal(content); err == nil {
			contentRaw = b
		}
	}

	var usagePtr *protocol.Usage

	if len(usage) > 0 {
		var u protocol.Usage

		if json.Unmarshal(usage, &u) == nil {
			usagePtr = &u
		}
	}

	return protocol.ChatResponse{
		ID:      id,
		Object:  objChatCompletion,
		Created: created,
		Model:   model,
		Choices: []protocol.Choice{
			{
				Index: 0,
				Message: protocol.ChatMessage{
					Role:    roleAssistant,
					Content: contentRaw,
				},
				FinishReason: &finishReason,
			},
		},
		Usage: usagePtr,
	}
}

// isJobTerminal сообщает, что job достиг терминального статуса.
func isJobTerminal(status string) bool {
	return status == protocol.StatusCompleted ||
		status == protocol.StatusFailed ||
		status == protocol.StatusCancelled
}

// completeJobJSON отвечает на non-streaming запрос (stream:false)
// Kafka-маршрута ОДНИМ JSON-объектом chat.completion согласно
// OpenAI-спеке.
//
// Job при этом выполняется в streaming-режиме через Worker (raw
// чанки живут в llm.events); Gateway дожидается terminal-события
// и собирает из него итоговый объект.
func (s *Server) completeJobJSON(
	w http.ResponseWriter,
	r *http.Request,
	requestID string,
	route router.Route,
	rawBody []byte,
	reused bool,
) {
	// Регистрация ДО публикации llm.requests: Worker может успеть
	// опубликовать события раньше, чем клиент зарегистрирует stream.
	stream := s.dispatch.Register(requestID)
	defer s.dispatch.Unregister(requestID)

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

	// Fast path: idempotent-повтор после завершения job.
	if j, ok := s.jobs.Store().Get(requestID); ok &&
		isJobTerminal(j.Status) {

		s.writeJobTerminalJSON(w, requestID, route.Model, j)
		return
	}

	for {
		select {
		case <-r.Context().Done():
			s.metrics.Disconnects.Inc()
			return

		case ev := <-stream.Ch:
			if !isTerminal(ev.Type) {
				continue
			}

			s.writeTerminalEventJSON(w, requestID, route.Model, ev)
			return
		}
	}
}

// writeJobTerminalJSON отвечает по состоянию job из store.
func (s *Server) writeJobTerminalJSON(
	w http.ResponseWriter,
	requestID string,
	model string,
	j *jobs.Job,
) {
	switch j.Status {
	case protocol.StatusCompleted:
		created := time.Now().Unix()

		if !j.CompletedAt.IsZero() {
			created = j.CompletedAt.Unix()
		}

		writeJSON(
			w,
			http.StatusOK,
			chatCompletion(
				requestID,
				model,
				created,
				j.Content,
				j.FinishReason,
				j.Usage,
			),
		)

	case protocol.StatusFailed:
		msg := j.Error

		if msg == "" {
			msg = "job failed"
		}

		writeJSONError(w, http.StatusBadGateway, msg)

	default:
		writeJSONError(
			w,
			http.StatusBadGateway,
			"request cancelled",
		)
	}
}

// writeTerminalEventJSON отвечает по terminal-событию из llm.events.
func (s *Server) writeTerminalEventJSON(
	w http.ResponseWriter,
	requestID string,
	fallbackModel string,
	ev protocol.Event,
) {
	model := ev.Model

	if model == "" {
		model = fallbackModel
	}

	switch ev.Type {
	case protocol.TypeCompleted:
		var p protocol.CompletedPayload

		if err := json.Unmarshal(ev.Payload, &p); err != nil {
			writeJSONError(
				w,
				http.StatusInternalServerError,
				"decode completed event: "+err.Error(),
			)
			return
		}

		writeJSON(
			w,
			http.StatusOK,
			chatCompletion(
				requestID,
				model,
				ev.Timestamp.Unix(),
				p.Content,
				p.FinishReason,
				p.Usage,
			),
		)

	case protocol.TypeFailed:
		var p protocol.FailedPayload

		_ = json.Unmarshal(ev.Payload, &p)

		msg := p.Error

		if msg == "" {
			msg = "job failed"
		}

		writeJSONError(w, http.StatusBadGateway, msg)

	default:
		writeJSONError(
			w,
			http.StatusBadGateway,
			"request cancelled",
		)
	}
}

// completionAcc накапливает итог non-streaming completion из
// provider-событий: либо один объект chat.completion (non-streaming
// ответ), либо серию chat.completion.chunk (если provider стримит
// несмотря на stream:false).
type completionAcc struct {
	id        string
	model     string
	created   int64
	finish    string
	usage     json.RawMessage
	content   strings.Builder
	reasoning strings.Builder
}

func (a *completionAcc) absorb(raw string) {
	var obj map[string]any

	if err := json.Unmarshal([]byte(raw), &obj); err != nil {
		return
	}

	a.absorbMeta(obj)

	choices, _ := obj[fieldChoices].([]any)

	if len(choices) == 0 {
		return
	}

	ch, ok := choices[0].(map[string]any)

	if !ok {
		return
	}

	a.absorbChoice(ch)
}

func (a *completionAcc) absorbMeta(obj map[string]any) {
	if id, ok := obj[fieldID].(string); ok {
		a.id = id
	}

	if m, ok := obj[fieldModel].(string); ok {
		a.model = m
	}

	if c, ok := obj[fieldCreated].(float64); ok {
		a.created = int64(c)
	}

	if u, ok := obj[fieldUsage]; ok && u != nil {
		a.usage, _ = json.Marshal(u)
	}
}

func (a *completionAcc) absorbChoice(ch map[string]any) {
	if fr, ok := ch[fieldFinishReason].(string); ok && fr != "" {
		a.finish = fr
	}

	if delta, ok := ch[fieldDelta].(map[string]any); ok {
		a.appendDelta(delta)
		return
	}

	if msg, ok := ch[fieldMessage].(map[string]any); ok {
		a.appendDelta(msg)
	}
}

func (a *completionAcc) appendDelta(d map[string]any) {
	if s, ok := d[fieldContent].(string); ok {
		a.content.WriteString(s)
	}

	if s, ok := d[fieldReasoningContent].(string); ok {
		a.reasoning.WriteString(s)
	}
}

// completeDirectJSON отвечает на non-streaming запрос (stream:false)
// direct-маршрута (litellm/ollama) ОДНИМ JSON-объектом chat.completion
// согласно OpenAI-спеке.
func (s *Server) completeDirectJSON(
	w http.ResponseWriter,
	r *http.Request,
	requestID string,
	route router.Route,
	req protocol.ChatRequest,
	rawBody []byte,
) {
	prov, ok := s.providers[route.Provider]

	if !ok {
		writeJSONError(
			w,
			http.StatusBadGateway,
			"provider not configured: "+route.Provider,
		)
		return
	}

	s.jobs.Ensure(requestID, route.Provider, route.Model)

	s.jobs.Update(
		requestID,
		func(j *jobs.Job) {
			j.Status = protocol.StatusRunning
		},
	)

	s.metrics.RequestsTotal.
		WithLabelValues(route.Provider, "started").
		Inc()

	pReq := &providers.ChatRequest{
		Model:  route.Model,
		Stream: false,
		Extra:  rawBody,
	}

	for _, m := range req.Messages {
		content := string(m.Content)

		var str string

		if json.Unmarshal(m.Content, &str) == nil {
			content = str
		}

		pReq.Messages = append(
			pReq.Messages,
			providers.Message{Role: m.Role, Content: content},
		)
	}

	events := make(chan providers.ProviderEvent, 64)
	done := make(chan error, 1)

	go func() {
		done <- prov.Complete(r.Context(), pReq, events)
	}()

	acc := &completionAcc{}

	for ev := range events {
		if ev.Type == providers.EventRawSSE {
			acc.absorb(ev.Raw)
		}
	}

	// Complete() закрывает events; итоговую ошибку читаем после.
	if err := <-done; err != nil {
		s.metrics.RequestsFailed.
			WithLabelValues(route.Provider).
			Inc()

		s.jobs.Update(
			requestID,
			func(j *jobs.Job) {
				j.Status = protocol.StatusFailed
				j.Error = err.Error()
			},
		)

		writeJSONError(w, http.StatusBadGateway, err.Error())
		return
	}

	finish := acc.finish

	if finish == "" {
		finish = finishReasonStop
	}

	s.jobs.Update(
		requestID,
		func(j *jobs.Job) {
			j.Status = protocol.StatusCompleted
			j.Content = acc.content.String()
			j.Reasoning = acc.reasoning.String()
			j.ReasoningAvailable = acc.reasoning.Len() > 0
			j.FinishReason = finish
			j.Usage = acc.usage
		},
	)

	id := acc.id

	if id == "" {
		id = requestID
	}

	model := acc.model

	if model == "" {
		model = route.Model
	}

	writeJSON(
		w,
		http.StatusOK,
		chatCompletion(
			id,
			model,
			acc.created,
			acc.content.String(),
			finish,
			acc.usage,
		),
	)
}
