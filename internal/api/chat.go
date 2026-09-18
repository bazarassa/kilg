package api

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/kafka-llm-gateway/gateway/internal/jobs"
	"github.com/kafka-llm-gateway/gateway/internal/protocol"
	"github.com/kafka-llm-gateway/gateway/internal/providers"
	"github.com/kafka-llm-gateway/gateway/internal/router"
	"github.com/kafka-llm-gateway/gateway/internal/streaming"
)

// handleChat implements POST /v1/chat/completions.
func (s *Server) handleChat(w http.ResponseWriter, r *http.Request) {
	var req protocol.ChatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	if len(req.Messages) == 0 {
		writeJSONError(w, http.StatusBadRequest, "messages is required")
		return
	}
	if req.Model == "" {
		writeJSONError(w, http.StatusBadRequest, "model is required")
		return
	}

	route := s.router.Route(req.Model)
	requestID := NewULID()
	reused := false

	// Idempotency: reuse an existing job for the same key.
	if key := r.Header.Get("Idempotency-Key"); key != "" {
		if existing := s.idempotentGet(key); existing != "" {
			requestID = existing
			reused = true
		} else {
			s.idempotentSet(key, requestID)
		}
	}

	rawBody, _ := json.Marshal(req)

	// Async mode: return immediately with a job handle.
	if isAsync(r) {
		if !reused {
			if err := s.enqueue(r.Context(), requestID, route, rawBody, keyOf(r)); err != nil {
				writeJSONError(w, http.StatusServiceUnavailable, "failed to enqueue: "+err.Error())
				return
			}
		}
		status := protocol.StatusQueued
		if j, ok := s.jobs.Store().Get(requestID); ok {
			status = j.Status
		}
		writeJSON(w, http.StatusAccepted, protocol.JobAccepted{
			ID:     requestID,
			Object: "chat.completion.job",
			Status: status,
		})
		return
	}

	// Non-async heavy route: enqueue (durable) then stream the response.
	// Register the stream BEFORE enqueuing so no early event (e.g. "started")
	// is lost to the dispatcher.
	if route.Async {
		stream := s.dispatch.Register(requestID)
		if !reused {
			if err := s.enqueue(r.Context(), requestID, route, rawBody, keyOf(r)); err != nil {
				s.dispatch.Unregister(requestID)
				writeJSONError(w, http.StatusServiceUnavailable, "failed to enqueue: "+err.Error())
				return
			}
		}
		s.streamJobWith(w, r, requestID, stream)
		return
	}

	// Direct provider route (litellm / ollama).
	s.streamDirect(w, r, requestID, route, req, rawBody)
}

func keyOf(r *http.Request) string { return r.Header.Get("Idempotency-Key") }

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
	for _, c := range s {
		if c == ',' {
			out = append(out, trimSpace(cur))
			cur = ""
			continue
		}
		cur += string(c)
	}
	if cur != "" {
		out = append(out, trimSpace(cur))
	}
	return out
}

func trimSpace(s string) string {
	start, end := 0, len(s)
	for start < end && (s[start] == ' ' || s[start] == '\t') {
		start++
	}
	for end > start && (s[end-1] == ' ' || s[end-1] == '\t') {
		end--
	}
	return s[start:end]
}

// enqueue publishes the job to Kafka and records it in the job store.
func (s *Server) enqueue(ctx context.Context, requestID string, route router.Route, rawBody []byte, idemKey string) error {
	now := time.Now().UTC()
	s.jobs.Ensure(requestID, route.Provider, route.Model)
	s.jobs.Update(requestID, func(j *jobs.Job) { j.Status = protocol.StatusQueued })

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
	if err := s.producer.Publish(ctx, s.topics.Requests, requestID, b); err != nil {
		return err
	}

	// Publish the queued event so the event log is complete from the start.
	ev, err := protocol.NewEvent(requestID, 0, protocol.TypeQueued, map[string]string{
		"provider": route.Provider,
		"model":    route.Model,
	})
	if err == nil {
		ev.Provider = route.Provider
		ev.Model = route.Model
		if eb, merr := json.Marshal(ev); merr == nil {
			_ = s.producer.Publish(ctx, s.topics.Events, requestID, eb)
		}
	}
	s.metrics.RequestsTotal.WithLabelValues(route.Provider, "queued").Inc()
	s.log.Info("job enqueued", "request_id", requestID, "provider", route.Provider, "model", route.Model)
	return nil
}

// streamJob streams a Kafka-backed job to the client as SSE, registering a
// fresh stream.
func (s *Server) streamJob(w http.ResponseWriter, r *http.Request, requestID string) {
	stream := s.dispatch.Register(requestID)
	s.streamJobWith(w, r, requestID, stream)
}

// streamJobWith streams using an already-registered stream.
func (s *Server) streamJobWith(w http.ResponseWriter, r *http.Request, requestID string, stream *streaming.Stream) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		s.dispatch.Unregister(requestID)
		writeJSONError(w, http.StatusInternalServerError, "streaming unsupported")
		return
	}
	streaming.SSEHeaders(w)
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	defer s.dispatch.Unregister(requestID)

	// Only replay when the client reconnects with a Last-Event-ID. A fresh
	// request starts from the live stream (the worker publishes a "started"
	// event immediately, so nothing is lost).
	lastSent := uint64(0)
	if lastID := r.Header.Get("Last-Event-ID"); lastID != "" {
		s.metrics.Reconnects.Inc()
		var terminal bool
		var err error
		lastSent, terminal, err = s.replayFrom(r.Context(), requestID, lastID, w, flusher)
		if err != nil {
			s.log.Warn("replay failed", "request_id", requestID, "err", err)
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
			// Skip events already delivered by the replay above.
			if ev.Sequence <= lastSent {
				continue
			}
			if err := s.writeEvent(w, flusher, ev); err != nil {
				s.metrics.Disconnects.Inc()
				return
			}
			lastSent = ev.Sequence
			if isTerminal(ev.Type) {
				_ = streaming.WriteDone(w, flusher)
				return
			}
		}
	}
}

// streamDirect runs a direct provider and streams raw SSE to the client.
func (s *Server) streamDirect(w http.ResponseWriter, r *http.Request, requestID string, route router.Route, req protocol.ChatRequest, rawBody []byte) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeJSONError(w, http.StatusInternalServerError, "streaming unsupported")
		return
	}
	prov, ok := s.providers[route.Provider]
	if !ok {
		writeJSONError(w, http.StatusBadGateway, "provider not configured: "+route.Provider)
		return
	}

	streaming.SSEHeaders(w)
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	s.jobs.Ensure(requestID, route.Provider, route.Model)
	s.jobs.Update(requestID, func(j *jobs.Job) { j.Status = protocol.StatusRunning })
	s.metrics.RequestsTotal.WithLabelValues(route.Provider, "started").Inc()

	pReq := &providers.ChatRequest{Model: route.Model, Stream: true, Extra: rawBody}
	for _, m := range req.Messages {
		content := string(m.Content)
		var str string
		if json.Unmarshal(m.Content, &str) == nil {
			content = str
		}
		pReq.Messages = append(pReq.Messages, providers.Message{Role: m.Role, Content: content})
	}

	events := make(chan providers.ProviderEvent, 64)
	done := make(chan error, 1)
	go func() { done <- prov.Complete(r.Context(), pReq, events) }()

	for ev := range events {
		switch ev.Type {
		case providers.EventRawSSE:
			if err := streaming.WriteEvent(w, flusher, requestID, ev.Raw); err != nil {
				s.metrics.Disconnects.Inc()
				return
			}
		case providers.EventDone:
			s.jobs.Update(requestID, func(j *jobs.Job) { j.Status = protocol.StatusCompleted })
			_ = streaming.WriteDone(w, flusher)
			return
		case providers.EventError:
			s.metrics.RequestsFailed.WithLabelValues(route.Provider).Inc()
			s.jobs.Update(requestID, func(j *jobs.Job) {
				j.Status = protocol.StatusFailed
				j.Error = ev.Err.Error()
			})
			_ = streaming.WriteEvent(w, flusher, requestID,
				`{"error":{"message":"`+jsonEscape(ev.Err.Error())+`"}}`)
			_ = streaming.WriteDone(w, flusher)
			return
		}
	}
	if err := <-done; err != nil {
		s.log.Warn("direct provider error", "request_id", requestID, "err", err)
	}
}

// writeEvent writes one protocol event to the client as SSE.
func (s *Server) writeEvent(w http.ResponseWriter, flusher http.Flusher, ev protocol.Event) error {
	switch ev.Type {
	case protocol.TypeRawSSE:
		var p protocol.RawSSEPayload
		if json.Unmarshal(ev.Payload, &p) == nil {
			return streaming.WriteEvent(w, flusher, ev.EventID, p.Data)
		}
		return nil
	case protocol.TypeCompleted, protocol.TypeFailed, protocol.TypeCancelled:
		return streaming.WriteEvent(w, flusher, ev.EventID, terminalChunk(ev))
	default:
		return streaming.WriteEvent(w, flusher, ev.EventID, metaChunk(ev))
	}
}

func isTerminal(t string) bool {
	return t == protocol.TypeCompleted || t == protocol.TypeFailed || t == protocol.TypeCancelled
}

func terminalChunk(ev protocol.Event) string {
	if ev.Type == protocol.TypeFailed {
		var p protocol.FailedPayload
		_ = json.Unmarshal(ev.Payload, &p)
		return `{"id":"` + ev.RequestID + `","object":"chat.completion.chunk","error":{"message":"` + jsonEscape(p.Error) + `"}}`
	}
	var p protocol.CompletedPayload
	_ = json.Unmarshal(ev.Payload, &p)
	fr := p.FinishReason
	if fr == "" {
		fr = "stop"
	}
	return `{"id":"` + ev.RequestID + `","object":"chat.completion.chunk","choices":[{"index":0,"delta":{},"finish_reason":"` + fr + `"}]}`
}

func metaChunk(ev protocol.Event) string {
	return `{"id":"` + ev.RequestID + `","object":"chat.completion.chunk","_meta":{"type":"` + ev.Type + `","sequence":` + itoa(ev.Sequence) + `}}`
}

func jsonEscape(s string) string {
	b, _ := json.Marshal(s)
	return string(b[1 : len(b)-1])
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

func (s *Server) idempotentSet(key, id string) {
	s.idemMu.Lock()
	defer s.idemMu.Unlock()
	s.idem[key] = id
}
