package api

import (
	"context"
	"net/http"

	"github.com/kafka-llm-gateway/gateway/internal/jobs"
	"github.com/kafka-llm-gateway/gateway/internal/protocol"
	"github.com/kafka-llm-gateway/gateway/internal/streaming"
)

// jobInfo converts a job to its API representation.
func jobInfo(j *jobs.Job) protocol.JobInfo {
	return protocol.JobInfo{
		ID:           j.ID,
		Object:       "chat.completion.job",
		Status:       j.Status,
		Provider:     j.Provider,
		Model:        j.Model,
		Sequence:     j.Sequence,
		CreatedAt:    j.CreatedAt.UTC().Format("2006-01-02T15:04:05Z"),
		UpdatedAt:    j.UpdatedAt.UTC().Format("2006-01-02T15:04:05Z"),
		FinishReason: j.FinishReason,
		Error:        j.Error,
	}
}

// handleJobsList implements GET /v1/jobs.
func (s *Server) handleJobsList(w http.ResponseWriter, r *http.Request) {
	list := s.jobs.Store().List()
	out := make([]protocol.JobInfo, 0, len(list))
	for _, j := range list {
		out = append(out, jobInfo(j))
	}
	writeJSON(w, http.StatusOK, map[string]any{"object": "list", "data": out})
}

// handleJob implements GET /v1/jobs/{id}.
func (s *Server) handleJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	j, ok := s.jobs.Store().Get(id)
	if !ok {
		// Try to recover state from the durable event log.
		if rec := s.recoverJob(r.Context(), id); rec != nil {
			j = rec
		} else {
			writeJSONError(w, http.StatusNotFound, "job not found")
			return
		}
	}
	writeJSON(w, http.StatusOK, jobInfo(j))
}

// handleJobEvents implements GET /v1/jobs/{id}/events (SSE replay).
func (s *Server) handleJobEvents(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeJSONError(w, http.StatusInternalServerError, "streaming unsupported")
		return
	}
	streaming.SSEHeaders(w)
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	if r.Header.Get("Last-Event-ID") != "" {
		s.metrics.Reconnects.Inc()
	}
	_, terminal, err := s.replayJobEvents(r.Context(), id, r.Header.Get("Last-Event-ID"), w, flusher)
	if err != nil {
		s.log.Warn("job events replay failed", "request_id", id, "err", err)
	}
	if !terminal {
		_ = streaming.WriteDone(w, flusher)
	}
}

// recoverJob rebuilds a job from the durable event log.
func (s *Server) recoverJob(ctx context.Context, id string) *jobs.Job {
	recs, err := s.replay.ReadAll(ctx, id)
	if err != nil || len(recs) == 0 {
		return nil
	}
	// Ensure the job exists, then apply each event.
	s.jobs.Ensure(id, "", "")
	for _, rec := range recs {
		var ev protocol.Event
		if err := jsonUnmarshal(rec.Value, &ev); err != nil {
			continue
		}
		s.jobs.Apply(ev)
	}
	j, ok := s.jobs.Store().Get(id)
	if !ok {
		return nil
	}
	return j
}
