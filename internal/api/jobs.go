package api

import (
	"context"
	"encoding/json"
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
//
// @Summary List jobs
// @Description Lists all chat completion jobs
// @Tags jobs
// @Produce json
// @Success 200 {object} map[string]any
// @Security BearerAuth
// @Router /jobs [get]
func (s *Server) handleJobsList(w http.ResponseWriter, r *http.Request) {
	list := s.jobs.Store().List()
	out := make([]protocol.JobInfo, 0, len(list))
	for _, j := range list {
		out = append(out, jobInfo(j))
	}
	writeJSON(w, http.StatusOK, map[string]any{"object": "list", "data": out})
}

// handleJob implements GET /v1/jobs/{id}.
//
// @Summary Get job by ID
// @Description Retrieves job status and metadata
// @Tags jobs
// @Produce json
// @Param id path string true "Job ID"
// @Success 200 {object} protocol.JobInfo
// @Failure 404 {object} map[string]any
// @Security BearerAuth
// @Router /jobs/{id} [get]
func (s *Server) handleJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	j, ok := s.jobs.Store().Get(id)
	if !ok {
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
//
// @Summary Stream job events
// @Description Streams all events for a job (supports resume via Last-Event-ID)
// @Tags jobs
// @Produce text/event-stream
// @Param id path string true "Job ID"
// @Param Last-Event-ID header string false "Resume from this event ID"
// @Success 200 {string} string "SSE stream"
// @Failure 404 {object} map[string]any
// @Security BearerAuth
// @Router /jobs/{id}/events [get]
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

// handleCancelJob implements DELETE /v1/jobs/{id}.
//
// @Summary Cancel job
// @Description Cancels a queued or running job
// @Tags jobs
// @Produce json
// @Param id path string true "Job ID"
// @Success 200 {object} map[string]string
// @Failure 404 {object} map[string]any
// @Failure 409 {object} map[string]any
// @Security BearerAuth
// @Router /jobs/{id} [delete]
func (s *Server) handleCancelJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	j, ok := s.jobs.Store().Get(id)
	if !ok {
		writeJSONError(w, http.StatusNotFound, "job not found")
		return
	}
	
	// Only cancel queued/running jobs
	if j.Status != protocol.StatusQueued && j.Status != protocol.StatusRunning {
		writeJSONError(w, http.StatusConflict, "job already completed or failed")
		return
	}

	s.jobs.Update(id, func(job *jobs.Job) {
		job.Status = protocol.StatusCancelled
	})

	// Publish cancellation event
	ev, err := protocol.NewEvent(id, j.Sequence+1, protocol.TypeCancelled, map[string]string{
		"reason": "user_cancelled",
	})
	if err == nil {
		ev.Provider = j.Provider
		ev.Model = j.Model
		if b, merr := json.Marshal(ev); merr == nil {
			_ = s.producer.Publish(r.Context(), s.topics.Events, id, b)
		}
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"id":     id,
		"object": "chat.completion.job",
		"status": protocol.StatusCancelled,
	})
}

// recoverJob rebuilds a job from the durable event log.
func (s *Server) recoverJob(ctx context.Context, id string) *jobs.Job {
	recs, err := s.replay.ReadAll(ctx, id)
	if err != nil || len(recs) == 0 {
		return nil
	}
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
