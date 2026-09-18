package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/kafka-llm-gateway/gateway/internal/protocol"
	"github.com/kafka-llm-gateway/gateway/internal/streaming"
)

// replayFrom reads events with sequence > afterSeq and writes them.
// It returns the highest sequence written (0 if none) and whether a terminal
// event was written (and [DONE] emitted).
func (s *Server) replayFrom(ctx context.Context, requestID, afterSeq string, w http.ResponseWriter, flusher http.Flusher) (uint64, bool, error) {
	var after uint64
	if afterSeq != "" {
		after = parseSequence(afterSeq)
	}
	recs, err := s.replay.ReadAll(ctx, requestID)
	if err != nil {
		return after, false, err
	}
	last := after
	for _, rec := range recs {
		var ev protocol.Event
		if json.Unmarshal(rec.Value, &ev) != nil {
			continue
		}
		if ev.Sequence <= after {
			continue
		}
		s.metrics.ReplayedEvents.Inc()
		if err := s.writeEvent(w, flusher, ev); err != nil {
			return last, false, err
		}
		if ev.Sequence > last {
			last = ev.Sequence
		}
		if isTerminal(ev.Type) {
			_ = streaming.WriteDone(w, flusher)
			return last, true, nil
		}
	}
	return last, false, nil
}

// replayJobEvents serves GET /v1/jobs/{id}/events with Last-Event-ID support.
func (s *Server) replayJobEvents(ctx context.Context, requestID, afterSeq string, w http.ResponseWriter, flusher http.Flusher) (uint64, bool, error) {
	return s.replayFrom(ctx, requestID, afterSeq, w, flusher)
}

// parseSequence extracts the sequence number from a Last-Event-ID value.
// The event id format is "<request_id>-<sequence>" (sequence zero-padded to
// 10 digits); a bare number is also accepted.
func parseSequence(s string) uint64 {
	s = strings.TrimSpace(s)
	if i := strings.LastIndex(s, "-"); i >= 0 {
		s = s[i+1:]
	}
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0
	}
	return n
}
