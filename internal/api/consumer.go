package api

import (
	"context"
	"encoding/json"

	"github.com/kafka-llm-gateway/gateway/internal/kafka"
	"github.com/kafka-llm-gateway/gateway/internal/protocol"
)

// RunEventConsumer consumes llm.events, dispatches them to live SSE streams
// and updates job state. This is what makes the gateway able to resume after
// a restart: it rebuilds state from the durable log.
func (s *Server) RunEventConsumer(ctx context.Context, consumer kafka.EventConsumer) error {
	log := s.log
	return consumer.Consume(ctx, func(rec kafka.Record) error {
		var ev protocol.Event
		if err := json.Unmarshal(rec.Value, &ev); err != nil {
			log.Warn("bad event record", "err", err)
			return nil
		}
		log.Debug("event consumed", "request_id", ev.RequestID, "type", ev.Type, "seq", ev.Sequence)
		// Update job state (idempotent, monotonic).
		s.jobs.Apply(ev)
		// Dispatch to any live stream.
		s.dispatch.Dispatch(ev)
		return nil
	})
}
