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
func (s *Server) RunEventConsumer(
	ctx context.Context,
	consumer kafka.EventConsumer,
) error {
	log := s.log

	return consumer.Consume(ctx, func(
		callbackCtx context.Context,
		rec kafka.Record,
	) error {
		var ev protocol.Event

		if err := json.Unmarshal(rec.Value, &ev); err != nil {
			log.Warn(
				"bad event record",
				"err", err,
			)
			return nil
		}

		log.Debug(
			"event consumed",
			"request_id", ev.RequestID,
			"type", ev.Type,
			"seq", ev.Sequence,
		)

		// Keep the callback context available for future context-aware
		// processing. The current operations are synchronous and do not
		// require it yet.
		_ = callbackCtx

		// Update job state. Apply must remain idempotent and monotonic.
		s.jobs.Apply(ev)

		// Dispatch to any live stream.
		s.dispatch.Dispatch(ev)

		return nil
	})
}
