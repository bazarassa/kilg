// Command gateway runs the OpenAI-compatible LLM gateway.
package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/kafka-llm-gateway/gateway/internal/api"
	"github.com/kafka-llm-gateway/gateway/internal/config"
	"github.com/kafka-llm-gateway/gateway/internal/jobs"
	"github.com/kafka-llm-gateway/gateway/internal/kafka"
	"github.com/kafka-llm-gateway/gateway/internal/observability"
	"github.com/kafka-llm-gateway/gateway/internal/providers"
	"github.com/kafka-llm-gateway/gateway/internal/providers/heavy"
	"github.com/kafka-llm-gateway/gateway/internal/providers/litellm"
	"github.com/kafka-llm-gateway/gateway/internal/providers/ollama"
	"github.com/kafka-llm-gateway/gateway/internal/router"
	"github.com/kafka-llm-gateway/gateway/internal/streaming"
)

func main() {
	cfg := config.Load()
	log := observability.NewLogger(cfg.LogLevel)

	reg := prometheus.NewRegistry()
	metrics := observability.NewMetrics(reg)

	producer, err := kafka.NewProducerWithRewrite(cfg.KafkaBrokers, cfg.KafkaAddressRewrite)
	if err != nil {
		log.Error("kafka producer init failed", "err", err)
		os.Exit(1)
	}
	defer producer.Close()

	topics := kafka.Topics{
		Requests:  cfg.KafkaRequestTopic,
		Events:    cfg.KafkaEventsTopic,
		Completed: cfg.KafkaCompletedTopic,
		Failed:    cfg.KafkaFailedTopic,
		DLQ:       cfg.KafkaDLQTopic,
	}

	if cfg.KafkaAutoCreate {
		if err := ensureTopics(cfg, topics); err != nil {
			log.Warn("ensure topics failed (continuing)", "err", err)
		}
	}

	jm := jobs.NewManager(jobs.NewMemoryStore())
	dispatch := streaming.NewDispatcher(cfg.BufferSize)
	replay := kafka.NewReplayReader(cfg.KafkaBrokers, cfg.KafkaEventsTopic)
	replay.SetRewrite(cfg.KafkaAddressRewrite)
	rt := router.New(cfg)

	provs := map[string]providers.Provider{
		"heavy":   heavy.New(cfg),
		"litellm": litellm.New(cfg),
		"ollama":  ollama.New(cfg),
	}

	srv := api.NewServer(cfg, producer, topics, rt, jm, dispatch, replay, metrics, log, provs)
	srv.SetMetricsHandler(promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))

	httpServer := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	eventConsumer := kafka.NewConsumer(cfg.KafkaBrokers, cfg.KafkaConsumerGroup, cfg.KafkaEventsTopic, cfg.KafkaConsumerStart)
	eventConsumer.SetRewrite(cfg.KafkaAddressRewrite)
	consumerDone := make(chan error, 1)
	go func() { consumerDone <- srv.RunEventConsumer(ctx, eventConsumer) }()

	errCh := make(chan error, 1)
	go func() {
		log.Info("gateway listening", "addr", cfg.HTTPAddr)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
		log.Info("shutdown signal received")
	case err := <-errCh:
		log.Error("server error", "err", err)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_ = httpServer.Shutdown(shutdownCtx)
	<-consumerDone
	log.Info("gateway stopped")
}

func ensureTopics(cfg config.Config, topics kafka.Topics) error {
	return kafka.EnsureTopicsWithRewrite(context.Background(), cfg.KafkaBrokers, topics, cfg.KafkaPartitions, cfg.KafkaReplication, cfg.KafkaAddressRewrite)
}
