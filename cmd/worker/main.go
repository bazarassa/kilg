// Command worker runs the heavy LLM worker.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http"
	"time"

	"github.com/kafka-llm-gateway/gateway/internal/config"
	"github.com/kafka-llm-gateway/gateway/internal/kafka"
	"github.com/kafka-llm-gateway/gateway/internal/observability"
	"github.com/kafka-llm-gateway/gateway/internal/providers/heavy"
	"github.com/kafka-llm-gateway/gateway/internal/worker"
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

	provider := heavy.New(cfg)
	w := worker.New(producer, provider, topics, metrics, log, cfg.HeavyWorkers, cfg.HeavyMaxRetry, cfg.HeavyHeartbeat)

	// Metrics endpoint for the worker.
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	})
	httpServer := &http.Server{Addr: cfg.WorkerHTTPAddr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	consumer := kafka.NewConsumer(cfg.KafkaBrokers, cfg.KafkaWorkerGroup, cfg.KafkaRequestTopic, cfg.KafkaConsumerStart)
	consumer.SetRewrite(cfg.KafkaAddressRewrite)

	errCh := make(chan error, 1)
	go func() {
		log.Info("worker listening", "addr", cfg.WorkerHTTPAddr)
		if err := httpServer.ListenAndServe(); err != nil {
			_ = err
		}
	}()

	log.Info("worker starting", "group", cfg.KafkaWorkerGroup, "topic", cfg.KafkaRequestTopic, "concurrency", cfg.HeavyWorkers)
	runErr := w.Run(ctx, consumer)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_ = httpServer.Shutdown(shutdownCtx)

	if runErr != nil && ctx.Err() == nil {
		log.Error("worker stopped with error", "err", runErr)
		os.Exit(1)
	}
	log.Info("worker stopped")
	_ = errCh
}

func ensureTopics(cfg config.Config, topics kafka.Topics) error {
	return kafka.EnsureTopicsWithRewrite(context.Background(), cfg.KafkaBrokers, topics, cfg.KafkaPartitions, cfg.KafkaReplication, cfg.KafkaAddressRewrite)
}
