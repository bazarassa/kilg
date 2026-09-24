// Command worker runs the heavy LLM worker.
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

	"github.com/kafka-llm-gateway/gateway/internal/config"
	"github.com/kafka-llm-gateway/gateway/internal/kafka"
	"github.com/kafka-llm-gateway/gateway/internal/observability"
	"github.com/kafka-llm-gateway/gateway/internal/providers/heavy"
	"github.com/kafka-llm-gateway/gateway/internal/worker"
)

func main() {
	cfg := config.Load()

	log := observability.NewLogger(
		cfg.LogLevel,
	)

	reg := prometheus.NewRegistry()
	metrics := observability.NewMetrics(reg)

	producer, err := kafka.NewProducerWithRewrite(
		cfg.KafkaBrokers,
		cfg.KafkaAddressRewrite,
	)
	if err != nil {
		log.Error(
			"kafka producer init failed",
			"err", err,
		)
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
		if err := ensureTopics(
			cfg,
			topics,
		); err != nil {
			// Не останавливаем worker:
			//
			// production Kafka может запретить auto-create/admin API,
			// хотя сами topics уже существуют.
			log.Warn(
				"ensure topics failed (continuing)",
				"err", err,
			)
		}
	}

	// Heavy provider отвечает за HTTP communication с LLM.
	provider := heavy.New(cfg)

	w := worker.New(
		producer,
		provider,
		topics,
		metrics,
		log,
		cfg.HeavyWorkers,
		cfg.HeavyMaxRetry,
		cfg.HeavyHeartbeat,
	)

	// Worker health/metrics HTTP server.
	mux := http.NewServeMux()

	mux.Handle(
		"/metrics",
		promhttp.HandlerFor(
			reg,
			promhttp.HandlerOpts{},
		),
	)

	mux.HandleFunc(
		"/health",
		func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
		},
	)

	httpServer := &http.Server{
		Addr: cfg.WorkerHTTPAddr,
		Handler: mux,

		ReadHeaderTimeout: 10 * time.Second,
	}

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	// Kafka consumer group worker.
	//
	// Все replicas worker используют один group:
	//
	//     heavy-llm-worker
	//
	// Поэтому Kafka сама распределяет partitions.
	consumer := kafka.NewConsumer(
		cfg.KafkaBrokers,
		cfg.KafkaWorkerGroup,
		cfg.KafkaRequestTopic,
		cfg.KafkaConsumerStart,
	)

	consumer.SetRewrite(
		cfg.KafkaAddressRewrite,
	)

	go func() {
		log.Info(
			"worker HTTP server listening",
			"addr", cfg.WorkerHTTPAddr,
		)

		if err := httpServer.ListenAndServe(); err != nil &&
			!errors.Is(err, http.ErrServerClosed) {

			log.Error(
				"worker HTTP server stopped",
				"err", err,
			)

			stop()
		}
	}()

	log.Info(
		"worker starting",
		"group", cfg.KafkaWorkerGroup,
		"topic", cfg.KafkaRequestTopic,
		"concurrency", cfg.HeavyWorkers,
	)

	// Run блокирует до shutdown или фатальной ошибки consumer.
	runErr := w.Run(
		ctx,
		consumer,
	)

	shutdownCtx, cancel := context.WithTimeout(
		context.Background(),
		30*time.Second,
	)
	defer cancel()

	_ = httpServer.Shutdown(
		shutdownCtx,
	)

	if runErr != nil &&
		ctx.Err() == nil {

		log.Error(
			"worker stopped with error",
			"err", runErr,
		)

		os.Exit(1)
	}

	log.Info("worker stopped")
}

func ensureTopics(
	cfg config.Config,
	topics kafka.Topics,
) error {
	return kafka.EnsureTopicsWithRewrite(
		context.Background(),
		cfg.KafkaBrokers,
		topics,
		cfg.KafkaPartitions,
		cfg.KafkaReplication,
		cfg.KafkaAddressRewrite,
	)
}
