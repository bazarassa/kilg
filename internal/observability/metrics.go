// Package observability provides Prometheus metrics and structured logging.
package observability

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics holds all Prometheus collectors.
type Metrics struct {
	RequestsTotal   *prometheus.CounterVec
	RequestsActive  prometheus.Gauge
	RequestsFailed  *prometheus.CounterVec
	GenerationDur   *prometheus.HistogramVec
	QueueDur        *prometheus.HistogramVec
	TokensTotal     *prometheus.CounterVec
	ReasoningTokens prometheus.Counter
	OutputTokens    prometheus.Counter
	KafkaPublish    *prometheus.CounterVec
	KafkaPublishErr *prometheus.CounterVec
	ConsumerLag     *prometheus.GaugeVec
	Disconnects     prometheus.Counter
	Reconnects      prometheus.Counter
	ReplayedEvents  prometheus.Counter
}

// NewMetrics registers collectors on the given registry.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	f := promauto.With(reg)
	return &Metrics{
		RequestsTotal: f.NewCounterVec(prometheus.CounterOpts{
			Name: "llm_requests_total",
			Help: "Total LLM requests by provider and status.",
		}, []string{"provider", "status"}),
		RequestsActive: f.NewGauge(prometheus.GaugeOpts{
			Name: "llm_requests_active",
			Help: "Currently active LLM generations.",
		}),
		RequestsFailed: f.NewCounterVec(prometheus.CounterOpts{
			Name: "llm_requests_failed_total",
			Help: "Failed LLM requests by provider.",
		}, []string{"provider"}),
		GenerationDur: f.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "llm_generation_duration_seconds",
			Help:    "Generation duration in seconds.",
			Buckets: []float64{1, 5, 30, 60, 300, 600, 1800, 3600, 7200},
		}, []string{"provider"}),
		QueueDur: f.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "llm_queue_duration_seconds",
			Help:    "Time spent queued before generation started.",
			Buckets: []float64{0.01, 0.1, 1, 5, 30, 300, 3600},
		}, []string{"provider"}),
		TokensTotal: f.NewCounterVec(prometheus.CounterOpts{
			Name: "llm_tokens_total",
			Help: "Total tokens by type.",
		}, []string{"type"}),
		ReasoningTokens: f.NewCounter(prometheus.CounterOpts{
			Name: "llm_reasoning_tokens_total",
			Help: "Total reasoning tokens.",
		}),
		OutputTokens: f.NewCounter(prometheus.CounterOpts{
			Name: "llm_output_tokens_total",
			Help: "Total output tokens.",
		}),
		KafkaPublish: f.NewCounterVec(prometheus.CounterOpts{
			Name: "kafka_publish_total",
			Help: "Kafka publishes by topic and result.",
		}, []string{"topic", "result"}),
		KafkaPublishErr: f.NewCounterVec(prometheus.CounterOpts{
			Name: "kafka_publish_errors_total",
			Help: "Kafka publish errors by topic.",
		}, []string{"topic"}),
		ConsumerLag: f.NewGaugeVec(prometheus.GaugeOpts{
			Name: "kafka_consumer_lag",
			Help: "Consumer lag by topic and partition.",
		}, []string{"topic", "partition"}),
		Disconnects: f.NewCounter(prometheus.CounterOpts{
			Name: "llm_client_disconnects_total",
			Help: "Client SSE disconnects.",
		}),
		Reconnects: f.NewCounter(prometheus.CounterOpts{
			Name: "llm_reconnects_total",
			Help: "SSE reconnects using Last-Event-ID.",
		}),
		ReplayedEvents: f.NewCounter(prometheus.CounterOpts{
			Name: "llm_replayed_events_total",
			Help: "Events replayed from Kafka.",
		}),
	}
}
