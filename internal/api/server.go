// Package api implements the OpenAI-compatible HTTP gateway.
package api

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/kafka-llm-gateway/gateway/internal/config"
	"github.com/kafka-llm-gateway/gateway/internal/jobs"
	"github.com/kafka-llm-gateway/gateway/internal/kafka"
	"github.com/kafka-llm-gateway/gateway/internal/observability"
	"github.com/kafka-llm-gateway/gateway/internal/providers"
	"github.com/kafka-llm-gateway/gateway/internal/router"
	"github.com/kafka-llm-gateway/gateway/internal/streaming"
)

// Server is the HTTP gateway.
type Server struct {
	cfg      config.Config
	producer kafka.EventProducer
	topics   kafka.Topics
	router   *router.Router
	jobs     *jobs.Manager
	dispatch *streaming.Dispatcher
	replay   kafka.Reader
	metrics  *observability.Metrics
	log      *slog.Logger

	// Direct providers for non-async routes.
	providers map[string]providers.Provider

	// Idempotency key -> request id.
	idemMu sync.Mutex
	idem   map[string]string

	// metricsHandler serves /metrics (Prometheus).
	metricsHandler http.Handler
}

// NewServer builds the gateway server.
func NewServer(
	cfg config.Config,
	producer kafka.EventProducer,
	topics kafka.Topics,
	rt *router.Router,
	jm *jobs.Manager,
	dispatch *streaming.Dispatcher,
	replay kafka.Reader,
	metrics *observability.Metrics,
	log *slog.Logger,
	provs map[string]providers.Provider,
) *Server {
	return &Server{
		cfg:       cfg,
		producer:  producer,
		topics:    topics,
		router:    rt,
		jobs:      jm,
		dispatch:  dispatch,
		replay:    replay,
		metrics:   metrics,
		log:       log,
		providers: provs,
		idem:      map[string]string{},
	}
}

// SetMetricsHandler wires the Prometheus handler for /metrics.
func (s *Server) SetMetricsHandler(h http.Handler) { s.metricsHandler = h }

// Handler builds the HTTP mux.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/chat/completions", s.handleChat)
	mux.HandleFunc("GET /v1/models", s.handleModels)
	mux.HandleFunc("GET /v1/health", s.handleHealth)
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("GET /ready", s.handleReady)
	mux.HandleFunc("GET /v1/jobs", s.handleJobsList)
	mux.HandleFunc("GET /v1/jobs/{id}", s.handleJob)
	mux.HandleFunc("GET /v1/jobs/{id}/events", s.handleJobEvents)
	mux.HandleFunc("POST /v1/embeddings", s.handleEmbeddings)
	mux.HandleFunc("GET /metrics", s.handleMetrics)
	return withAuth(s.cfg, withLogging(s.log, mux))
}

// withLogging adds request logging.
func withLogging(log *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		log.Debug("http", "method", r.Method, "path", r.URL.Path, "dur", time.Since(start).String())
	})
}

// withAuth enforces Bearer auth when API keys are configured.
func withAuth(cfg config.Config, next http.Handler) http.Handler {
	if len(cfg.APIKeys) == 0 {
		return next
	}
	allowed := map[string]struct{}{}
	for _, k := range cfg.APIKeys {
		allowed[k] = struct{}{}
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Health/metrics endpoints are open.
		if r.URL.Path == "/health" || r.URL.Path == "/ready" || r.URL.Path == "/metrics" || r.URL.Path == "/v1/health" {
			next.ServeHTTP(w, r)
			return
		}
		auth := r.Header.Get("Authorization")
		token := strings.TrimPrefix(auth, "Bearer ")
		if _, ok := allowed[token]; !ok {
			writeJSONError(w, http.StatusUnauthorized, "invalid api key")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeJSONError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]any{
		"error": map[string]any{"message": msg, "type": "invalid_request_error", "code": code},
	})
}
