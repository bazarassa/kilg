package api

import (
	"net/http"
	"time"

	"github.com/kafka-llm-gateway/gateway/internal/protocol"
)

// handleModels implements GET /v1/models.
func (s *Server) handleModels(w http.ResponseWriter, r *http.Request) {
	now := time.Now().Unix()
	data := make([]protocol.ModelInfo, 0)
	seen := map[string]bool{}
	for _, id := range s.router.Models() {
		if id == "" || seen[id] {
			continue
		}
		seen[id] = true
		data = append(data, protocol.ModelInfo{
			ID:      id,
			Object:  "model",
			Created: now,
			OwnedBy: "gateway",
		})
	}
	writeJSON(w, http.StatusOK, protocol.ModelsList{Object: "list", Data: data})
}

// handleHealth implements GET /health and /v1/health.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleReady implements GET /ready.
func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

// handleMetrics implements GET /metrics (Prometheus).
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	// The Prometheus handler is mounted separately in main; this is a
	// fallback that reports the collector registry if wired.
	if s.metricsHandler != nil {
		s.metricsHandler.ServeHTTP(w, r)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "metrics not wired"})
}
