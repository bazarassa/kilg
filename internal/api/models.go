package api

import (
	"net/http"
	"time"

	"github.com/kafka-llm-gateway/gateway/internal/protocol"
)

// handleModels implements GET /v1/models.
//
// @Summary List models
// @Description Lists all available models across providers
// @Tags models
// @Produce json
// @Success 200 {object} protocol.ModelsList
// @Security BearerAuth
// @Router /models [get]
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

// handleGetModel implements GET /v1/models/{id}.
//
// @Summary Get model by ID
// @Description Retrieves information about a specific model
// @Tags models
// @Produce json
// @Param id path string true "Model ID"
// @Success 200 {object} protocol.ModelInfo
// @Failure 404 {object} map[string]any
// @Security BearerAuth
// @Router /models/{id} [get]
func (s *Server) handleGetModel(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	for _, mid := range s.router.Models() {
		if mid == id {
			writeJSON(w, http.StatusOK, protocol.ModelInfo{
				ID:      id,
				Object:  "model",
				Created: time.Now().Unix(),
				OwnedBy: "gateway",
			})
			return
		}
	}
	writeJSONError(w, http.StatusNotFound, "model not found")
}

// handleHealth implements GET /health and /v1/health.
//
// @Summary Health check
// @Description Returns service health status
// @Tags system
// @Produce json
// @Success 200 {object} map[string]string
// @Router /health [get]
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleReady implements GET /ready.
//
// @Summary Readiness check
// @Description Returns service readiness status
// @Tags system
// @Produce json
// @Success 200 {object} map[string]string
// @Router /ready [get]
func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

// handleMetrics implements GET /metrics (Prometheus).
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if s.metricsHandler != nil {
		s.metricsHandler.ServeHTTP(w, r)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "metrics not wired"})
}
