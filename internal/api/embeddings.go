package api

import (
	"encoding/json"
	"net/http"

	"github.com/kafka-llm-gateway/gateway/internal/providers/ollama"
)

// handleEmbeddings implements POST /v1/embeddings.
func (s *Server) handleEmbeddings(w http.ResponseWriter, r *http.Request) {
	prov, ok := s.providers["ollama"]
	if !ok {
		writeJSONError(w, http.StatusBadGateway, "ollama provider not configured")
		return
	}
	oc, ok := prov.(*ollama.Client)
	if !ok {
		writeJSONError(w, http.StatusBadGateway, "ollama provider not available")
		return
	}

	var req struct {
		Model   string   `json:"model"`
		Input   json.RawMessage `json:"input"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}

	var inputs []string
	if err := json.Unmarshal(req.Input, &inputs); err != nil {
		var single string
		if err2 := json.Unmarshal(req.Input, &single); err2 != nil {
			writeJSONError(w, http.StatusBadRequest, "input must be a string or array of strings")
			return
		}
		inputs = []string{single}
	}
	if len(inputs) == 0 {
		writeJSONError(w, http.StatusBadRequest, "input is required")
		return
	}

	resp, err := oc.Embed(r.Context(), req.Model, inputs)
	if err != nil {
		writeJSONError(w, http.StatusBadGateway, "embedding failed: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, resp)
}
