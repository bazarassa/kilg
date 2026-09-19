package observability

import (
	"testing"
)

func TestNewLogger(t *testing.T) {
	tests := []string{"debug", "info", "warn", "error", "invalid"}
	
	for _, level := range tests {
		log := NewLogger(level)
		if log == nil {
			t.Errorf("NewLogger(%q) returned nil", level)
		}
	}
}
