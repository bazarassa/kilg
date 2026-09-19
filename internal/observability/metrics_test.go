package observability

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestNewMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)
	
	if m == nil {
		t.Fatal("NewMetrics returned nil")
	}
	if m.RequestsTotal == nil {
		t.Error("RequestsTotal is nil")
	}
	if m.GenerationDur == nil {
		t.Error("GenerationDur is nil")
	}
}

func TestMetricsIncrement(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)
	
	// Should not panic
	m.RequestsTotal.WithLabelValues("heavy", "queued").Inc()
	m.RequestsActive.Inc()
	m.RequestsActive.Dec()
	m.GenerationDur.WithLabelValues("heavy").Observe(1.5)
}

