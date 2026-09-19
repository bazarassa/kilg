package config

import (
	"os"
	"testing"
	"time"
)

func TestDefault(t *testing.T) {
	cfg := Default()

	if cfg.HTTPAddr != ":8080" {
		t.Errorf("HTTPAddr = %q", cfg.HTTPAddr)
	}
	if cfg.HeavyWorkers != 2 {
		t.Errorf("HeavyWorkers = %d", cfg.HeavyWorkers)
	}
	if cfg.BufferSize != 1024 {
		t.Errorf("BufferSize = %d", cfg.BufferSize)
	}
}

func TestLoadFromEnv(t *testing.T) {
	// Save and restore env
	oldAddr := os.Getenv("HTTP_ADDR")
	oldKeys := os.Getenv("API_KEYS")
	defer func() {
		os.Setenv("HTTP_ADDR", oldAddr)
		os.Setenv("API_KEYS", oldKeys)
	}()

	os.Setenv("HTTP_ADDR", ":9090")
	os.Setenv("API_KEYS", "key1,key2,key3")

	cfg := Load()

	if cfg.HTTPAddr != ":9090" {
		t.Errorf("HTTPAddr = %q, want :9090", cfg.HTTPAddr)
	}
	if len(cfg.APIKeys) != 3 {
		t.Errorf("APIKeys length = %d, want 3", len(cfg.APIKeys))
	}
	if cfg.APIKeys[0] != "key1" {
		t.Errorf("APIKeys[0] = %q", cfg.APIKeys[0])
	}
}

func TestLoadDurations(t *testing.T) {
	old := os.Getenv("HEAVY_LLM_TIMEOUT")
	defer os.Setenv("HEAVY_LLM_TIMEOUT", old)

	os.Setenv("HEAVY_LLM_TIMEOUT", "30m")
	cfg := Load()

	if cfg.HeavyTimeout != 30*time.Minute {
		t.Errorf("HeavyTimeout = %v, want 30m", cfg.HeavyTimeout)
	}
}

func TestLoadIntegers(t *testing.T) {
	old := os.Getenv("HEAVY_WORKERS")
	defer os.Setenv("HEAVY_WORKERS", old)

	os.Setenv("HEAVY_WORKERS", "10")
	cfg := Load()

	if cfg.HeavyWorkers != 10 {
		t.Errorf("HeavyWorkers = %d, want 10", cfg.HeavyWorkers)
	}
}

func TestLoadBooleans(t *testing.T) {
	old := os.Getenv("KAFKA_AUTO_CREATE")
	defer os.Setenv("KAFKA_AUTO_CREATE", old)

	os.Setenv("KAFKA_AUTO_CREATE", "false")
	cfg := Load()

	if cfg.KafkaAutoCreate != false {
		t.Errorf("KafkaAutoCreate = %v, want false", cfg.KafkaAutoCreate)
	}
}
