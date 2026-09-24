// Package config loads gateway/worker configuration from environment
// variables with sensible defaults.
package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds all configuration for the gateway and worker processes.
type Config struct {
	// Gateway
	HTTPAddr       string
	WorkerHTTPAddr string
	APIKeys        []string // empty means auth disabled
	DefaultModel   string   // default logical route, for example "heavy" (litellm or ollama)

	// Kafka
	KafkaBrokers        []string
	KafkaRequestTopic   string
	KafkaEventsTopic    string
	KafkaCompletedTopic string
	KafkaFailedTopic    string
	KafkaDLQTopic       string
	KafkaConsumerGroup  string
	KafkaWorkerGroup    string
	KafkaAutoCreate     bool
	KafkaPartitions     int
	KafkaReplication    int
	KafkaConsumerStart  string // "latest" or "earliest"
	KafkaAddressRewrite string // comma-separated "advertised=actual" pairs

	// Heavy LLM
	HeavyBaseURL   string
	HeavyModel     string
	HeavyAPIKey    string
	HeavyTimeout   time.Duration // overall generation timeout
	HeavyIdle      time.Duration // idle (no chunk) timeout
	HeavyWorkers   int           // max concurrent heavy generations
	HeavyMaxRetry  int
	HeavyHeartbeat time.Duration

	// LiteLLM
	LiteLLMURL     string
	LiteLLMModel   string
	LiteLLMAPIKey  string
	LiteLLMTimeout time.Duration

	// Ollama
	OllamaURL        string
	OllamaModel      string
	OllamaEmbedModel string
	OllamaAPIKey     string
	OllamaTimeout    time.Duration

	// Misc
	ContextWindow int
	BufferSize    int // per-request in-memory SSE buffer
	LogLevel      string
}

// Default returns a Config populated with defaults.
func Default() Config {
	return Config{
		HTTPAddr:            ":8080",
		WorkerHTTPAddr:      ":8081",
		DefaultModel:        "heavy",
		KafkaBrokers:        []string{"REDACTED:9092"},
		KafkaRequestTopic:   "llm.requests",
		KafkaEventsTopic:    "llm.events",
		KafkaCompletedTopic: "llm.completed",
		KafkaFailedTopic:    "llm.failed",
		KafkaDLQTopic:       "llm.dlq",
		KafkaConsumerGroup:  "gateway",
		KafkaWorkerGroup:    "heavy-llm-worker",
		KafkaAutoCreate:     true,
		KafkaPartitions:     3,
		KafkaReplication:    1,
		KafkaConsumerStart:  "latest",
		KafkaAddressRewrite: "",

		HeavyBaseURL:   "https://deep.llm.net/v1",
		HeavyModel:     "HauhauCS/Qwen3.8-27B-Uncensored-HauhauCS-Aggressive-MTP-GGUF:Q6_K_P",
		HeavyTimeout:   2 * time.Hour,
		HeavyIdle:      10 * time.Minute,
		HeavyWorkers:   2,
		HeavyMaxRetry:  3,
		HeavyHeartbeat: 30 * time.Second,

		LiteLLMURL:     "http://fast.llm.net:8081/v1",
		LiteLLMModel:   "/models/models/gemma-4-E4B/gemma-4-E4B-it-ultra-uncensored-heretic-Q2_K_XL.gguf",
		LiteLLMTimeout: 5 * time.Minute,

		OllamaURL:        "http://ollama.llm.net:11434",
		OllamaModel:      "llama3.2:3b-instruct-q4_K_M",
		OllamaEmbedModel: "nomic-embed-text:latest",
		OllamaTimeout:    5 * time.Minute,

		ContextWindow: 8192,
		BufferSize:    1024,
		LogLevel:      "info",
	}
}

// Load reads configuration from the environment.
func Load() Config {
	c := Default()
	c.HTTPAddr = getEnv("HTTP_ADDR", c.HTTPAddr)
	c.DefaultModel = getEnv("DEFAULT_MODEL", c.DefaultModel)
	c.WorkerHTTPAddr = getEnv("WORKER_HTTP_ADDR", c.WorkerHTTPAddr)
	if keys := getEnv("API_KEYS", ""); keys != "" {
		for _, k := range strings.Split(keys, ",") {
			if k = strings.TrimSpace(k); k != "" {
				c.APIKeys = append(c.APIKeys, k)
			}
		}
	}

	c.KafkaBrokers = splitList(getEnv("KAFKA_BROKERS", strings.Join(c.KafkaBrokers, ",")))
	c.KafkaRequestTopic = getEnv("KAFKA_REQUEST_TOPIC", c.KafkaRequestTopic)
	c.KafkaEventsTopic = getEnv("KAFKA_EVENTS_TOPIC", c.KafkaEventsTopic)
	c.KafkaCompletedTopic = getEnv("KAFKA_COMPLETED_TOPIC", c.KafkaCompletedTopic)
	c.KafkaFailedTopic = getEnv("KAFKA_FAILED_TOPIC", c.KafkaFailedTopic)
	c.KafkaDLQTopic = getEnv("KAFKA_DLQ_TOPIC", c.KafkaDLQTopic)
	c.KafkaConsumerGroup = getEnv("KAFKA_CONSUMER_GROUP", c.KafkaConsumerGroup)
	c.KafkaWorkerGroup = getEnv("KAFKA_WORKER_GROUP", c.KafkaWorkerGroup)
	c.KafkaAutoCreate = getBool("KAFKA_AUTO_CREATE", c.KafkaAutoCreate)
	c.KafkaPartitions = getInt("KAFKA_PARTITIONS", c.KafkaPartitions)
	c.KafkaReplication = getInt("KAFKA_REPLICATION", c.KafkaReplication)
	c.KafkaConsumerStart = getEnv("KAFKA_CONSUMER_START", c.KafkaConsumerStart)
	c.KafkaAddressRewrite = getEnv("KAFKA_ADDRESS_REWRITE", c.KafkaAddressRewrite)

	c.HeavyBaseURL = getEnv("OPENAI_BASE_URL", c.HeavyBaseURL)
	c.HeavyModel = getEnv("OPENAI_MODEL", c.HeavyModel)
	c.HeavyAPIKey = getEnv("OPENAI_KEY", "REDACTED")
	c.HeavyTimeout = getDuration("HEAVY_LLM_TIMEOUT", c.HeavyTimeout)
	c.HeavyIdle = getDuration("HEAVY_LLM_IDLE_TIMEOUT", c.HeavyIdle)
	c.HeavyWorkers = getInt("HEAVY_WORKERS", c.HeavyWorkers)
	c.HeavyMaxRetry = getInt("HEAVY_MAX_RETRY", c.HeavyMaxRetry)
	c.HeavyHeartbeat = getDuration("HEAVY_HEARTBEAT", c.HeavyHeartbeat)

	c.LiteLLMURL = getEnv("LITELLM_URL", c.LiteLLMURL)
	c.LiteLLMModel = getEnv("LITELLM_MODEL", c.LiteLLMModel)
	c.LiteLLMAPIKey = getEnv("LITELLM_API_KEY", "")
	c.LiteLLMTimeout = getDuration("LITELLM_TIMEOUT", c.LiteLLMTimeout)

	c.OllamaURL = getEnv("OLLAMA_URL", c.OllamaURL)
	c.OllamaModel = getEnv("OLLAMA_MODEL", c.OllamaModel)
	c.OllamaEmbedModel = getEnv("OLLAMA_EMBED_MODEL", c.OllamaEmbedModel)
	c.OllamaAPIKey = getEnv("OLLAMA_API_KEY", "")
	c.OllamaTimeout = getDuration("OLLAMA_TIMEOUT", c.OllamaTimeout)

	c.ContextWindow = getInt("CONTEXT_WINDOW", c.ContextWindow)
	c.BufferSize = getInt("SSE_BUFFER_SIZE", c.BufferSize)
	c.LogLevel = getEnv("LOG_LEVEL", c.LogLevel)
	return c
}

func getEnv(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && strings.TrimSpace(v) != "" {
		return strings.TrimSpace(v)
	}
	return def
}

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func getInt(key string, def int) int {
	if v, ok := os.LookupEnv(key); ok {
		if n, err := strconv.Atoi(strings.TrimSpace(v)); err == nil {
			return n
		}
	}
	return def
}

func getBool(key string, def bool) bool {
	if v, ok := os.LookupEnv(key); ok {
		if b, err := strconv.ParseBool(strings.TrimSpace(v)); err == nil {
			return b
		}
	}
	return def
}

func getDuration(key string, def time.Duration) time.Duration {
	if v, ok := os.LookupEnv(key); ok {
		if d, err := time.ParseDuration(strings.TrimSpace(v)); err == nil {
			return d
		}
	}
	return def
}
