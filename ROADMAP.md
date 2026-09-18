# ROADMAP.md

# LLM Gateway

## Сделано

### Архитектура

* [x] Определена архитектура Go LLM Gateway.
* [x] Определено разделение Gateway / Kafka / Worker / LLM Provider.
* [x] Определено разделение HTTP lifecycle и LLM generation lifecycle.
* [x] Kafka определена как durable buffer и event log для heavy LLM.
* [x] Определена модель `request_id` для корреляции полного жизненного цикла запроса.
* [x] Определена модель `sequence` для сохранения порядка streaming events.
* [x] Определена стратегия `at-least-once delivery`.
* [x] Определено сохранение raw provider payload.
* [x] Определено сохранение reasoning events без фильтрации.
* [x] Определён механизм replay после отключения клиента.
* [x] Определён async API для запросов, превышающих HTTP timeout.
* [x] Определено разделение Fast Path и Durable Path.

### LLM Providers

* [x] Определён Heavy LLM provider через OpenAI-compatible API.
* [x] Определён LiteLLM provider для быстрых instruct-запросов.
* [x] Определён Ollama provider для простых запросов.
* [x] Определён Ollama provider для embeddings.
* [x] Определена модель `nomic-embed-text:latest` для embeddings.
* [x] Определён deterministic routing по имени модели.

### Kafka

* [x] Определён Kafka broker `REDACTED:9092`.
* [x] Определён topic `llm.requests`.
* [x] Определён topic `llm.events`.
* [x] Определён topic `llm.completed`.
* [x] Определён topic `llm.failed`.
* [x] Определён topic `llm.dlq`.
* [x] Определено использование `request_id` как Kafka message key.
* [x] Определена необходимость `acks=all`.
* [x] Определено использование idempotent Kafka producer.

---

## В работе

### 1. Go Project Foundation

* [ ] Создать Go module.
* [ ] Создать `cmd/gateway`.
* [ ] Создать `cmd/worker`.
* [ ] Создать базовую структуру `internal/`.
* [ ] Реализовать configuration layer.
* [ ] Реализовать environment-based configuration.
* [ ] Реализовать `log/slog`.
* [ ] Реализовать graceful shutdown.
* [ ] Реализовать `/health`.
* [ ] Реализовать `/ready`.
* [ ] Добавить `go test ./...`.
* [ ] Добавить `go vet ./...`.

### 2. Domain Model

* [ ] Реализовать `ChatRequest`.
* [ ] Реализовать `ChatResponse`.
* [ ] Реализовать `ChatCompletionChunk`.
* [ ] Реализовать `LLMJob`.
* [ ] Реализовать `LLMEvent`.
* [ ] Реализовать `JobStatus`.
* [ ] Реализовать provider abstraction.
* [ ] Реализовать `request_id`.
* [ ] Реализовать `event_id`.
* [ ] Реализовать `sequence`.
* [ ] Реализовать `json.RawMessage` для provider payload.

### 3. Kafka

* [ ] Реализовать Kafka producer.
* [ ] Реализовать Kafka consumer.
* [ ] Создать Kafka topics.
* [ ] Настроить producer `acks=all`.
* [ ] Включить idempotent producer.
* [ ] Реализовать retry.
* [ ] Реализовать consumer groups.
* [ ] Реализовать graceful consumer shutdown.
* [ ] Реализовать DLQ.
* [ ] Проверить ordering по `request_id`.
* [ ] Проверить consumer offset management.

### 4. Event Protocol

* [ ] Реализовать `queued` event.
* [ ] Реализовать `started` event.
* [ ] Реализовать `reasoning` event.
* [ ] Реализовать `content` event.
* [ ] Реализовать `tool_call` event.
* [ ] Реализовать `usage` event.
* [ ] Реализовать `heartbeat` event.
* [ ] Реализовать `completed` event.
* [ ] Реализовать `failed` event.
* [ ] Реализовать `cancelled` event.
* [ ] Добавить schema tests.
* [ ] Проверить сохранение неизвестных provider fields.

### 5. Heavy LLM Worker

* [ ] Реализовать Heavy provider client.
* [ ] Подключить OpenAI-compatible `/v1/chat/completions`.
* [ ] Реализовать streaming request.
* [ ] Реализовать SSE parser.
* [ ] Реализовать обработку долгого HTTP stream.
* [ ] Разделить connect timeout и generation timeout.
* [ ] Реализовать configurable generation timeout.
* [ ] Реализовать Worker Kafka consumer.
* [ ] Реализовать Worker concurrency limit.
* [ ] Реализовать публикацию каждого upstream event в Kafka.
* [ ] Реализовать `completed` после сохранения последнего chunk.
* [ ] Реализовать retry transient errors.
* [ ] Реализовать failed event.

### 6. Go API

* [ ] Реализовать `POST /v1/chat/completions`.
* [ ] Реализовать `GET /v1/models`.
* [ ] Реализовать OpenAI-compatible request parsing.
* [ ] Реализовать OpenAI-compatible SSE response.
* [ ] Реализовать Bearer authentication.
* [ ] Реализовать request validation.
* [ ] Реализовать provider routing.
* [ ] Реализовать request creation.
* [ ] Реализовать публикацию heavy jobs в Kafka.

### 7. Live Streaming

* [ ] Реализовать SSE dispatcher.
* [ ] Реализовать mapping `request_id -> active stream`.
* [ ] Реализовать Kafka event → SSE.
* [ ] Сохранять порядок events.
* [ ] Не блокировать Worker на медленном клиенте.
* [ ] Обрабатывать client disconnect.
* [ ] Не отменять Heavy job при HTTP disconnect.

### 8. Replay

* [ ] Реализовать `Last-Event-ID`.
* [ ] Реализовать sequence cursor.
* [ ] Реализовать replay из Kafka.
* [ ] Реализовать повторное подключение клиента.
* [ ] Реализовать обнаружение пропущенных sequence.
* [ ] Реализовать deduplication events.

### 9. Async Jobs

* [ ] Реализовать `POST /v1/jobs`.
* [ ] Реализовать `GET /v1/jobs/{id}`.
* [ ] Реализовать `GET /v1/jobs/{id}/events`.
* [ ] Реализовать `POST /v1/jobs/{id}/cancel`.
* [ ] Поддержать `Prefer: respond-async`.
* [ ] Вернуть `202 Accepted` для async jobs.
* [ ] Сохранять состояние job независимо от HTTP connection.

### 10. LiteLLM

* [ ] Реализовать LiteLLM provider.
* [ ] Реализовать streaming.
* [ ] Реализовать non-streaming requests.
* [ ] Подключить `LITELLM_URL`.
* [ ] Подключить `LITELLM_API_KEY`.
* [ ] Подключить `LITELLM_MODEL`.
* [ ] Проверить OpenAI compatibility.

### 11. Ollama

* [ ] Реализовать Ollama chat provider.
* [ ] Реализовать Ollama streaming.
* [ ] Реализовать embeddings provider.
* [ ] Подключить `OLLAMA_URL`.
* [ ] Подключить `OLLAMA_MODEL`.
* [ ] Подключить `OLLAMA_EMBED_MODEL`.
* [ ] Поддержать `CONTEXT_WINDOW`.

### 12. Router

* [ ] Реализовать routing по `model`.
* [ ] Heavy model → Kafka.
* [ ] Gemma → LiteLLM.
* [ ] Llama → Ollama.
* [ ] Вернуть ошибку для неизвестной модели.
* [ ] Добавить конфигурацию routing rules.

### 13. Reliability

* [ ] Реализовать `Idempotency-Key`.
* [ ] Защитить от повторного запуска одного job.
* [ ] Реализовать retry policy.
* [ ] Реализовать DLQ.
* [ ] Реализовать Worker crash recovery.
* [ ] Реализовать Gateway restart recovery.
* [ ] Реализовать graceful shutdown.
* [ ] Проверить duplicate events.
* [ ] Проверить incomplete generation.

### 14. Observability

* [ ] Добавить Prometheus metrics.
* [ ] Добавить `llm_requests_total`.
* [ ] Добавить `llm_requests_active`.
* [ ] Добавить `llm_generation_duration_seconds`.
* [ ] Добавить `llm_queue_duration_seconds`.
* [ ] Добавить `llm_reasoning_tokens_total`.
* [ ] Добавить `llm_output_tokens_total`.
* [ ] Добавить `kafka_consumer_lag`.
* [ ] Добавить `kafka_publish_errors_total`.
* [ ] Добавить `llm_client_disconnects_total`.
* [ ] Добавить `llm_replayed_events_total`.
* [ ] Добавить correlation logging по `request_id`.

### 15. Security

* [ ] Реализовать API authentication.
* [ ] Исключить provider API keys из Kafka payload.
* [ ] Исключить API keys из logs.
* [ ] Исключить API keys из metrics.
* [ ] Добавить request size limits.
* [ ] Добавить rate limiting.
* [ ] Поддержать secrets через environment/Kubernetes Secret.
* [ ] Проверить TLS для внешних provider endpoints.
* [ ] Проверить Kafka authentication/TLS при необходимости.

### 16. Integration Tests

* [ ] Kafka request → Worker → LLM lifecycle.
* [ ] Gateway → Kafka → Worker lifecycle.
* [ ] Streaming response lifecycle.
* [ ] Client disconnect during generation.
* [ ] Gateway restart during generation.
* [ ] Worker restart during generation.
* [ ] Replay after reconnect.
* [ ] Duplicate event handling.
* [ ] Idempotency-Key handling.
* [ ] Failed job lifecycle.
* [ ] DLQ lifecycle.
* [ ] Slow client handling.
* [ ] 30+ minute generation simulation.
* [ ] Preservation of reasoning events.
* [ ] Preservation of full response.

---

## Идеи на будущее

### Intelligent Routing

* [ ] Complexity classifier.
* [ ] Автоматический выбор Heavy / LiteLLM / Ollama.
* [ ] Routing по стоимости.
* [ ] Routing по latency.
* [ ] Routing по текущей загрузке Worker.
* [ ] Routing по размеру context.

### Heavy LLM Scheduling

* [ ] Priority queues.
* [ ] Fair scheduling.
* [ ] Per-user quotas.
* [ ] Dynamic Worker concurrency.
* [ ] GPU-aware scheduling.
* [ ] Queue ETA.

### Storage

* [ ] Отдельное persistent storage для job metadata.
* [ ] PostgreSQL для job state.
* [ ] Object Storage для долгосрочного хранения generation events.
* [ ] Архивирование Kafka events.
* [ ] TTL для completed jobs.

### Kafka

* [ ] Kafka cluster из нескольких brokers.
* [ ] Replication factor >= 3.
* [ ] `min.insync.replicas >= 2`.
* [ ] TLS/SASL.
* [ ] Kafka ACL.
* [ ] Dedicated Kafka cluster для production.
* [ ] Schema Registry при необходимости.

### API

* [ ] Полная совместимость с OpenAI API.
* [ ] `/v1/embeddings`.
* [ ] `/v1/responses`.
* [ ] Tool calling.
* [ ] Structured output.
* [ ] Batch API.
* [ ] Job cancellation.
* [ ] Job priority API.

### Observability

* [ ] OpenTelemetry.
* [ ] Distributed tracing.
* [ ] Grafana dashboards.
* [ ] Alertmanager alerts.
* [ ] Per-provider SLO.
* [ ] Per-model latency statistics.
* [ ] Token accounting.

### High Availability

* [ ] Несколько Gateway instances.
* [ ] Несколько Heavy Workers.
* [ ] Multi-broker Kafka.
* [ ] Kubernetes deployment.
* [ ] Horizontal Pod Autoscaler.
* [ ] Automatic Worker scaling.
* [ ] Multi-zone deployment.

### Advanced Features

* [ ] Request batching.
* [ ] Prompt caching.
* [ ] KV-cache aware scheduling.
* [ ] Speculative decoding.
* [ ] Model fallback.
* [ ] Automatic retry на альтернативном provider.
* [ ] Multi-model ensemble.
* [ ] Semantic cache.
* [ ] RAG integration.

