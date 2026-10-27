# ROADMAP.md

# LLM Gateway

> Актуализировано: **2026-10-27** (по результатам анализа — см. [docs/ANALYSIS.md](docs/ANALYSIS.md), [docs/PROJECT.md](docs/PROJECT.md)).
> Легенда: [x] сделано и работает • [~] сделано частично / с отклонениями • [ ] не сделано.

## Сделано

### Архитектура

* [x] Определена архитектура Go LLM Gateway.
* [x] Определено разделение Gateway / Kafka / Worker / LLM Provider.
* [x] Определено разделение HTTP lifecycle и LLM generation lifecycle.
* [x] Kafka определена как durable buffer и event log для heavy LLM.
* [x] Определена модель `request_id` для корреляции полного жизненного цикла запроса.
* [x] Определена модель `sequence` для сохранения порядка streaming events.
* [x] Определена стратегия `at-least-once delivery`.
* [x] Определено сохранение raw provider payload (на direct-пути — passthrough; на heavy-пути — **нет**, см. Критично-1).
* [x] Определено сохранение reasoning events без фильтрации (на heavy-пути **не выполняется**, см. Критично-1).
* [x] Определён механизм replay после отключения клиента.
* [x] Определён async API для запросов, превышающих HTTP timeout.
* [x] Определено разделение Fast Path (litellm/ollama) и Durable Path (Kafka + heavy).

### 1. Go Project Foundation

* [x] Создать Go module.
* [x] Создать `cmd/gateway`.
* [x] Создать `cmd/worker`.
* [x] Создать базовую структуру `internal/`.
* [x] Реализовать configuration layer.
* [x] Реализовать environment-based configuration.
* [x] Реализовать `log/slog`.
* [x] Реализовать graceful shutdown.
* [x] Реализовать `/health`.
* [x] Реализовать `/ready`.
* [x] Добавить `go test ./...`.
* [x] Добавить `go vet ./...`.

### 2. Domain Model

* [x] Реализовать `ChatRequest` / `ChatResponse` / `ChatChunk` (с сохранением неизвестных полей).
* [x] Реализовать `Job` (job state) и store.
* [x] Реализовать `Event` (envelope).
* [x] Реализовать `JobStatus`.
* [x] Реализовать provider abstraction (`providers.Provider`).
* [x] Реализовать `request_id` (монотонный ULID).
* [x] Реализовать `event_id`.
* [x] Реализовать `sequence`.
* [x] Реализовать `json.RawMessage` для provider payload.

### 3. Kafka

* [x] Реализовать Kafka producer (`acks=all`, idempotent, retry, подтверждение до ACK).
* [x] Реализовать Kafka consumer (consumer groups, session context, at-least-once, MarkMessage после обработки).
* [x] Создать Kafka topics (auto-create, 5 топиков).
* [x] Настроить producer `acks=all`.
* [x] Включить idempotent producer.
* [x] Реализовать retry (producer retry + worker job retry).
* [x] Реализовать consumer groups (`gateway`, `heavy-llm-worker`).
* [x] Реализовать graceful consumer shutdown.
* [x] Реализовать DLQ (poison messages + exhausted retries).
* [x] Проверить ordering по `request_id` (key = request_id).
* [x] Реализовать consumer offset management (auto-commit 1s, commit after processing).

### 4. Event Protocol

* [x] Реализовать `queued` / `started` / `completed` / `failed` / `cancelled` / `raw_sse` events.
* [ ] Реализовать `reasoning` / `content` / `tool_call` / `usage` / `heartbeat` **на heavy-пути** (типы объявлены, worker их не публикует — Критично-1).
* [x] Добавить schema tests (`internal/protocol`).
* [x] Проверить сохранение неизвестных provider fields (Custom Unmarshal/Marshal, `Extra`).

### 5. Heavy LLM Worker

* [x] Реализовать Heavy provider client (OpenAI-compatible, connect/header/idle/generation timeouts).
* [x] Подключить OpenAI-compatible `/v1/chat/completions`.
* [~] Реализовать streaming request — **на direct-пути (Gateway) да; в Worker нет (всегда `stream=false`) — Критично-1**.
* [~] Реализовать SSE parser — в `OpenAIClient` (direct-путь); Worker не использует.
* [x] Реализовать обработку долгого HTTP stream (context-based generation timeout, `http.Client.Timeout` не ставится).
* [x] Разделить connect timeout и generation timeout.
* [x] Реализовать configurable generation timeout.
* [x] Реализовать Worker Kafka consumer.
* [x] Реализовать Worker concurrency limit (семафор, `HEAVY_WORKERS`).
* [ ] Реализовать публикацию каждого upstream event в Kafka (сейчас — 3 события: queued/started/completed).
* [~] Реализовать `completed` после сохранения последнего chunk — верно для non-streaming; перепроверить при переходе на streaming.
* [x] Реализовать retry transient errors (4xx non-retryable, 5xx/timeout retryable, backoff).
* [x] Реализовать failed event (+ `llm.failed`, + DLQ).
* [ ] Реализовать heartbeat events (`HEAVY_HEARTBEAT` принимается, игнорируется).

### 6. Go API

* [x] Реализовать `POST /v1/chat/completions`.
* [x] Реализовать `GET /v1/models` (+ `/{id}`).
* [x] Реализовать OpenAI-compatible request parsing.
* [x] Реализовать OpenAI-compatible response — `stream:false` → один JSON `chat.completion` (Критично-2 исправлено, 2026-10-27); `stream:true` → SSE. Остаток (M1): в synth-чанках SSE нет `created`/`model`.
* [~] Реализовать Bearer authentication — работает при заданных `API_KEYS`; **по умолчанию отключена**.
* [x] Реализовать request validation.
* [x] Реализовать provider routing (deterministic по `model`, включая `DEFAULT_MODEL`).
* [x] Реализовать request creation (ULID).
* [x] Реализовать публикацию heavy jobs в Kafka.

### 7. Live Streaming

* [x] Реализовать SSE dispatcher.
* [x] Реализовать mapping `request_id -> active stream`.
* [x] Реализовать Kafka event → SSE.
* [x] Сохранять порядок events.
* [x] Не блокировать Worker на медленном клиенте (bounded buffer + Kafka + replay).
* [x] Обрабатывать client disconnect.
* [x] Не отменять Heavy job при HTTP disconnect.

### 8. Replay

* [x] Реализовать `Last-Event-ID`.
* [x] Реализовать sequence cursor.
* [x] Реализовать replay из Kafka (full-scan reader — см. Высоко-1).
* [x] Реализовать повторное подключение клиента (POST-путь: replay → live).
* [~] Реализовать обнаружение пропущенных sequence (live-поток дедуплицирует `sequence <= lastSent`; **replay-путь дубли не устраняет — Высоко-2**).
* [~] Реализовать deduplication events (partial, см. выше).
* [ ] `GET /v1/jobs/{id}/events` — живой tail: сейчас разовый replay с `[DONE]` даже для running-jobs (Высоко-4).

### 9. Async Jobs

* [~] Реализовать `POST /v1/jobs` — **отсутствует как отдельный эндпоинт** (async доступен только через `Prefer: respond-async`).
* [x] Реализовать `GET /v1/jobs/{id}` (+ восстановление из Kafka при отсутствии).
* [~] Реализовать `GET /v1/jobs/{id}/events` (replay есть, live tail нет).
* [ ] Реализовать `POST /v1/jobs/{id}/cancel` / working cancel — `DELETE` меняет локальный статус и публикует `cancelled`, но **Worker не потребляет `llm.events`, отмена не доходит до LLM; terminal-статусы не защищают от «отмотки» — Критично-4**.
* [x] Поддержать `Prefer: respond-async`.
* [x] Вернуть `202 Accepted` для async jobs.
* [~] Сохранять состояние job независимо от HTTP connection (in-memory; после рестарта список пуст, восстановление — on-demand).

### 10. LiteLLM

* [x] Реализовать LiteLLM provider.
* [x] Реализовать streaming (raw SSE passthrough).
* [x] Reализовать non-streaming requests (JSON → SSE-чанки; JSON-ответ клиенту — нет, Критично-2).
* [x] Подключить `LITELLM_URL` / `LITELLM_API_KEY` / `LITELLM_MODEL`.
* [x] Проверить OpenAI compatibility (на уровне запроса/ответа — см. Критично-2).

### 11. Ollama

* [x] Реализовать Ollama chat provider (нативный `/api/generate`).
* [x] Реализовать Ollama streaming (NDJSON → OpenAI chunks).
* [x] Реализовать embeddings provider (`/api/embed`, `POST /v1/embeddings`).
* [x] Подключить `OLLAMA_URL` / `OLLAMA_MODEL` / `OLLAMA_EMBED_MODEL`.
* [ ] Поддержать `CONTEXT_WINDOW` (переменная читается, **не используется** — закладка под auto-async по PLAN/BACKLOG №14).

### 12. Router

* [x] Реализовать routing по `model`.
* [x] Heavy model → Kafka.
* [x] Gemma → LiteLLM.
* [x] Llama → Ollama.
* [ ] Вернуть ошибку для неизвестной модели (сейчас unknown → heavy с моделью из запроса).
* [ ] Добавить конфигурацию routing rules (сейчас хардкод по алиасам/физическим именам).

### 13. Reliability

* [~] Реализовать `Idempotency-Key` (in-memory, без TTL — неограниченный рост; lost on restart — заявить).
* [ ] Защитить от повторного запуска одного job (в пределах процесса — да; дедупликация worker-повторов при redelivery — нет, Высоко-2).
* [x] Реализовать retry policy (worker).
* [x] Реализовать DLQ.
* [~] Реализовать Worker crash recovery (at-least-once redelivery; дубли событий в event log не устранены).
* [~] Реализовать Gateway restart recovery (per-job `recoverJob`; job-list после restart пуст).
* [x] Реализовать graceful shutdown.
* [~] Проверить duplicate events (live — да; replay — нет).
* [ ] Проверить incomplete generation (нет сценария «генерация не завершена, что делаем с job»).

### 14. Observability

* [x] Добавить Prometheus metrics (`/metrics` в gateway и worker).
* [x] Добавить `llm_requests_total` / `llm_requests_active` / `llm_requests_failed_total`.
* [x] Добавить `llm_generation_duration_seconds`.
* [ ] Добавить `llm_queue_duration_seconds` (зарегистрирована, не заполняется).
* [ ] Добавить `llm_reasoning_tokens_total` (зарегистрирована, не заполняется; нет данных — heavy-путь non-streaming).
* [ ] Добавить `llm_output_tokens_total` (зарегистрирована, не заполняется).
* [ ] Добавить `kafka_consumer_lag` (зарегистрирована, не заполняется — «особо важная» по PLAN §42).
* [x] Добавить `kafka_publish_errors_total` / `kafka_publish_total`.
* [x] Добавить `llm_client_disconnects_total` / `llm_reconnects_total` / `llm_replayed_events_total`.
* [x] Добавить correlation logging по `request_id`.

### 15. Security

* [~] Реализовать API authentication (работает при заданных `API_KEYS`; **по умолчанию отключена** — production-риск).
* [x] Исключить provider API keys из Kafka payload.
* [x] Исключить API keys from logs.
* [x] Исключить API keys from metrics.
* [ ] Add request size limits (нет `MaxBytesReader` на POST-эндпоинтах).
* [ ] Add rate limiting.
* [~] Поддержать secrets through environment (есть; **дефолтный API-ключ зашит в `config.go`** — вынести).
* [x] Проверить TLS for external provider endpoints (TLS ≥1.2, настраиваемые URL).
* [ ] Проверить Kafka authentication/TLS (не реализовано).

### 16. Integration Tests

* [ ] Kafka request → Worker → LLM lifecycle (e2e).
* [ ] Gateway → Kafka → Worker lifecycle (e2e).
* [ ] Streaming response lifecycle (e2e; на heavy-пути streaming отсутствует).
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
* [ ] Preservation of reasoning events (**не может пройти — Критично-1**).
* [ ] Preservation of full response.

---

## 🔴 Критично (по анализу от 2026-10-27 — детали в docs/ANALYSIS.md)

1. **Критично-1 (C1): heavy-путь non-streaming — ключевая цель. → ✅ Реализовано (2026-10-27).**
   Worker теперь в streaming-режиме: raw SSE-чанки provider'а публикуются в
   `llm.events` verbatim (`raw_sse`) с монотонным sequence (в т.ч. между
   retries), `reasoning_content` агрегируется и сохраняется, heartbeat по
   `HEAVY_HEARTBEAT` (дефолт 30с), `completed` — после сохранённого
   последнего чанка (INV-004), агрегат — в `llm.completed`. Gateway не
   дублирует content для streamed completions; `GET /v1/jobs/{id}` отдаёт
   итоговый content/reasoning/usage (async mode). PLAN §8/§9/§14/§21/§55,
   INV-004/005, acceptance Tests 1–2/5/7/8 — выполнены. Остаток: live tail
   у jobs-events (H4).
2. **Критично-2 (C2): `stream:false` отвечает SSE, а не JSON. → ✅ Исправлено (2026-10-27).**
   Теперь `stream:false` (и отсутствие поля — дефолт по спеке) возвращает
   один JSON-объект `chat.completion` (`application/json`) с полями по
   OpenAI-спеке: `id`, `object`, `created`, `model`,
   `choices[0].message{role,content}`, `finish_reason`, `usage`.
   Kafka-путь: Gateway дожидается terminal-события и собирает ответ из
   `CompletedPayload` (агрегированные content/reasoning/usage от
   streaming-Worker'а). Direct-путь: Gateway запускает provider напрямую
   и агрегирует ответ (объект `chat.completion` или серию чанков).
   Ошибки provider → 502 + JSON Error-схемы. Поведение `stream:true`
   (SSE) не изменено.
3. **Критично-3 (C3): тестовый конвейер сломан. → ✅ Исправлено (2026-10-27).**
   Было: `internal/worker` и `internal/providers` test-бинари не
   компилировались (устаревшие тесты: `fakeProvider` без `CompleteSync`;
   `ParseChunk` не существует), `internal/config` `TestDefault` падает
   (ожидает `:8080`, дефолт `:18080`). Теперь: тесты переписаны под
   `SyncProvider`-worker и актуальный API `OpenAIClient`, добавлены недостающие
   пакеты (ollama, heavy, litellm, kafka-dialer, jobs, streaming, protocol,
   api-embeddings/direct-route); `go test ./...`, `go test -race ./...`,
   `go vet ./...` — зелёные. Остаток: `golangci-lint` не зелёный из-за
   **предсуществующего** долга в неизменённых файлах (wsl_v5/goconst/gocritic —
   ~120 находок), а не из-за тестов; см. docs/ANALYSIS.md §C3.
4. **Критично-4 (C4): отмена фиктивная + терминальные статусы не защищены.**
   Worker не потребляет `llm.events` → LLM не останавливается; `cancelled`
   может столкнуться по sequence с `completed`; `jobs.Apply` переводит
   `cancelled` → `completed`. → Реальный cancel-механизм + state machine.
5. **Критично-5 (C5): `docs/` в .gitignore — документация не под версионированием.**
   Свежий клон теряет BACKLOG.md, swagger-спеку и всю документацию;
   `bin/moq` (8.6 МБ) закоммичен. → Починить .gitignore (игнорировать только
   сгенерированный swagger), вынести бинарники из git.

## 🟠 Высокий приоритет (по анализу)

* **H1:** Replay/`recoverJob` — полный скан `llm.events` от начала топика на каждый вызов
  (O(размер лога)). → Per-request cursor/offset или служебный compacted-топик состояния.
* **H2:** Дубли событий при redelivery не устраняются в replay → двойной content у клиента.
* **H3:** Безопасность по умолчанию open: нет auth (по умолчанию), нет rate limit,
  нет лимита размера тела. → `MaxBytesReader`, rate limit, обязательный auth в production.
* **H4:** `GET /v1/jobs/{id}/events` — разовый replay с `[DONE]`, нет живого tail.
* **H5:** Sarama (IBM) — библиотека в maintenance mode; API зафиксирован V2_4_0_0.
* **H6:** Метрики `kafka_consumer_lag`, `llm_queue_duration_seconds`, `llm_tokens_total`,
  `llm_reasoning/output_tokens_total` зарегистрированы, но не заполняются.
* **H7:** Single-broker Kafka, RF=1, retention не управляется → при потере брокера
  теряются все события (PLAN §13 «critical infrastructure limitation»).

## Идеи на будущее

### Intelligent Routing

* [ ] Complexity classifier.
* [ ] Автоматический выбор Heavy / LiteLLM / Ollama (BACKLOG №14: auto-async при «тяжёлом» запросе по `CONTEXT_WINDOW`).
* [ ] Routing по стоимости.
* [ ] Routing by latency.
* [ ] Routing by current Worker load.
* [ ] Routing by context size.

### Heavy LLM Scheduling

* [ ] Priority queues.
* [ ] Fair scheduling.
* [ ] Per-user quotas.
* [ ] Dynamic Worker concurrency.
* [ ] GPU-aware scheduling.
* [ ] Queue ETA.

### Storage

* [ ] Отдельное persistent storage для job metadata (сейчас in-memory).
* [ ] PostgreSQL for job state.
* [ ] Object storage for long-term generation events.
* [ ] Kafka event archiving.
* [ ] TTL for completed jobs.

### Kafka

* [ ] Multi-broker Kafka cluster (≥3).
* [ ] Replication factor >= 3.
* [ ] `min.insync.replicas >= 2`.
* [ ] TLS/SASL, Kafka ACL.
* [ ] Dedicated production Kafka cluster.
* [ ] Schema Registry if needed.
* [ ] Evaluate migration from IBM/sarama (maintenance mode) to modern client (franz-go).

### API

* [~] Full OpenAI API compatibility — `stream:false` JSON done (C2); remaining: `usage`/`created`/`model` in the SSE synthesized chunks (M1).
* [ ] `/v1/embeddings` для других провайдеров (сейчас Ollama-only).
* [ ] `/v1/responses`.
* [ ] Tool calling.
* [ ] Structured output.
* [ ] Batch API.
* [ ] Job cancellation (working — Критично-4).
* [ ] Job priority API.
* [ ] JWT-токены / временные токены (BACKLOG №5).

### Observability

* [ ] OpenTelemetry (BACKLOG №18, вместе с Jaeger-визуализацией).
* [ ] Distributed tracing.
* [ ] Grafana dashboards.
* [ ] Alertmanager alerts.
* [ ] Per-provider SLO.
* [ ] Per-model latency stats.
* [ ] Token accounting (fill metrics H6).

### High Availability

* [ ] Multiple Gateway instances (job-list recovery — M2).
* [ ] Multiple Heavy Workers (duplicated processes, BACKLOG №16).
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

---

## Repo/Docs hygiene (новый раздел, 2026-10-27)

* [ ] Убрать `docs` из .gitignore (игнорировать только `docs/docs.go`, `docs/swagger.json`, `docs/swagger.yaml`).
* [ ] Убрать `bin/moq` из git (бинарник ставится через `make .install-moq`).
* [ ] Убрать/документировать `Makefile_bk`, `goprint.sh`, `coverage.out` (stale).
* [ ] Поправить `Makefile`: мёртвый `export`-блок в `start`, дублирующийся `OPENAI_MODEL`, `clean` с несуществующей `$(PROJECT_BIN_GATEWAY)`, расхождение `run`/AGENTS.md (mock vs реальная модель).
* [ ] Убрать дефолтный API-ключ-заглушку из `config.go` (`OPENAI_KEY` дефолт «REDACTED» → пустой).
* [ ] Убрать мёртвый код `terminalChunk` из `chat.go`.
* [ ] Опечатки/шум в AGENTS.md, PLAN.md, README.md (L4/L8 в ANALYSIS.md).
