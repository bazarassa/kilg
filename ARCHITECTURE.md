# ARCHITECTURE.md

# LLM Gateway Architecture

## 1. Назначение

LLM Gateway — распределённая Go-система, предоставляющая единый OpenAI-compatible API поверх нескольких LLM providers.

Система должна решать две задачи:

1. Предоставлять клиенту привычный API, совместимый с llama.cpp/OpenAI.
2. Надёжно выполнять долгие generation jobs, которые могут продолжаться 30 минут и более.

Основная архитектурная идея:

```text
HTTP connection
       ≠
LLM generation
```

HTTP-соединение является временным transport layer.

Kafka job является durable representation выполняющейся операции.

---

# 2. Основные компоненты

```text
                         ┌──────────────────┐
                         │      CLIENT      │
                         │                  │
                         │ OpenAI / llama   │
                         │ compatible      │
                         └────────┬─────────┘
                                  │
                                  │ HTTP/SSE
                                  ▼
                         ┌──────────────────┐
                         │   GO GATEWAY     │
                         │                  │
                         │ API              │
                         │ Auth             │
                         │ Router           │
                         │ Job Manager      │
                         │ SSE Dispatcher   │
                         │ Replay           │
                         └───────┬──────────┘
                                 │
              ┌──────────────────┼───────────────────┐
              │                  │                   │
              ▼                  ▼                   ▼
        ┌──────────┐       ┌──────────┐       ┌────────────┐
        │  Ollama  │       │ LiteLLM  │       │   Kafka    │
        └──────────┘       └──────────┘       └─────┬──────┘
                                                    │
                                                    ▼
                                             ┌──────────────┐
                                             │ Heavy Worker │
                                             └──────┬───────┘
                                                    │
                                                    ▼
                                             ┌──────────────┐
                                             │   llama.cpp  │
                                             │    Qwen 27B  │
                                             └──────┬───────┘
                                                    │
                                                    │ SSE
                                                    ▼
                                             ┌──────────────┐
                                             │    Kafka     │
                                             │  llm.events  │
                                             └──────┬───────┘
                                                    │
                                                    ▼
                                             ┌──────────────┐
                                             │ SSE Replay   │
                                             │ Dispatcher   │
                                             └──────┬───────┘
                                                    │
                                                    ▼
                                                  CLIENT
```

---

# 3. Architectural Principles

## 3.1. Gateway не выполняет Heavy LLM

Gateway не должен непосредственно ждать завершения heavy generation.

Неправильно:

```text
Client
  ↓
Gateway
  ↓
llama.cpp
  ↓
30+ minutes
```

Правильно:

```text
Client
  ↓
Gateway
  ↓
Kafka
  ↓
Worker
  ↓
llama.cpp
```

---

## 3.2. Client disconnect не отменяет job

```text
Client
  ↓
Gateway
  ↓
Kafka
  ↓
Worker
  ↓
LLM
```

Если:

```text
Client ──X── Gateway
```

Worker продолжает выполнение.

---

## 3.3. Kafka является durable event log

Kafka используется одновременно как:

* queue;
* buffer;
* event log;
* recovery source;
* replay source.

---

## 3.4. Raw provider data не теряется

Если provider возвращает:

```json
{
  "delta": {
    "reasoning_content": "..."
  }
}
```

это поле должно попасть в Kafka без удаления.

Предпочтительный pipeline:

```text
Provider SSE
    ↓
Raw event
    ↓
json.RawMessage
    ↓
Kafka
```

а не реконструкция JSON из ограниченной Go-структуры.

---

# 4. Provider Architecture

Все providers находятся за единым abstraction layer.

```text
                 Provider Interface
                        │
          ┌─────────────┼─────────────┐
          │             │             │
          ▼             ▼             ▼
       Heavy         LiteLLM        Ollama
          │             │             │
          ▼             ▼             ▼
       Kafka         direct          direct
          │
          ▼
      Worker
          │
          ▼
      llama.cpp
```

---

# 5. Heavy Provider

Heavy provider используется для сложных reasoning requests.

Конфигурация:

```text
OPENAI_BASE_URL
OPENAI_KEY
OPENAI_MODEL
```

Gateway не вызывает его напрямую.

```text
Gateway
   ↓
llm.requests
   ↓
Heavy Worker
   ↓
OPENAI_BASE_URL
   ↓
llama.cpp
```

---

# 6. LiteLLM Provider

LiteLLM предназначен для быстрых instruct requests.

```text
Gateway
   ↓
LiteLLM
```

Конфигурация:

```text
LITELLM_URL
LITELLM_API_KEY
LITELLM_MODEL
```

Kafka не является обязательной частью этого пути.

---

# 7. Ollama Provider

Ollama предназначен для простых запросов и embeddings.

```text
Gateway
   ├── chat ─────> Ollama
   │
   └── embeddings -> Ollama
```

Конфигурация:

```text
OLLAMA_URL
OLLAMA_API_KEY
OLLAMA_MODEL
OLLAMA_EMBED_MODEL
CONTEXT_WINDOW
```

---

# 8. Request Lifecycle

Heavy request:

```text
1. Client
      │
      ▼
2. Gateway
      │
      ▼
3. Validate request
      │
      ▼
4. Generate request_id
      │
      ▼
5. Publish llm.requests
      │
      ▼
6. Heavy Worker
      │
      ▼
7. llama.cpp
      │
      ▼
8. SSE events
      │
      ▼
9. Publish llm.events
      │
      ▼
10. Gateway Dispatcher
      │
      ▼
11. Client SSE
```

---

# 9. Request ID

Каждый job имеет стабильный:

```text
request_id
```

Он используется во всех внутренних системах:

```text
HTTP
Kafka
Worker
logs
metrics
events
replay
```

Рекомендуемый формат:

```text
ULID
```

или:

```text
UUIDv7
```

---

# 10. Event ID

Каждый event имеет:

```text
event_id
```

Он уникален для конкретного event.

Используется для:

* deduplication;
* debugging;
* tracing.

---

# 11. Sequence

Каждый request имеет собственный sequence:

```text
request A:
    0
    1
    2
    3

request B:
    0
    1
    2
```

Sequence гарантирует порядок событий внутри одного generation.

---

# 12. Event Envelope

Рекомендуемая структура:

```go
type LLMEvent struct {
    EventID   string          `json:"event_id"`
    RequestID string          `json:"request_id"`
    Sequence  uint64          `json:"sequence"`
    Type      string          `json:"type"`
    Provider  string          `json:"provider"`
    Model     string          `json:"model"`
    Timestamp time.Time       `json:"timestamp"`
    Payload   json.RawMessage `json:"payload"`
}
```

---

# 13. Event Types

Минимальный набор:

```text
queued
started
reasoning
content
tool_call
usage
heartbeat
completed
failed
cancelled
```

Provider-specific events также должны поддерживаться.

---

# 14. Kafka Topology

```text
Kafka
│
├── llm.requests
│
├── llm.events
│
├── llm.completed
│
├── llm.failed
│
└── llm.dlq
```

Broker:

```text
REDACTED:9092
```

---

# 15. `llm.requests`

Назначение:

```text
Gateway → Heavy Worker
```

Key:

```text
request_id
```

Payload содержит:

* request_id;
* provider;
* model;
* original chat request;
* creation timestamp;
* generation parameters.

---

# 16. `llm.events`

Назначение:

```text
Worker → Gateway
```

Содержит каждый streaming event.

Пример:

```text
request_id=A
sequence=0
reasoning

request_id=A
sequence=1
reasoning

request_id=A
sequence=2
content

request_id=A
sequence=3
content
```

---

# 17. `llm.completed`

Содержит notification о завершении job.

Важно:

```text
last event
    ↓
Kafka ACK
    ↓
completed
    ↓
Kafka ACK
```

---

# 18. `llm.failed`

Содержит окончательно завершённые failed jobs.

Payload должен содержать:

```text
request_id
error code
error message
provider
model
attempt
timestamp
```

Секреты не сохраняются.

---

# 19. `llm.dlq`

Используется для jobs, которые невозможно обработать после установленного количества retries.

В DLQ должен попадать исходный job и диагностическая информация.

---

# 20. Kafka Ordering

Kafka key:

```text
request_id
```

Все events одного job должны попадать в одну partition.

Это гарантирует ordering внутри partition.

---

# 21. Delivery Semantics

Система использует:

```text
at-least-once
```

Необходимо учитывать возможную повторную доставку.

Для этого используются:

```text
event_id
request_id
sequence
```

---

# 22. Idempotent Producer

Kafka producer должен использовать:

```text
acks=all
enable.idempotence=true
```

Retry разрешён.

---

# 23. Worker Architecture

```text
Kafka Consumer
      │
      ▼
Job Decoder
      │
      ▼
Concurrency Limiter
      │
      ▼
Heavy Provider
      │
      ▼
SSE Parser
      │
      ▼
Event Builder
      │
      ▼
Kafka Producer
```

---

# 24. Worker Concurrency

Количество одновременно выполняющихся Heavy jobs ограничено.

Например:

```text
HEAVY_MAX_CONCURRENCY=2
```

Kafka содержит остальные jobs.

```text
job1 → Worker
job2 → Worker
job3 → Kafka queue
job4 → Kafka queue
```

---

# 25. SSE Processing

Worker не должен ждать полного response.

Он читает stream:

```text
chunk
 ↓
parse
 ↓
event
 ↓
Kafka
```

Каждый chunk обрабатывается отдельно.

---

# 26. Generation Timeout

Для Heavy Worker timeout должен быть отдельным от HTTP Gateway timeout.

Например:

```text
HEAVY_LLM_TIMEOUT=2h
```

Gateway может иметь:

```text
HTTP timeout = 1000s
```

Это не должно ограничивать:

```text
Heavy Worker generation = 2h
```

---

# 27. Gateway Streaming

Для live streaming:

```text
Kafka
  ↓
Dispatcher
  ↓
request_id
  ↓
HTTP stream
  ↓
SSE
```

Gateway не должен получать данные непосредственно от Heavy Worker.

---

# 28. Slow Client

Медленный клиент:

```text
Client
   ↓
slow TCP
```

не должен блокировать:

```text
Worker
```

Worker продолжает:

```text
LLM → Kafka
```

Gateway читает Kafka независимо от скорости upstream generation.

---

# 29. Client Disconnect

При disconnect:

```text
HTTP connection
      X
```

делается:

```text
remove active stream
```

но не:

```text
cancel LLM job
```

Job продолжает выполняться.

---

# 30. Replay

Клиент может запросить продолжение:

```http
Last-Event-ID: 1532
```

Gateway должен найти:

```text
sequence > 1532
```

и отправить события клиенту.

---

# 31. Async Jobs

Для долгих запросов:

```text
POST /v1/jobs
```

Gateway:

```text
validate
 ↓
create request_id
 ↓
Kafka
 ↓
202 Accepted
```

После этого клиент может использовать:

```text
GET /v1/jobs/{id}
```

и:

```text
GET /v1/jobs/{id}/events
```

---

# 32. Cancellation

Cancellation является отдельной операцией.

```text
POST /v1/jobs/{id}/cancel
```

HTTP disconnect не является cancellation.

После cancellation:

```text
Gateway
   ↓
cancel event
   ↓
Worker
   ↓
context cancellation
   ↓
LLM
```

---

# 33. Full Response

Kafka events должны позволять восстановить:

```text
reasoning
content
tool calls
usage
finish reason
metadata
```

Полный результат является производным представлением event stream.

Kafka event history является первичным источником.

---

# 34. Reasoning Preservation

Если upstream возвращает:

```text
reasoning_content
```

Gateway/Worker сохраняет его.

Запрещено молча:

```text
drop reasoning
truncate reasoning
replace reasoning
```

Если reasoning отсутствует у provider:

```text
reasoning = unavailable
```

---

# 35. Provider Routing

Первичная реализация:

```text
                Router
                  │
       ┌──────────┼──────────┐
       │          │          │
       ▼          ▼          ▼
     Heavy      Gemma       Llama
       │          │          │
       ▼          ▼          ▼
     Kafka      LiteLLM    Ollama
```

Routing должен быть deterministic.

---

# 36. OpenAI-compatible API

Основные endpoints:

```text
POST /v1/chat/completions
GET  /v1/models
```

Async endpoints:

```text
POST /v1/jobs
GET  /v1/jobs/{id}
GET  /v1/jobs/{id}/events
POST /v1/jobs/{id}/cancel
```

Health:

```text
GET /health
GET /ready
```

---

# 37. Streaming Compatibility

Клиент должен видеть стандартный:

```text
Content-Type: text/event-stream
```

и:

```text
data: {...}

data: {...}

data: [DONE]
```

Внутренняя Kafka architecture не должна быть видна клиенту.

---

# 38. Security

Provider credentials находятся только внутри server-side components.

```text
Client
  |
  X provider API key
  |
Gateway
  |
  +── Provider secret
  |
  +── Kafka credentials
```

Provider API keys не передаются в Kafka job.

---

# 39. Secret Management

Production secrets:

```text
Kubernetes Secret
Vault
Docker Secret
```

Не использовать:

```text
Git
source code
Kafka payload
logs
metrics
traces
```

---

# 40. Observability

Каждый request должен быть связан через:

```text
request_id
```

Минимальные metrics:

```text
llm_requests_total
llm_requests_active
llm_requests_failed_total

llm_queue_duration_seconds
llm_generation_duration_seconds

llm_reasoning_tokens_total
llm_output_tokens_total

kafka_publish_total
kafka_publish_errors_total
kafka_consumer_lag

llm_client_disconnects_total
llm_replayed_events_total
```

---

# 41. Logging

Использовать structured logging через:

```text
log/slog
```

Пример:

```text
INFO generation completed
request_id=01K...
provider=heavy
model=...
duration=1832s
```

Не логировать:

```text
Authorization
API keys
Kafka credentials
```

---

# 42. Graceful Shutdown

Gateway:

```text
SIGTERM
 ↓
stop new requests
 ↓
finish active streams
 ↓
stop consumers
 ↓
shutdown
```

Worker:

```text
SIGTERM
 ↓
stop accepting new jobs
 ↓
finish current generation
 ↓
publish final event
 ↓
commit offset
 ↓
shutdown
```

---

# 43. Crash Recovery

## Gateway crash

```text
Gateway X
   |
Worker
   |
Kafka
   |
events remain
```

После restart Gateway может продолжить consumption/replay.

---

## Worker crash

```text
Worker X
   |
Kafka offset
   |
Worker restart
   |
retry
```

Необходимо учитывать возможные duplicate events.

---

# 44. Retention

`llm.events` должен использовать обычный retention.

Не использовать compaction как основной механизм хранения event history.

Причина:

```text
один request_id
      ↓
много событий
      ↓
все нужны для replay
```

---

# 45. Production Kafka

Для production желательно:

```text
Kafka brokers >= 3
replication.factor >= 3
min.insync.replicas >= 2
acks=all
```

Текущая инфраструктура с одним broker не обеспечивает отказоустойчивость самого Kafka storage.

Она обеспечивает только durable buffering относительно Gateway/Worker failures.

---

# 46. Package Structure

```text
cmd/
├── gateway/
│   └── main.go
│
└── worker/
    └── main.go

internal/
├── api/
│   ├── chat.go
│   ├── jobs.go
│   ├── events.go
│   └── models.go
│
├── config/
│   └── config.go
│
├── kafka/
│   ├── producer.go
│   ├── consumer.go
│   └── topics.go
│
├── jobs/
│   ├── manager.go
│   └── state.go
│
├── providers/
│   ├── provider.go
│   ├── heavy/
│   │   └── client.go
│   ├── litellm/
│   │   └── client.go
│   └── ollama/
│       └── client.go
│
├── router/
│   ├── router.go
│   └── policy.go
│
├── streaming/
│   ├── dispatcher.go
│   ├── sse.go
│   └── replay.go
│
├── protocol/
│   ├── events.go
│   └── openai.go
│
└── observability/
    ├── logging.go
    ├── metrics.go
    └── tracing.go
```

---

# 47. Dependency Direction

Зависимости должны двигаться внутрь:

```text
API
 ↓
Application
 ↓
Domain
 ↓
Infrastructure
```

Domain не должен зависеть от:

```text
Kafka
HTTP
llama.cpp
Ollama
LiteLLM
```

Provider implementations находятся на infrastructure layer.

---

# 48. Testing Architecture

Unit tests:

```text
domain
protocol
router
SSE parser
event sequencing
```

Integration tests:

```text
Kafka
Gateway
Worker
Provider
```

End-to-end:

```text
Client
 ↓
Gateway
 ↓
Kafka
 ↓
Worker
 ↓
LLM mock
 ↓
Kafka
 ↓
Gateway
 ↓
Client
```

Для тестов Heavy LLM желательно использовать mock OpenAI-compatible SSE server.

Это позволяет тестировать:

* тысячи chunks;
* reasoning;
* задержки;
* disconnect;
* malformed events;
* provider errors;
* completion;
* reconnect.

---

# 49. Critical Invariants

### INV-001

```text
HTTP disconnect MUST NOT cancel durable Heavy job.
```

### INV-002

```text
Every committed LLM event MUST be replayable.
```

### INV-003

```text
Events belonging to one request MUST preserve order.
```

### INV-004

```text
completed MUST be published after the final response event.
```

### INV-005

```text
Provider reasoning fields MUST NOT be silently discarded.
```

### INV-006

```text
Worker MUST NOT depend on a live HTTP client.
```

### INV-007

```text
Slow HTTP client MUST NOT block Heavy generation.
```

### INV-008

```text
Duplicate Kafka delivery MUST NOT corrupt the client stream.
```

### INV-009

```text
Provider credentials MUST never enter Kafka events.
```

### INV-010

```text
Async job MUST survive HTTP timeout.
```

---

# 50. Failure Model

Система должна корректно переживать:

```text
client disconnect
gateway restart
worker restart
Kafka temporary unavailable
LLM temporary unavailable
LLM timeout
provider 5xx
slow client
duplicate Kafka event
```

