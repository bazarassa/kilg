## Существует несколько LLM провайдеров требуется создать высоконагруженный Go-сервис-прокси 

Существует LLM провайдер для сложных запросов (OpenAI-совместимый API который может размышлять по 30 минут и более)                             

OPENAI_BASE_URL=https://llm.example.com/v1                                                  # Кастомный URL провайдера LLM
OPENAI_KEY=mpQ                                                               # Ключ  LLM провайдера
OPENAI_MODEL=HauhauCS/Qwen3.8-27B-Uncensored-HauhauCS-Aggressive-MTP-GGUF:Q6_K_P             # Плотный 

Проблема в том, что при нескольких одновременных запросах или после одного тяжелого запроса может не хватить времени (На веб-сервере максимальный таймаут 1000 ) и запрос отвалится по таймауту а модель квен в это время будет еще размышлять

Нужно сделать архитектуру решения и создать PALAN.md разработки в котором в качеств буфера между клиентом и API LLM плотной модели запущенной через lama.cpp будет использоваться кафка - cокет подключения к ней REDACTED:9092
 
kafkacat -L -b REDACTED:9092
Metadata for all topics (from broker -1: REDACTED:9092/bootstrap):
 1 brokers:
  broker 1 at REDACTED:9093
 0 topics:

#Порт 9092: Обычно зарезервирован для незашифрованных соединений ( PLAINTEXT )
#Порт 9093: Наиболее распространенный стандартный порт по умолчанию для клиентских SSL/TLS- соединений

# Дополнительный LLM провайдер с быстрой instruct моделью
LITELLM_URL=https://llm.example.com/v1
LITELLM_API_KEY=mpQ
LITELLM_MODEL=gemma-4-E4B-it-GGUF

# Локальный LLM (Ollama): простые вопросы и эмбеддинги
OLLAMA_URL=http://ollama.local:11434
OLLAMA_API_KEY=ollama
OLLAMA_MODEL=llama3.2:3b-instruct-q4_K_M
CONTEXT_WINDOW=8192                                                  # Лимит токенов контекста
OLLAMA_EMBED_MODEL=nomic-embed-text:latest                           # Модель эмбединга (размерность 768)

---
Требуется реализовать решение на go используя современные технологии - самое главное условие без потреть предовать ответы моделей через топики а также их рассуждения при этом клиент должен подключаться также как и к обычной lama.cpp

Да. Здесь лучше не пытаться просто увеличить HTTP timeout. Правильное решение — **развязать жизненный цикл HTTP-запроса и жизненный цикл генерации LLM через Kafka**.

Ключевой принцип:

> **HTTP-клиент общается с Go Gateway так, будто это обычный llama.cpp/OpenAI-compatible сервер, а внутри Gateway запрос превращается в durable job в Kafka. LLM Worker независимо выполняет 30+ минут генерации, а каждый фрагмент ответа и reasoning сохраняется в Kafka и одновременно доставляется подключённому клиенту.**

При этом необходимо учитывать важное ограничение: если внешний web-server действительно имеет **жёсткий абсолютный timeout 1000 секунд**, стандартный `POST /v1/chat/completions` невозможно гарантированно удерживать 30+ минут. Поэтому архитектура должна поддерживать **обычный streaming-режим + асинхронный режим с тем же OpenAI-compatible API**.

Ниже — готовый `PLAN.md`.

# PLAN.md

# Kafka Interference LLM Gateway — план разработки

## 1. Цель проекта

Создать высоконадёжный Go-сервис-прокси между клиентами OpenAI/llama.cpp-compatible API и несколькими LLM-провайдерами.

Основная задача — обеспечить выполнение тяжёлых запросов к reasoning-модели, которые могут выполняться 30 минут и более, без потери:

* запроса пользователя;
* обычных response tokens;
* reasoning tokens;
* промежуточных chunks;
* финального ответа;
* метаданных генерации;
* состояния выполнения job.

Kafka используется как **durable buffer / event log** между HTTP API и тяжёлой LLM.

Клиент при этом должен работать с Gateway практически так же, как с обычным `llama.cpp` OpenAI-compatible сервером:

```text
POST /v1/chat/completions
Authorization: Bearer <key>
Content-Type: application/json
```

и получать стандартный OpenAI-compatible streaming SSE:

```text
data: {...}

data: {...}

data: [DONE]
```

---

# 2. Исходная проблема

Существует несколько LLM-провайдеров.

## 2.1. Heavy reasoning LLM

OpenAI-compatible API:

```text
OPENAI_BASE_URL=https://llm.example.com/v1
OPENAI_MODEL=HauhauCS/Qwen3.8-27B-Uncensored-HauhauCS-Aggressive-MTP-GGUF:Q6_K_P
```

Модель работает через llama.cpp и может размышлять десятки минут.

Проблема:

```text
Client
   |
   | HTTP request
   v
Web Server
   |
   | timeout <= 1000 sec
   v
LLM
   |
   | generation > 1000 sec
   X
HTTP timeout
```

При этом LLM продолжает генерировать ответ, но HTTP-соединение уже потеряно.

---

# 3. Требуемая архитектура

Итоговая архитектура:

```text
                         ┌───────────────────────────┐
                         │        Client             │
                         │                           │
                         │ OpenAI / llama.cpp client │
                         └─────────────┬─────────────┘
                                       │
                                       │ HTTPS
                                       │ OpenAI API
                                       ▼
                         ┌───────────────────────────┐
                         │       Go LLM Gateway      │
                         │                           │
                         │ /v1/chat/completions      │
                         │ /v1/models                │
                         │ /v1/health                │
                         │                           │
                         │ Router                    │
                         │ Job Manager               │
                         │ Kafka Producer            │
                         │ Kafka Consumer             │
                         │ SSE Stream Manager        │
                         └─────────────┬─────────────┘
                                       │
                                       │ Kafka
                                       ▼
                 ┌─────────────────────────────────────────┐
                 │                Kafka                     │
                 │                                         │
                 │ llm.requests                            │
                 │ llm.events                              │
                 │ llm.completed                           │
                 │ llm.failed                              │
                 │ llm.dlq                                 │
                 └──────────────────┬──────────────────────┘
                                    │
                                    ▼
                         ┌───────────────────────────┐
                         │      LLM Worker           │
                         │                           │
                         │ Consumer                  │
                         │ Provider Client           │
                         │ SSE Parser                │
                         │ Event Producer            │
                         └─────────────┬─────────────┘
                                       │
                       ┌───────────────┼────────────────┐
                       │               │                │
                       ▼               ▼                ▼
                 Heavy LLM         LiteLLM           Ollama
                 llama.cpp         instruct          local
```

---

# 4. Основной принцип работы

HTTP Gateway **не должен выполнять тяжёлый LLM-запрос непосредственно внутри HTTP handler**.

Вместо этого:

```text
HTTP
  |
  v
validate request
  |
  v
generate request_id
  |
  v
publish Kafka job
  |
  v
wait/stream events
```

А worker:

```text
Kafka request
      |
      v
call llama.cpp
      |
      v
receive SSE chunks
      |
      v
publish every chunk to Kafka
      |
      v
completion event
```

---

# 5. Request ID

Каждому запросу обязательно назначается уникальный:

```text
request_id
```

Например:

```text
01K7ABCDEF123456789
```

Рекомендуется использовать ULID.

Request ID должен присутствовать во всех внутренних Kafka events.

Например:

```json
{
  "request_id": "01K7ABCDEF123456789",
  "event_id": 1532,
  "type": "token",
  "timestamp": "2026-09-09T01:30:00Z"
}
```

---

# 6. Kafka topics

Минимальная схема:

```text
llm.requests
llm.events
llm.completed
llm.failed
llm.dlq
```

## 6.1. llm.requests

Очередь заданий для Worker.

Key:

```text
request_id
```

Value:

```json
{
  "request_id": "...",
  "provider": "heavy",
  "model": "...",
  "created_at": "...",
  "payload": {
    "model": "...",
    "messages": [],
    "stream": true,
    "temperature": 0.7
  }
}
```

---

# 7. llm.events

Главный topic.

В него записываются **все промежуточные события генерации**.

Например:

```json
{
  "request_id": "01K7ABCDEF",
  "event_id": 1,
  "type": "started",
  "data": {}
}
```

Затем:

```json
{
  "request_id": "01K7ABCDEF",
  "event_id": 2,
  "type": "reasoning",
  "data": {
    "delta": "..."
  }
}
```

Затем:

```json
{
  "request_id": "01K7ABCDEF",
  "event_id": 3,
  "type": "reasoning",
  "data": {
    "delta": "..."
  }
}
```

И:

```json
{
  "request_id": "01K7ABCDEF",
  "event_id": 10000,
  "type": "content",
  "data": {
    "delta": "..."
  }
}
```

Финал:

```json
{
  "request_id": "01K7ABCDEF",
  "event_id": 10001,
  "type": "completed",
  "data": {
    "finish_reason": "stop"
  }
}
```

---

# 8. Критически важное требование: reasoning нельзя терять

Gateway/Worker **не должен фильтровать reasoning**.

Если upstream llama.cpp возвращает:

```json
{
  "choices": [
    {
      "delta": {
        "reasoning_content": "..."
      }
    }
  ]
}
```

то этот chunk должен быть сохранён.

Необходимо сохранять **оригинальное содержимое upstream SSE event**.

То есть предпочтительно:

```text
llama.cpp SSE
      |
      v
raw event
      |
      +------> Kafka
      |
      +------> client
```

а не:

```text
llama.cpp
      |
      v
JSON parsing
      |
      v
reconstruct JSON
```

Причина — реконструкция может привести к потере неизвестных полей, reasoning metadata или provider-specific extensions.

---

# 9. Raw event preservation

Каждый SSE event должен иметь:

```json
{
  "request_id": "...",
  "sequence": 123,
  "event_type": "raw_sse",
  "payload": "data: { ... }",
  "created_at": "..."
}
```

`payload` должен содержать оригинальный upstream event.

Таким образом Kafka фактически становится:

```text
durable transcript
```

генерации.

---

# 10. Sequence number

Для каждого request:

```text
sequence = 0
sequence = 1
sequence = 2
...
```

Sequence должен монотонно увеличиваться.

Например:

```text
request_id=A sequence=0
request_id=A sequence=1
request_id=A sequence=2
request_id=A sequence=3
```

Это позволяет обнаруживать:

* пропущенные chunks;
* дубли;
* неправильный порядок;
* повторную доставку.

---

# 11. Kafka partitioning

Partition key:

```text
request_id
```

Это гарантирует, что события одного запроса попадают в одну partition.

Например:

```text
llm.events

partition 0:
    request A
    request C

partition 1:
    request B
    request D

partition 2:
    request E
```

Для одного request порядок сохраняется.

---

# 12. Kafka producer

Producer должен работать с максимально надёжной конфигурацией.

Обязательные требования:

```text
acks=all
enable.idempotence=true
```

Также:

```text
retries > 0
```

и разумные значения:

```text
delivery timeout
request timeout
retry backoff
```

Producer должен считать event успешно сохранённым только после подтверждения Kafka.

---

# 13. Kafka durability

Текущий broker:

```text
REDACTED:9092
```

обнаруживает:

```text
broker 1 at REDACTED:9093
```

и сейчас Kafka показывает:

```text
0 topics
```

Это означает, что перед production необходимо отдельно настроить Kafka.

Особенно важно:

```text
replication.factor >= 3
min.insync.replicas >= 2
acks=all
```

если будет доступно минимум 3 brokers.

Если Kafka остаётся single-broker:

```text
replication.factor = 1
```

то Kafka защищает от падения consumer/worker/Gateway, но **не защищает от потери самого broker**.

Это критическое ограничение инфраструктуры.

---

# 14. LLM Worker

Worker — отдельный Go-процесс.

Он не должен зависеть от HTTP timeout.

Workflow:

```text
Kafka Consumer
      |
      v
receive llm.requests
      |
      v
create LLM context
      |
      v
HTTP request to llama.cpp
      |
      v
SSE stream
      |
      v
publish every event
      |
      v
llm.events
```

Если генерация длится:

```text
30 минут
```

worker продолжает работать всё это время.

---

# 15. Worker timeout

HTTP timeout Worker -> llama.cpp должен быть существенно больше ожидаемого времени генерации.

Нельзя использовать:

```go
http.Client{
    Timeout: 1000 * time.Second,
}
```

для heavy model.

Рекомендуется:

```text
connect timeout       = 10s
TLS handshake         = 10s
response header       = например 60s
idle timeout          = configurable
overall generation    = configurable
```

Важно различать:

```text
connection timeout
```

и:

```text
generation timeout
```

Generation timeout должен быть configurable.

Например:

```env
HEAVY_LLM_TIMEOUT=2h
```

---

# 16. HTTP Gateway

Gateway реализует OpenAI-compatible API:

```text
POST /v1/chat/completions
GET  /v1/models
GET  /health
GET  /ready
```

Основной endpoint:

```text
/v1/chat/completions
```

---

# 17. Router

Gateway должен выбирать провайдера.

Например:

```text
simple question
       |
       v
Ollama

normal instruct request
       |
       v
LiteLLM

complex reasoning
       |
       v
Kafka -> Heavy Worker -> llama.cpp
```

Провайдер можно выбирать:

### Вариант A

По имени модели:

```text
llama3.2:3b-instruct-q4_K_M
gemma-4-E4B-it-GGUF
HauhauCS/Qwen...
```

### Вариант B

Через явный параметр:

```json
{
  "model": "heavy"
}
```

### Вариант C

Через policy/router:

```text
complexity classifier
```

Для MVP рекомендуется A+B.

---

# 18. Провайдеры

## Heavy

```env
OPENAI_BASE_URL=https://llm.example.com/v1
OPENAI_MODEL=...
```

Маршрут:

```text
Gateway
   |
   v
Kafka
   |
   v
Heavy Worker
   |
   v
llama.cpp
```

---

## LiteLLM

```env
LITELLM_URL=https://llm.example.com/v1
LITELLM_MODEL=gemma-4-E4B-it-GGUF
```

Маршрут:

```text
Gateway
   |
   v
LiteLLM
```

Kafka для быстрых запросов в MVP необязательна.

---

## Ollama

```env
OLLAMA_URL=http://ollama.local:11434
OLLAMA_MODEL=llama3.2:3b-instruct-q4_K_M
OLLAMA_EMBED_MODEL=nomic-embed-text:latest
CONTEXT_WINDOW=8192
```

Маршрут:

```text
Gateway
   |
   v
Ollama
```

Embeddings:

```text
/v1/embeddings
```

могут обслуживаться отдельным Ollama provider.

---

# 19. Streaming

Для обычного streaming клиента:

```http
POST /v1/chat/completions
```

с:

```json
{
  "stream": true
}
```

Gateway должен выдавать:

```text
Content-Type: text/event-stream
Cache-Control: no-cache
Connection: keep-alive
```

И передавать chunks клиенту.

---

# 20. Главное отличие от обычного proxy

Обычный proxy:

```text
client
  |
  v
proxy
  |
  v
llama.cpp
```

Kafka Gateway:

```text
client
  |
  v
gateway
  |
  v
Kafka
  |
  v
worker
  |
  v
llama.cpp
```

При этом response stream:

```text
llama.cpp
   |
   v
worker
   |
   +----> Kafka
   |
   v
gateway
   |
   v
client
```

---

# 21. Что происходит при отключении клиента

Это принципиально важный сценарий.

Допустим:

```text
T+0     request
T+10m   reasoning
T+15m   client disconnect
T+30m   LLM finishes
```

Worker **не должен прекращать generation**.

Он продолжает:

```text
LLM
 |
 v
Kafka
```

Все chunks сохраняются.

После завершения:

```text
completed
```

остается в Kafka.

---

# 22. Что происходит после перезапуска Gateway

Gateway запускается снова:

```text
Gateway restart
      |
      v
Kafka
      |
      v
read events
```

Поскольку events находятся в Kafka, они не потеряны.

Gateway может определить:

```text
request_id
status
last_sequence
```

и восстановить состояние.

---

# 23. Асинхронный API

Для запросов, которые потенциально превышают абсолютный HTTP timeout, нужен asynchronous mode.

Например:

```http
POST /v1/chat/completions
Prefer: respond-async
```

Gateway может вернуть:

```json
{
  "id": "01K7ABCDEF",
  "object": "chat.completion.job",
  "status": "queued"
}
```

HTTP request завершается быстро.

Затем:

```text
GET /v1/chat/completions/{request_id}
```

или:

```text
GET /v1/jobs/{request_id}
```

получает состояние.

Streaming:

```text
GET /v1/jobs/{request_id}/events
```

---

# 24. Почему нужен async mode

Если перед Gateway стоит nginx/load balancer с жёстким:

```text
timeout = 1000s
```

то невозможно гарантировать:

```text
HTTP connection > 1000s
```

даже если Go и Kafka работают идеально.

Поэтому:

```text
standard mode
```

используется для обычных запросов.

А:

```text
async mode
```

используется для потенциально долгих запросов.

Это единственный архитектурно надёжный вариант.

---

# 25. Совместимость с llama.cpp

Основной endpoint должен максимально соответствовать:

```text
OpenAI Chat Completions API
```

Например:

```bash
curl http://gateway:8080/v1/chat/completions \
  -H "Authorization: Bearer TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "heavy",
    "messages": [
      {
        "role": "user",
        "content": "..."
      }
    ],
    "stream": true
  }'
```

Клиенту не нужно знать:

```text
Kafka
Worker
provider
partition
consumer
```

---

# 26. Kafka не должна быть видна клиенту

Клиент подключается только к:

```text
Gateway
```

а не:

```text
Kafka
```

То есть:

```text
Client
   |
   | OpenAI-compatible HTTP
   v
Gateway
   |
   | Kafka protocol
   v
Kafka
```

Это позволяет заменить Kafka в будущем без изменения клиентов.

---

# 27. Event envelope

Рекомендуемый формат:

```go
type LLMEvent struct {
    EventID     string          `json:"event_id"`
    RequestID   string          `json:"request_id"`
    Sequence    uint64          `json:"sequence"`
    Type        string          `json:"type"`
    Provider    string          `json:"provider"`
    Model       string          `json:"model"`
    Timestamp   time.Time       `json:"timestamp"`
    Payload     json.RawMessage `json:"payload"`
}
```

Для raw SSE:

```go
type RawSSEPayload struct {
    Data string `json:"data"`
}
```

---

# 28. Event types

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

Но Worker должен быть готов передавать неизвестные provider-specific events.

---

# 29. Idempotency

Клиент может повторить запрос из-за:

```text
timeout
network failure
retry
```

Поэтому Gateway должен поддерживать:

```http
Idempotency-Key: <value>
```

Связь:

```text
Idempotency-Key
        |
        v
request_id
```

Один и тот же Idempotency-Key не должен запускать две одинаковые heavy generation.

---

# 30. Exactly-once vs at-least-once

Необходимо не обещать невозможное.

На уровне всей системы наиболее практичный вариант:

```text
Kafka = at-least-once delivery
```

с:

```text
idempotent producer
+
request_id
+
sequence
+
event_id
```

Это позволяет сделать consumer-side deduplication.

Фактическая гарантия:

```text
no lost committed Kafka events
```

при соблюдении Kafka durability.

---

# 31. Финальный ответ

Особое внимание необходимо уделить финальному результату.

Нельзя считать job завершённым только потому, что worker получил HTTP EOF.

Последовательность:

```text
LLM stream
    |
    v
last token
    |
    v
publish final content event
    |
    v
Kafka ACK
    |
    v
publish completed event
    |
    v
Kafka ACK
    |
    v
job = completed
```

То есть:

```text
completed
```

публикуется **после сохранения последнего response event**.

---

# 32. Полный ответ должен сохраняться

Помимо chunks желательно сформировать итоговый aggregate:

```json
{
  "request_id": "...",
  "status": "completed",
  "reasoning": "...",
  "content": "...",
  "usage": {},
  "model": "...",
  "started_at": "...",
  "completed_at": "..."
}
```

Это позволяет получить полный результат после окончания генерации.

Kafka events остаются первичным источником истины.

---

# 33. Reasoning storage policy

Необходимо сохранить возможность:

```text
raw reasoning
```

если provider его отдаёт.

Но Gateway не должен самостоятельно пытаться "извлекать мысли" из обычного текста.

То есть:

```text
reasoning_content
```

сохраняется как provider-provided field.

Если upstream provider его не предоставляет:

```text
reasoning = unavailable
```

а не генерируется искусственно.

---

# 34. Backpressure

Heavy model может генерировать быстрее, чем Gateway успевает отправлять клиенту.

Kafka решает эту проблему:

```text
Worker
  |
  v
Kafka
  |
  v
Gateway
  |
  v
slow client
```

Worker не должен зависеть от скорости конкретного HTTP клиента.

---

# 35. Медленный клиент

Если клиент читает SSE медленно:

```text
client slow
```

Gateway не должен блокировать Worker.

Kafka выступает buffer.

Gateway должен иметь bounded in-memory buffers.

Например:

```text
per-request buffer = configurable
```

При переполнении:

```text
stop direct streaming
continue Kafka consumption
```

и обеспечить последующее replay.

---

# 36. Replay

Желательно поддержать:

```http
Last-Event-ID: 123
```

Gateway может продолжить выдачу:

```text
sequence > 123
```

Это особенно полезно при:

```text
network disconnect
proxy reconnect
browser reconnect
```

---

# 37. SSE reconnect

Клиент:

```text
GET /v1/jobs/{id}/events
Last-Event-ID: 1532
```

Gateway:

```text
Kafka
   |
   | sequence > 1532
   v
SSE
```

Таким образом клиент не теряет chunks.

---

# 38. Архитектура Go

Предлагаемая структура:

```text
cmd/
    gateway/
        main.go

internal/
    api/
        chat.go
        models.go
        jobs.go
        events.go

    router/
        router.go
        policy.go

    kafka/
        producer.go
        consumer.go
        topics.go

    jobs/
        manager.go
        state.go

    providers/
        interface.go

        heavy/
            client.go

        litellm/
            client.go

        ollama/
            client.go

    streaming/
        sse.go
        replay.go
        dispatcher.go

    storage/
        interface.go

    config/
        config.go

    observability/
        metrics.go
        logging.go

pkg/
    protocol/
        events.go
        openai.go
```

---

# 39. Provider interface

Все LLM providers должны иметь общий интерфейс.

Например концептуально:

```go
type Provider interface {
    Name() string

    Complete(
        ctx context.Context,
        request *ChatRequest,
        events chan<- ProviderEvent,
    ) error
}
```

Heavy provider отличается тем, что его выполнение запускается Worker.

---

# 40. Kafka abstraction

Kafka client должен быть скрыт за интерфейсом.

Например:

```go
type EventProducer interface {
    Publish(ctx context.Context, topic string, event Event) error
}
```

Это позволит тестировать Gateway без реального Kafka.

---

# 41. Конфигурация

Предлагаемый `.env`:

```env
# Gateway
HTTP_ADDR=:8080

# Kafka
KAFKA_BROKERS=REDACTED:9092

KAFKA_REQUEST_TOPIC=llm.requests
KAFKA_EVENTS_TOPIC=llm.events
KAFKA_COMPLETED_TOPIC=llm.completed
KAFKA_FAILED_TOPIC=llm.failed
KAFKA_DLQ_TOPIC=llm.dlq

KAFKA_CONSUMER_GROUP=gateway
KAFKA_WORKER_GROUP=heavy-llm-worker

# Heavy LLM
OPENAI_BASE_URL=https://llm.example.com/v1
OPENAI_MODEL=...

HEAVY_LLM_TIMEOUT=2h

# LiteLLM
LITELLM_URL=https://llm.example.com/v1
LITELLM_MODEL=gemma-4-E4B-it-GGUF

# Ollama
OLLAMA_URL=http://ollama.local:11434
OLLAMA_MODEL=llama3.2:3b-instruct-q4_K_M
OLLAMA_EMBED_MODEL=nomic-embed-text:latest

CONTEXT_WINDOW=8192
```

Секреты API keys должны храниться отдельно от `.env` в production:

```text
Kubernetes Secret
Docker Secret
Vault
environment secret
```

И не должны попадать:

```text
logs
Kafka payload
Git
metrics
traces
```

---

# 42. Метрики

Обязательные Prometheus metrics:

```text
llm_requests_total
llm_requests_active
llm_requests_failed_total

llm_generation_duration_seconds
llm_queue_duration_seconds

llm_tokens_total
llm_reasoning_tokens_total
llm_output_tokens_total

kafka_publish_total
kafka_publish_errors_total

kafka_consumer_lag

llm_client_disconnects_total

llm_reconnects_total
llm_replayed_events_total
```

Особенно важен:

```text
kafka_consumer_lag
```

---

# 43. Logging

Каждая запись должна содержать:

```text
request_id
provider
model
event_id
sequence
duration
```

Например:

```text
INFO request completed
request_id=01K7...
provider=heavy
model=Qwen...
duration=1842s
events=18432
```

Но никогда:

```text
Authorization: Bearer ...
API key
```

---

# 44. Distributed tracing

В будущем желательно использовать:

```text
OpenTelemetry
```

Trace:

```text
HTTP request
   |
   +--> Kafka publish
   |
   +--> worker
          |
          +--> llama.cpp
          |
          +--> Kafka events
   |
   +--> SSE
```

Request ID должен коррелировать с trace/span metadata.

---

# 45. Graceful shutdown

Gateway:

```text
SIGTERM
   |
   v
stop accepting new requests
   |
   v
finish active HTTP streams
   |
   v
commit Kafka offsets
   |
   v
shutdown
```

Worker:

```text
SIGTERM
   |
   v
stop receiving new jobs
   |
   v
finish current generation
   |
   v
publish final event
   |
   v
commit offset
   |
   v
shutdown
```

Для Kubernetes желательно использовать:

```text
terminationGracePeriodSeconds
```

достаточного размера.

---

# 46. Crash recovery Worker

Если Worker падает:

```text
Kafka request
      |
      v
Worker
      |
      X crash
```

Kafka offset не должен быть committed до момента, когда задача безопасно обработана.

После restart:

```text
Kafka
  |
  v
Worker
  |
  v
retry
```

Но это создаёт возможность повторного upstream generation.

Поэтому необходимо учитывать:

```text
request_id
job status
sequence
```

и, по возможности, provider-side idempotency.

---

# 47. Worker concurrency

Нельзя разрешать неограниченное количество heavy requests.

Например:

```env
HEAVY_WORKERS=2
```

или:

```env
HEAVY_MAX_CONCURRENCY=2
```

Причина:

```text
2 x Qwen 27B
```

могут быть приемлемы,

а:

```text
20 x Qwen 27B
```

могут полностью уничтожить latency и память GPU/CPU.

Kafka в данном случае становится настоящей очередью.

---

# 48. Очередь

Если одновременно пришло:

```text
20 heavy requests
```

Kafka хранит:

```text
job 1 -> processing
job 2 -> queued
job 3 -> queued
...
job 20 -> queued
```

Worker pool:

```text
worker 1 -> job 1
worker 2 -> job 2
```

После завершения:

```text
worker 1 -> job 3
```

---

# 49. Priority

На следующем этапе можно добавить priority.

Например:

```text
priority=high
priority=normal
priority=low
```

Но для MVP лучше не усложнять Kafka topology.

---

# 50. Dead Letter Queue

Если job невозможно обработать:

```text
llm.requests
      |
      v
worker
      |
      X
```

после N retries:

```text
llm.dlq
```

В DLQ сохраняется:

```text
request_id
original payload
error
attempt
timestamp
```

---

# 51. Retry policy

Не все ошибки нужно retry.

Retry:

```text
connection reset
timeout
502
503
504
temporary network error
```

Не retry:

```text
400
401
403
invalid model
invalid request
context too large
```

---

# 52. Security

Gateway должен поддерживать:

```text
Authorization: Bearer
```

и не передавать клиентские секреты в Kafka.

Kafka payload должен содержать только необходимые параметры запроса.

API keys providers:

```text
OPENAI_KEY
LITELLM_API_KEY
OLLAMA_API_KEY
```

должны находиться только на Gateway/Worker.

---

# 53. API compatibility

MVP должен поддержать:

```text
POST /v1/chat/completions
GET /v1/models
```

Параметры:

```text
model
messages
stream
temperature
top_p
max_tokens
max_completion_tokens
stop
presence_penalty
frequency_penalty
response_format
```

Необходимо сохранять unknown fields там, где это возможно, чтобы provider-specific параметры не терялись.

---

# 54. Streaming response format

Gateway должен выдавать OpenAI-compatible SSE.

Пример:

```text
data: {"id":"...","object":"chat.completion.chunk",...}

data: {"id":"...","object":"chat.completion.chunk",...}

data: [DONE]
```

При reasoning provider-specific field:

```json
{
  "choices": [
    {
      "delta": {
        "reasoning_content": "..."
      }
    }
  ]
}
```

Gateway не должен его удалять.

---

# 55. Не преобразовывать streaming без необходимости

Главное правило:

```text
provider SSE -> Kafka -> Gateway SSE
```

а не:

```text
provider SSE
    ->
Go object
    ->
new JSON
    ->
Kafka
    ->
new JSON
```

Первый вариант минимизирует риск потери данных.

---

# 56. Архив полного результата

После `completed` желательно иметь возможность собрать:

```text
raw events
   |
   v
Result Aggregator
   |
   v
Full response
```

В результате:

```json
{
  "request_id": "...",
  "reasoning": "...",
  "content": "...",
  "events": 18342,
  "usage": {},
  "completed": true
}
```

Kafka остаётся source of truth.

---

# 57. Retention

Для `llm.events` необходимо задать retention, соответствующий требованиям проекта.

Например:

```text
7 days
```

или:

```text
30 days
```

Если требуется долговременное хранение:

```text
Kafka
   |
   v
Object Storage
```

например:

```text
S3-compatible storage
```

Но это не входит в MVP.

---

# 58. Не использовать Kafka compacted topic для raw events

`llm.events` не должен быть обычным compacted topic.

Причина:

```text
request_id = key
```

и Kafka compaction потенциально оставит только последние записи для одного key.

Для event log необходим обычный retention-based topic.

---

# 59. Разделение control plane и data plane

Рекомендуется концептуально разделить:

### Control

```text
requests
completed
failed
```

### Data

```text
events
```

Это позволит позже масштабировать их независимо.

---

# 60. MVP

Первая версия должна содержать только:

```text
Go Gateway
Kafka Producer
Kafka Consumer
Heavy Worker
Heavy llama.cpp provider
LiteLLM provider
Ollama provider
OpenAI-compatible API
SSE streaming
request_id
sequence
raw event preservation
async jobs
replay
metrics
graceful shutdown
```

Не включать в MVP:

```text
complex AI router
vector DB
priority scheduler
S3 archival
multi-region
autoscaling
distributed tracing
```

---

# 61. Этап 1 — Kafka

Создать topics:

```text
llm.requests
llm.events
llm.completed
llm.failed
llm.dlq
```

Проверить:

```bash
kafkacat -L -b REDACTED:9092
```

Создать тестовый producer/consumer.

Проверить:

```text
message -> Kafka -> consumer
```

---

# 62. Этап 2 — Event protocol

Создать:

```text
internal/protocol/events.go
```

Определить:

```text
Event
Request
Completion
Failure
```

Реализовать:

```text
request_id
event_id
sequence
timestamp
payload
```

Добавить JSON serialization tests.

---

# 63. Этап 3 — Heavy Worker

Создать:

```text
cmd/worker/main.go
internal/providers/heavy/client.go
```

Worker должен:

```text
consume request
      |
      v
call llama.cpp
      |
      v
read SSE
      |
      v
publish every event
```

На этом этапе HTTP Gateway ещё не нужен.

---

# 64. Этап 4 — Gateway

Создать:

```text
cmd/gateway/main.go
internal/api/chat.go
```

Поддержать:

```text
POST /v1/chat/completions
```

Алгоритм:

```text
HTTP request
    |
    v
validate
    |
    v
request_id
    |
    v
Kafka publish
    |
    v
wait for events
    |
    v
SSE response
```

---

# 65. Этап 5 — Event Dispatcher

Создать:

```text
internal/streaming/dispatcher.go
```

Dispatcher получает:

```text
Kafka event
```

и ищет:

```text
request_id -> active HTTP stream
```

Если клиент подключён:

```text
Kafka -> SSE
```

Если клиента нет:

```text
Kafka -> retention
```

---

# 66. Этап 6 — Replay

Добавить:

```text
Last-Event-ID
```

и endpoint:

```text
GET /v1/jobs/{request_id}/events
```

Gateway должен уметь читать Kafka events начиная с нужной sequence.

---

# 67. Этап 7 — Async API

Добавить:

```text
Prefer: respond-async
```

и:

```text
POST /v1/jobs
GET  /v1/jobs/{request_id}
GET  /v1/jobs/{request_id}/events
```

Это решает проблему абсолютного HTTP timeout.

---

# 68. Этап 8 — LiteLLM

Добавить provider:

```text
internal/providers/litellm
```

Быстрые instruct-запросы не должны проходить через heavy Kafka queue.

---

# 69. Этап 9 — Ollama

Добавить:

```text
internal/providers/ollama
```

Поддержать:

```text
chat
embeddings
```

Embeddings:

```text
nomic-embed-text:latest
```

---

# 70. Этап 10 — Router

Реализовать простой deterministic router:

```text
model == heavy
    -> Kafka

model == gemma
    -> LiteLLM

model == llama3.2
    -> Ollama
```

Позже заменить его на complexity-based router.

---

# 71. Этап 11 — Observability

Добавить:

```text
Prometheus
structured logging
request correlation
Kafka lag
generation duration
queue duration
```

---

# 72. Этап 12 — Failure testing

Обязательно протестировать:

### Gateway crash

```text
Gateway -> crash
Worker -> continue
Kafka -> events preserved
```

### Worker crash

```text
Worker -> crash
Kafka -> request remains
Worker restart -> retry
```

### Client disconnect

```text
Client -> disconnect
Worker -> continue
Kafka -> full response
```

### Kafka temporary outage

```text
Producer -> retry
```

### llama.cpp timeout

```text
Worker -> failed
Kafka -> failed event
```

### Slow client

```text
Worker -> unaffected
Kafka -> buffer
Gateway -> stream
```

---

# 73. Acceptance tests

Система считается готовой, если выполняются следующие сценарии.

## Test 1

Запрос:

```text
heavy model
```

выполняется:

```text
> 30 min
```

и не теряется.

---

## Test 2

Клиент отключается через:

```text
5 min
```

После завершения:

```text
100% events
```

доступны через replay.

---

## Test 3

Gateway перезапускается во время generation.

После restart:

```text
request_id
```

остаётся известен.

---

## Test 4

Worker перезапускается.

Kafka сохраняет request.

---

## Test 5

Reasoning:

```text
reasoning_content
```

полностью сохраняется.

---

## Test 6

Обычный OpenAI-compatible клиент может отправить:

```text
/v1/chat/completions
```

без знания о Kafka.

---

## Test 7

Streaming client получает:

```text
SSE chunks
```

в правильном порядке.

---

## Test 8

Повторное подключение с:

```text
Last-Event-ID
```

не приводит к потере chunks.

---

# 74. Критические архитектурные решения

## Решение №1

Kafka — это не просто очередь.

Она используется как:

```text
durable event log
```

для генерации.

---

## Решение №2

Worker не связан с HTTP lifecycle.

```text
HTTP disconnect != LLM cancellation
```

---

## Решение №3

Каждый chunk сохраняется отдельно.

```text
chunk -> Kafka
```

---

## Решение №4

Reasoning не удаляется.

---

## Решение №5

Raw provider event желательно сохранять без преобразования.

---

## Решение №6

Все события имеют:

```text
request_id
sequence
event_id
```

---

## Решение №7

Для запросов > HTTP timeout используется async API.

---

# 75. Главный поток данных

Итоговый flow:

```text
                 CLIENT
                    |
                    | OpenAI API
                    v
             ┌───────────────┐
             │  Go Gateway   │
             └───────┬───────┘
                     |
                     | request_id
                     |
                     v
             ┌───────────────┐
             │    Kafka      │
             │ llm.requests  │
             └───────┬───────┘
                     |
                     v
             ┌───────────────┐
             │ Heavy Worker  │
             └───────┬───────┘
                     |
                     | SSE
                     v
             ┌───────────────┐
             │   llama.cpp   │
             │ Qwen 27B      │
             └───────┬───────┘
                     |
                     | raw events
                     v
             ┌───────────────┐
             │    Kafka      │
             │  llm.events   │
             └───────┬───────┘
                     |
                     v
             ┌───────────────┐
             │ Event Manager │
             └───────┬───────┘
                     |
                     | SSE
                     v
                  CLIENT
```

При этом Kafka хранит:

```text
request
+
reasoning chunks
+
content chunks
+
tool events
+
usage
+
completed
+
failed
```

---

# 76. Итоговая гарантия

При корректно настроенной Kafka система должна обеспечивать:

```text
HTTP timeout
       !=
LLM timeout
```

и:

```text
client disconnect
       !=
LLM cancellation
```

а также:

```text
Gateway restart
       !=
loss of generation
```

и:

```text
Worker restart
       !=
loss of committed Kafka events
```

Главная гарантия архитектуры:

> **После успешной записи event в Kafka этот event не должен зависеть от состояния HTTP-соединения клиента.**

---

# 77. Что необходимо реализовать первым

Порядок разработки:

```text
1. Kafka topics
2. Event protocol
3. Heavy Worker
4. llama.cpp SSE client
5. Raw event preservation
6. Gateway
7. Kafka -> SSE dispatcher
8. request_id / sequence
9. replay
10. async API
11. LiteLLM
12. Ollama
13. router
14. metrics
15. failure tests
```

Не следует начинать с router или UI.

Критическая часть системы — это:

```text
Kafka
+
Worker
+
event protocol
+
durable streaming
```

Именно она решает исходную проблему долгого reasoning-запроса.

### Важное замечание по архитектуре

Есть один принципиальный момент: **Kafka сама по себе не делает обычный HTTP-запрос бессрочным**. Если внешний reverse proxy действительно принудительно обрывает HTTP через 1000 секунд, то поток:

```text
POST /v1/chat/completions
        ↓
SSE 30–40 минут
```

физически не может быть гарантирован. Поэтому я заложил два режима:

* **обычный OpenAI/llama.cpp-compatible streaming** — для запросов, которые укладываются в инфраструктурный timeout;
* **async job + replayable SSE** — для тяжёлых запросов на 30+ минут.

При этом **ни один chunk, включая `reasoning_content`, не должен зависеть от существования клиентского TCP-соединения**: сначала он становится событием Kafka, а уже затем доставляется клиенту.

И ещё: в сообщении был передан API-ключ провайдера. Если это реальный рабочий ключ, его стоит **сразу ротировать** и больше не хранить в `PLAN.md`, Git, Kafka events или логах.

