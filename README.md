# KILG 

Kafka Interference LLM Gateway - это Kafka-backed gateway for routing chat completion requests to different LLM providers.

Проект находится на начальной стадии разработки. Основная архитектура уже собрана и покрыта базовыми тестами, однако API, конфигурация, обработка ошибок и внутренние контракты ещё могут изменяться без обратной совместимости.

## Статус проекта

**Early development / начальная стадия разработки**

Сейчас проект представляет собой рабочий прототип инфраструктурного LLM gateway со следующими компонентами:

- HTTP API в OpenAI-compatible стиле.
- Асинхронная обработка запросов через Kafka.
- Потоковая выдача результатов через Server-Sent Events.
- Хранение состояния jobs в памяти.
- Повторное подключение к SSE-потоку через `Last-Event-ID`.
- Базовая идемпотентность через `Idempotency-Key`.
- Поддержка нескольких backend-провайдеров.
- Отдельный gateway и worker процессы.
- Конфигурация через environment variables.
- Набор unit-тестов для API, Kafka fake-компонентов, job manager, routing, streaming и worker.

Проект пока не следует считать production-ready.

## Возможности

### HTTP API

Поддерживаемые endpoint’ы:

```text
GET    /health
GET    /ready
GET    /metrics

GET    /v1/health

GET    /v1/models
GET    /v1/models/{model}

POST   /v1/chat/completions

GET    /v1/jobs
GET    /v1/jobs/{id}
DELETE /v1/jobs/{id}
GET    /v1/jobs/{id}/events

GET    /v1/chat/completions/{id}
DELETE /v1/chat/completions/{id}
GET    /v1/chat/completions/{id}/messages
```

### Chat Completions

Основной endpoint:

```text
POST /v1/chat/completions
```

Пример запроса с явным указанием модели:

```bash
curl -N -sS http://REDACTED:8080/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer mp123' \
  -d '{
    "model": "heavy",
    "messages": [
      {
        "role": "system",
        "content": "Ты популяный детский писатель."
      },
      {
        "role": "user",
        "content": "Расскажи историю про программиста и кота"
      }
    ]
  }'
```

Параметр `model` используется как логическое имя маршрута gateway, а не обязательно как физическое имя модели провайдера.

Примеры логических маршрутов:

```text
heavy
litellm
ollama
```

Физическое имя модели для каждого провайдера задаётся в конфигурации отдельно.

### Модель по умолчанию

Если поле `model` не передано, gateway может использовать:

```text
DEFAULT_MODEL
```

Например:

```bash
export DEFAULT_MODEL=heavy
```

В этом случае запрос без `model` будет обработан так, как будто клиент передал:

```json
{
  "model": "heavy"
}
```

Если `model` отсутствует и `DEFAULT_MODEL` не задан, gateway возвращает ошибку `400`.

Поддержка запросов без `model` является расширением gateway. В стандартном OpenAI-compatible API клиент обычно передаёт модель явно.

### Асинхронный режим

Для постановки запроса в Kafka без ожидания генерации используется


MIT Licence

Authored by KILG, 2026
