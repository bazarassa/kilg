# 🤖 Проект: KILG

## 🌟 Обзор и Цель
Kafka Interference LLM Gateway - Kafka-backed gateway for routing chat completion requests to different LLM providers.
Это клиент-серверная система, предоставляющая простейший OpenAI-compatible API, обмен запросами в котором осуществляется с использованмем надежного Kafka посредника, что гарантирует отправку/получение ответов от удаленных LLM даже при сетевых сбоях.

## 🛠 ️ Инструменты и Сборка (Build & Toos)

* **Зависимости ПО:** `apt install make #or yum install make`
* **Зависимости GO:** `See https://go.dev/doc/install && go version >= 1.24`
* **Установка:** `git clone https://github.com/bazarassa/kilg.git && cd kilg && make install`
* **Установить все сразу:** `make install`
* **Поставить зависимости(swag and moq):** `make .install-deps`
* **Поставить линтеры(golangci-lint):** `make .install-lint`
* **Запустить линтеры:** `make lint`
* **Запустить форматирование:** `make fmt`
* **Запустить форматирование:** `make vet`

## 🧾 Тесты и доументация (Tests & Docs)
* **Тесты:**
    * `make test`: Быстрый срез для обратной связи.
    * `make test-race`: Полный набор тестов.  
    * `make test-coverage`: Тесты с покрытием.
* **Покрытие:** Во время работы — тесты затронутых файлов; перед сообщением о готовности — полный прогон.
* **Проверки:** Не обходи хуки и не отключай проверки, чтобы они прошли.


* **Документация:** `make docs`
* **Сборка linux:** `make build-linux`
* **Сборка darwin:** `make build-mac`

## 🚀 Запуск и остановка (Start & Stop)
* **Запуск gateway:** `mkdir -p /tmp/llmtest && KAFKA_BROKERS=kafka.llm.net:9092 \
	KAFKA_ADDRESS_REWRITE=kafka.llm.net:9093=kafka.llm.net:9092 \
	KAFKA_CONSUMER_START=earliest \
	HTTP_ADDR=:18080 \
	LOG_LEVEL=info \
	OPENAI_BASE_URL=https://deep.llm.net/v1 \
	OPENAI_MODEL=mock-model \
	nohup ./bin/gateway-kilg-darwin-amd64 \
		> /tmp/llmtest/gateway.log 2>&1 &`

* **Запуск worker:** `mkdir -p /tmp/llmtest && @KAFKA_BROKERS=kafka.llm.net:9092 \
	KAFKA_ADDRESS_REWRITE=kafka.llm.net:9093=kafka.llm.net:9092 \
	KAFKA_CONSUMER_START=earliest \
	WORKER_HTTP_ADDR=:18081 \
	LOG_LEVEL=debug \
	nohup ./bin/worker-kilg-darwin-amd64 \
		> /tmp/llmtest/worker.log 2>&1 &`
* **Запуск worker и gateway:** `make run`


* **Остановка gateway:** `pgrep -f '[/](gateway-kilg-darwin-amd64|gateway-kilg-linux-amd64)$$' | xargs -r kill 2>/dev/null || true`
* **Остановка worker:** `pgrep -f '[/](worker-kilg-darwin-amd64|worker-kilg-linux-amd64)$$' | xargs -r kill 2>/dev/null || true`
* **Остановка worker и gateway:** `make stop`

## ⚙️ Поведение и Логика Агента (Agent Behavior)
* **Сначала разберись:** Всегда начинай с чтения кода и места вызова. Если трактовки ведут к разному результату, спроси пользователя.
* **Архитектура:** Используй `[Документация]`. Если нет, спроси пользователя и запиши ответ в `CLAUDE.md` (или эквивалент).
* **Пошаговые действия:** Всегда следуй протоколу: `[Оценка] -> [Проверка] -> [Финальный прогон]`.
* **Обработка сбоев (Blame Protocol):** Если сквозная проверка упала на ветке, сначала проверь на `main`. Если на `main` проходит — причина в наших изменениях. 
Если на `main` нельзя запустить — вынеси это в риски. **Не объявляй сбой без доказательств.**

## 📜 Git и Коммиты (Commit & Versioning)
* **Формат коммитов:** `Conventional Commits`. 
* **Запреты:** Никогда не коммить без явной просьбы. Не делай force-push, reset --hard или удаление файлов вне задачи без разрешения.
* **Версионирование:** `<Инструкции_по_версии>`. По умолчанию используй `PATCH`.
* **Гигиена коммитов:** Добавляй только конкретные файлы, а не всё рабочее дерево.

## 🛡️ Безопасность и Ограничения
* **Необратимые изменения:** Всегда проверяй, что работа не является необратимой (например, при коммите/деплое), пока не получишь явного подтверждение.
* **Ограничения каталогов:** Убедись, что изменения касаются нужных зон (если применимо).

## 🧠 Управление Знаниями и Ссылки
* **[Особые навыки/модули]:** `<Описание_модуля_или_навыка>`.
* **Внешние ссылки:** Если есть специфические правила для Claude Code, смотри `CLAUDE.md`.

