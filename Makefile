PROJECT_DIR := $(CURDIR)
PROJECT_BIN := $(PROJECT_DIR)/bin
PROJECT_DOCS := $(PROJECT_DIR)/docs

BINARY_NAME_GATEWAY := gateway
BINARY_NAME_WORKER := worker

BINARY_PATH_GATEWAY := $(PROJECT_BIN)/$(BINARY_NAME_GATEWAY)
BINARY_PATH_WORKER := $(PROJECT_BIN)/$(BINARY_NAME_WORKER)

MOQ := $(PROJECT_BIN)/moq
MOQ_VERSION := v0.3.1

GOLANGCI_LINT := $(PROJECT_BIN)/golangci-lint
GOLANGCI_LINT_VERSION := v2.13.2

GO ?= go
GOBIN := $(shell $(GO) env GOBIN)
ifeq ($(strip $(GOBIN)),)
GOBIN := $(shell $(GO) env GOPATH)/bin
endif

SWAG_BIN := $(GOBIN)/swag

VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo "1.0.0")
BUILD_TIME := $(shell date -u '+%Y-%m-%d_%H:%M:%S')
GIT_COMMIT := $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")

ENV_FILE ?= .env

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'

.PHONY: .install-deps 
.install-deps: ## Install dependencies
	@echo "Installing dependencies..."
	go mod download
	go mod tidy

.install-swag:
	@echo "Installing swag to $(SWAG_BIN)"
	@mkdir -p "$(GOBIN)"
	$(GO) install github.com/swaggo/swag/cmd/swag@latest

.PHONY: .install-moq
.install-moq:  ## Install moq
	@echo "Installing moq..."
	@mkdir -p $(PROJECT_BIN)
	@test -x $(MOQ) || \
		GOBIN=$(PROJECT_BIN) go install github.com/matryer/moq@$(MOQ_VERSION)

.PHONY: .install-linter
.install-linter: ## Install linter
	@echo "Installing golangci-lint $(GOLANGCI_LINT_VERSION)..."
	@mkdir -p $(PROJECT_BIN)
	@test -x $(GOLANGCI_LINT) || \
		curl -sSfL https://golangci-lint.run/install.sh | \
		sh -s -- -b $(PROJECT_BIN) $(GOLANGCI_LINT_VERSION)

.PHONY: lint
lint: .install-linter
	GOFLAGS=-buildvcs=false $(GOLANGCI_LINT) run ./... --config=./.golangci.yml

.PHONY: lint-fast
lint-fast: .install-linter
	GOFLAGS=-buildvcs=false $(GOLANGCI_LINT) run ./... --fast --config=./.golangci.yml

.PHONY: install
install: .install-deps .install-swag .install-moq .install-linter

.PHONY: fmt
fmt: ## Format code fmt
	@echo "Formatting code..."
	go fmt ./...

.PHONY: vet
vet: ## Check code vet
	@echo "Formatting code..."
	go vet ./...

docs: .install-swag
	@echo "Using Swagger binary: $(SWAG_BIN)"
	@test -x "$(SWAG_BIN)" || { \
		echo "Error: swag was not found at $(SWAG_BIN)"; \
		exit 1; \
	}
	@echo "Generating Swagger documentation..."
	@mkdir -p "$(PROJECT_DOCS)"
	"$(SWAG_BIN)" init -g cmd/gateway/main.go -o "$(PROJECT_DOCS)"

# Serve Swagger UI locally (Опционально не тестировалось)
.PHONY: swagger
swagger: docs
	@echo "Serving Swagger UI on http://localhost:8081/swagger/"
	@docker run --rm -p 8081:8080 -e SWAGGER_JSON=/docs/swagger.json -v $(PWD)/docs:/docs swaggerapi/swagger-ui

#ТЕСТЫ коротко
.PHONY: test
test:   ## Run fast tests
	go test -timeout 30s ./...

#ТЕСТЫ полностью
.PHONY: test-race
test-race: ## Run race tests
	@echo "Running tests..."
	go test -v -race -covermode=atomic -coverpkg=./... -coverprofile=coverage.out ./...
	@echo "Coverage report: coverage.out"

.PHONY: test-watch
test-watch: ## Run tests in watch mode (requires watcher)
	@go test -v ./... -watch

.PHONY: test-coverage
test-coverage: test ## Run tests with coverage report
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report: coverage.html"
	
.PHONY: test-bench
test-bench: ## Run benchmarks
	go test -bench=. -benchmem ./...

# Сборка для Linux (amd64) – go build -o bin/gateway ./cmd/gateway  и  go build -o bin/worker ./cmd/worker
.PHONY: build
build:  ## Builds for Linux (amd64) file name bin/gateway and bin/worker
	@mkdir -p $(PROJECT_BIN)
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
		go build \
		-buildvcs=false \
		-o $(BINARY_PATH_GATEWAY) \
		./cmd/gateway \
                && \
                go build \
                -buildvcs=false \
                -o $(BINARY_PATH_WORKER) \
                ./cmd/worker

# Сборка для Linux (amd64)
.PHONY: build-linux
build-linux: ## Builds for Linux (amd64) file name bin/gateway-kilg-linux-amd64 and bin/worker-kilg-linux-amd64
	@mkdir -p $(PROJECT_BIN)
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
		go build \
		-buildvcs=false \
		-o $(PROJECT_BIN)/$(BINARY_NAME_GATEWAY)-kilg-linux-amd64 \
		./cmd/gateway \
		&& \
		go build \
		-buildvcs=false \
		-o $(PROJECT_BIN)/$(BINARY_NAME_WORKER)-kilg-linux-amd64 \
		./cmd/worker

# Сборка для MAC (amd64)
.PHONY: build-mac
build-mac: ## Builds for Mac (amd64) file name bin/gateway-kilg-darwin-amd64 and bin/worker-kilg-darwin-amd64
	@mkdir -p $(PROJECT_BIN)
	CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 \
		go build \
		-buildvcs=false \
		-o $(PROJECT_BIN)/$(BINARY_NAME_GATEWAY)-kilg-darwin-amd64 \
		./cmd/gateway \
                && \
	        go build \
		-buildvcs=false \
		-o $(PROJECT_BIN)/$(BINARY_NAME_WORKER)-kilg-darwin-amd64 \
		./cmd/worker

.PHONY: build-ci
build-ci: ## Builds for CI Linux (amd64) file name bin/gateway and bin/worker 
	@mkdir -p $(PROJECT_BIN)
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
		go build \
		-buildvcs=false \
		-ldflags "-s -w -X main.commit=$(CI_COMMIT_SHORT_SHA) -X main.buildDate=$(shell date -u +%Y-%m-%dT%H:%M:%SZ)" \
		-o $(BINARY_PATH_GATEWAY) \
		./cmd/gateway \
                && \
                go build \
                -buildvcs=false \
                -ldflags "-s -w -X main.commit=$(CI_COMMIT_SHORT_SHA) -X main.buildDate=$(shell date -u +%Y-%m-%dT%H:%M:%SZ)" \
                -o $(BINARY_PATH_WORKER) \
                ./cmd/worker

.PHONY: run
run: build
	@mkdir -p /tmp/llmtest

	@KAFKA_BROKERS=31.28.27.171:9092 \
	KAFKA_ADDRESS_REWRITE=31.28.27.171:9093=31.28.27.171:9092 \
	KAFKA_CONSUMER_START=earliest \
	HTTP_ADDR=:18080 \
	LOG_LEVEL=info \
	OPENAI_BASE_URL=http://REDACTED:19090/v1 \
	OPENAI_MODEL=mock-model \
	nohup $(BINARY_PATH_GATEWAY) \
		> /tmp/llmtest/gateway.log 2>&1 &

	@KAFKA_BROKERS=REDACTED:9092 \
	KAFKA_ADDRESS_REWRITE=31.28.27.171:9093=31.28.27.171:9092 \
	KAFKA_CONSUMER_START=earliest \
	WORKER_HTTP_ADDR=:18081 \
	LOG_LEVEL=debug \
	nohup $(BINARY_PATH_WORKER) \
		> /tmp/llmtest/worker.log 2>&1 &

	@echo "LLM gateway and worker started - See /tmp/llmtest/gateway.log and /tmp/llmtest/worker.log"

.PHONY: start
start: build-mac
	@mkdir -p /tmp/llmtest
	
	@export KAFKA_BROKERS=31.28.27.171:9092 \
	KAFKA_ADDRESS_REWRITE=31.28.27.171:9093=31.28.27.171:9092 \
	KAFKA_CONSUMER_START=earliest \
	HTTP_ADDR=:18080 \
        CONTEXT_WINDOW=8192 \
        SSE_BUFFER_SIZE=1024 \
	LOG_LEVEL=info \
	OPENAI_BASE_URL=https://deep.llm.net/v1 \
        OPENAI_MODEL=HauhauCS/Qwen3.8-27B-Uncensored-HauhauCS-Aggressive-MTP-GGUF:Q6_K_P \
        OPENAI_KEY=mpQ \
        OPENAI_MODEL=mock-model \
        HEAVY_LLM_TIMEOUT=2h \
        HEAVY_LLM_IDLE_TIMEOUT=10m \
        HEAVY_WORKERS=2 \
        HEAVY_MAX_RETRY=3 \
        HEAVY_HEARTBEAT=30s \
        LITELLM_URL=http://31.28.27.171:8081/v1 \
        LITELLM_MODEL=/models/models/gemma-4-E4B/gemma-4-E4B-it-ultra-uncensored-heretic-Q2_K_XL.gguf \
        LITELLM_API_KEY=mpQ \
        LITELLM_TIMEOUT=5m \
        OLLAMA_URL=http://31.28.27.171:11434 \
        OLLAMA_MODEL=llama3.2:3b-instruct-q4_K_M \
        OLLAMA_EMBED_MODEL=nomic-embed-text:latest \
        OLLAMA_API_KEY=ollama \
        OLLAMA_TIMEOUT=5m \

	@test -f "$(ENV_FILE)" || { echo "Missing $(ENV_FILE)"; exit 1; }
	@set -a; . "$(ENV_FILE)"; set +a; nohup $(BINARY_PATH_GATEWAY)-kilg-darwin-amd64 \
		> /tmp/llmtest/gateway.log 2>&1 &
	 
	@export KAFKA_BROKERS=31.28.27.171:9092 \
	KAFKA_ADDRESS_REWRITE=31.28.27.171:9093=31.28.27.171:9092 \
	KAFKA_CONSUMER_START=earliest \
	WORKER_HTTP_ADDR=:18081 \
	LOG_LEVEL=debug \
        
	@test -f "$(ENV_FILE)" || { echo "Missing $(ENV_FILE)"; exit 1; }
	@set -a; . "$(ENV_FILE)"; set +a; nohup $(BINARY_PATH_WORKER)-kilg-darwin-amd64 \
		> /tmp/llmtest/worker.log 2>&1 &

	@echo "LOG: LLM gateway and worker started - See /tmp/llmtest/gateway.log and /tmp/llmtest/worker.log"

.PHONY: stop
stop:   ## Stop LLM gateway and worker process
	@pgrep -f '[/](gateway-kilg-darwin-amd64|gateway-kilg-linux-amd64)$$' | xargs -r kill 2>/dev/null || true
	@pgrep -f '[/](worker-kilg-darwin-amd64|worker-kilg-linux-amd64)$$' | xargs -r kill 2>/dev/null || true
	@echo "LOG: LLM gateway and worker stopped"
	
.PHONY: kill
kill:   ## Kill LLM gateway and worker process
	@pkill -f 'bin/worker$$' 2>/dev/null || true
	@pkill -f 'bin/gateway$$' 2>/dev/null || true
	@echo "LLM gateway and worker stopped"

.PHONY: clean
clean:
	rm -rf $(PROJECT_BIN_GATEWAY) && rm -rf $(BINARY_PATH_WORKER)
	rm -f coverage.out coverage.html
	rm -f docs/swagger.json docs/swagger.yaml

