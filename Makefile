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

# Serve Swagger UI locally
.PHONY: swagger
swagger: docs
	@echo "Serving Swagger UI on http://REDACTED:8081/swagger/"
	@docker run --rm -p 8081:8080 -e SWAGGER_JSON=/docs/swagger.json -v $(PWD)/docs:/docs swaggerapi/swagger-ui

.PHONY: test
test:   ## Run fast tests
	go test -timeout 30s ./...

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

	@KAFKA_BROKERS=REDACTED:9092 \
	KAFKA_ADDRESS_REWRITE=REDACTED:9093=REDACTED:9092 \
	KAFKA_CONSUMER_START=earliest \
	HTTP_ADDR=:18080 \
	LOG_LEVEL=info \
	OPENAI_BASE_URL=http://REDACTED:19090/v1 \
	OPENAI_MODEL=mock-model \
	nohup $(BINARY_PATH_GATEWAY) \
		> /tmp/llmtest/gateway.log 2>&1 &

	@KAFKA_BROKERS=REDACTED:9092 \
	KAFKA_ADDRESS_REWRITE=REDACTED:9093=REDACTED:9092 \
	KAFKA_CONSUMER_START=earliest \
	WORKER_HTTP_ADDR=:18081 \
	LOG_LEVEL=debug \
	nohup $(BINARY_PATH_WORKER) \
		> /tmp/llmtest/worker.log 2>&1 &

	@echo "LLM gateway and worker started - See /tmp/llmtest/gateway.log and /tmp/llmtest/worker.log"

.PHONY: start
start: build-mac
	@mkdir -p /tmp/llmtest
	
	@KAFKA_BROKERS=REDACTED:9092 \
	KAFKA_ADDRESS_REWRITE=REDACTED:9093=REDACTED:9092 \
	KAFKA_CONSUMER_START=earliest \
	HTTP_ADDR=:18080 \
	LOG_LEVEL=info \
	OPENAI_BASE_URL=http://REDACTED:19090/v1 \
	OPENAI_MODEL=mock-model \
	nohup $(BINARY_PATH_GATEWAY)-kilg-darwin-amd64 \
		> /tmp/llmtest/gateway.log 2>&1 &
	 
	@KAFKA_BROKERS=REDACTED:9092 \
	KAFKA_ADDRESS_REWRITE=REDACTED:9093=REDACTED:9092 \
	KAFKA_CONSUMER_START=earliest \
	WORKER_HTTP_ADDR=:18081 \
	LOG_LEVEL=debug \
	nohup $(BINARY_PATH_WORKER)-kilg-darwin-amd64 \
		> /tmp/llmtest/worker.log 2>&1 &

	@echo "LLM gateway and worker started - See /tmp/llmtest/gateway.log and /tmp/llmtest/worker.log"

.PHONY: stop
stop:   ## Stop LLM gateway and worker process
	@pgrep -f '[/](gateway-kilg-darwin-amd64|gateway-kilg-darwin-amd64)$$' | xargs -r kill 2>/dev/null || true
	@pgrep -f '[/](worker-kilg-darwin-amd64|worker-kilg-darwin-amd64)$$' | xargs -r kill 2>/dev/null || true
	@echo "LLM gateway and worker stopped"
	
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

