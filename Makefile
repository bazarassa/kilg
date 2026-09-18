PROJECT_DIR := $(CURDIR)
PROJECT_BIN := $(PROJECT_DIR)/bin

BINARY_NAME_GATEWAY := gateway
BINARY_NAME_WORKER := worker

BINARY_PATH_GATEWAY := $(PROJECT_BIN)/$(BINARY_NAME_GATEWAY)
BINARY_PATH_WORKER := $(PROJECT_BIN)/$(BINARY_NAME_WORKER)

MOQ := $(PROJECT_BIN)/moq
MOQ_VERSION := v0.3.1

GOLANGCI_LINT := $(PROJECT_BIN)/golangci-lint
GOLANGCI_LINT_VERSION := v2.13.2

.PHONY: .install-moq
.install-moq:
	@echo "Installing moq..."
	@mkdir -p $(PROJECT_BIN)
	@test -x $(MOQ) || \
		GOBIN=$(PROJECT_BIN) go install github.com/matryer/moq@$(MOQ_VERSION)

.PHONY: .install-linter
.install-linter:
	@echo "Installing golangci-lint $(GOLANGCI_LINT_VERSION)..."
	@mkdir -p $(PROJECT_BIN)
	@test -x $(GOLANGCI_LINT) || \
		curl -sSfL https://golangci-lint.run/install.sh | \
		sh -s -- -b $(PROJECT_BIN) $(GOLANGCI_LINT_VERSION)

.PHONY: lint
lint: .install-linter
	GOFLAGS=-buildvcs=false \
		$(GOLANGCI_LINT) run ./... --config=./.golangci.yml

.PHONY: lint-fast
lint-fast: .install-linter
	GOFLAGS=-buildvcs=false \
		$(GOLANGCI_LINT) run ./... --fast --config=./.golangci.yml

.PHONY: install-env
install-env: .install-moq .install-linter

.PHONY: test
test:
	GOFLAGS=-buildvcs=false go test ./...

# Сборка для Linux (amd64) – go build -o bin/gateway ./cmd/gateway  и  go build -o bin/worker ./cmd/worker
.PHONY: build
build:
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
.PHONY: build-linux-amd64
build-linux:
        CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
                go build \
                -buildvcs=false \
                -o $(BUILD_DIR)/klg-gateway-linux-amd64 \
                ./cmd/gateway  \
                && \
                go build \
                -buildvcs=false \
                -o $(BUILD_DIR)/klg-worker-linux-amd64 \
                ./cmd/worker

# Сборка для MAC (amd64)
.PHONY: build-darwin-amd64
build-linux:
	CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 \
                go build \
		-buildvcs=false \
		-o $(BUILD_DIR)/klg-gateway-darwin-amd64 \
		./cmd/gateway \
                && \
	        go build \
		-buildvcs=false \
		-o $(BUILD_DIR)/klg-worker-darwin-amd64 \
		./cmd/worker

.PHONY: build-ci
build-ci:
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

.PHONY: kill
kill:
	@pkill -f 'bin/worker$$' 2>/dev/null || true
	@pkill -f 'bin/gateway$$' 2>/dev/null || true
	@echo "LLM gateway and worker stopped"

.PHONY: clean
clean:
	rm -rf $(PROJECT_BIN_GATEWAY) && rm -rf $(BINARY_PATH_WORKER)

