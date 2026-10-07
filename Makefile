# Developer tasks. Tools that aren't commonly installed (protoc, golangci-lint)
# run in Docker so every contributor gets the same versions.

GOLANGCI_LINT_IMAGE ?= golangci/golangci-lint:v2.5.0
PROTOC_IMAGE        ?= conductor-protoc
COMPOSE             ?= docker compose
WORKERS             ?= 3

.PHONY: help build test test-db lint fmt proto up down logs e2e clean

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-10s %s\n", $$1, $$2}'

build: ## Build all binaries into ./bin
	go build -o bin/ ./cmd/...

test: ## Run unit tests
	go test -race ./...

test-db: ## Run database tests against a throwaway Postgres container (needs Docker)
	go test -race -tags dbtest ./pkg/db/...

lint: ## Run golangci-lint
	docker run --rm -v "$(CURDIR):/app" -w /app $(GOLANGCI_LINT_IMAGE) golangci-lint run ./...

fmt: ## Format all Go code
	gofmt -w cmd pkg test

proto: ## Regenerate gRPC code from pkg/grpcapi/api.proto
	docker build -q -t $(PROTOC_IMAGE) -f scripts/protoc.Dockerfile scripts
	docker run --rm -v "$(CURDIR):/src" -w /src $(PROTOC_IMAGE) \
		protoc --go_out=. --go_opt=paths=source_relative \
		       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
		       pkg/grpcapi/api.proto

up: ## Start the stack with $(WORKERS) workers
	$(COMPOSE) up -d --build --scale worker=$(WORKERS)

down: ## Stop the stack and delete its data
	$(COMPOSE) down -v

logs: ## Follow logs from all services
	$(COMPOSE) logs -f

e2e: up ## Run the end-to-end suite against the running stack
	go test -tags e2e -count=1 -v -timeout 15m ./test/e2e/...

clean: ## Remove build output
	rm -rf bin
