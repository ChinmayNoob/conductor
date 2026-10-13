# Developer tasks. Tools that aren't commonly installed (protoc, golangci-lint)
# run in Docker so every contributor gets the same versions.

GOLANGCI_LINT_IMAGE ?= golangci/golangci-lint:v2.5.0
PROTOC_IMAGE        ?= conductor-protoc
CONTROLLER_GEN      ?= go run sigs.k8s.io/controller-tools/cmd/controller-gen@v0.22.0
COMPOSE             ?= docker compose
WORKERS             ?= 3

.PHONY: help build test test-db lint fmt proto up down logs e2e chaos bench clean \
        operator operator-generate helm-lint kind-up kind-down e2e-k8s docs docs-publish

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-18s %s\n", $$1, $$2}'

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

CHAOS_FILES := -f docker-compose.yml -f docker-compose.chaos.yml

chaos: ## Run the chaos suite (Toxiproxy in front of Postgres; restarts the stack)
	$(COMPOSE) $(CHAOS_FILES) --profile containers up -d --build --wait --scale worker=$(WORKERS)
	E2E_COMPOSE_FILES=docker-compose.yml,docker-compose.chaos.yml go test -tags 'e2e chaos' -count=1 -v -timeout 30m -run TestChaos ./test/e2e/

bench: ## Benchmark the stack at 1, 10 and 50 workers (see BENCHMARKS.md)
	./scripts/bench.sh

# --- Kubernetes ---

operator: ## Build and test the operator (its own module)
	cd operator && go build ./... && go test -race ./...

operator-generate: ## Regenerate the operator's deepcopy code and CRDs, and the chart's copies
	cd operator && $(CONTROLLER_GEN) object paths=./api/v1alpha1 && \
		$(CONTROLLER_GEN) crd paths=./api/v1alpha1 output:crd:artifacts:config=config/crd
	cp operator/config/crd/*.yaml deploy/helm/conductor/files/crds/

helm-lint: ## Lint the chart and check it renders in every mode
	helm lint deploy/helm/conductor
	go test ./deploy/...

kind-up: ## Run Conductor on a local kind cluster (builds images, installs KEDA and the chart)
	./scripts/kind-up.sh

kind-down: ## Delete the kind cluster
	kind delete cluster --name conductor

e2e-k8s: kind-up ## Run the end-to-end suite on the kind cluster
	E2E_PLATFORM=kubernetes E2E_API_URL=http://localhost:30081 E2E_API_KEY=$$(./scripts/kind-up.sh --api-key) \
		E2E_LLM=mock go test -tags e2e -count=1 -v -timeout 40m ./test/e2e/

# --- Documentation ---

docs: ## Build the documentation pages into docs/slashdocs/dist
	go run ./scripts/docs build

docs-publish: ## Publish the documentation to slashdocs (SLASHDOCS_SERVER, default localhost:8080)
	go run ./scripts/docs publish

clean: ## Remove build output
	rm -rf bin
