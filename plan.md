# Conductor: Roadmap to an Open-Source Workflow Engine

## Vision

**A self-hosted, Postgres-only durable workflow engine for background jobs and AI agents.**

Teams that need reliable multi-step jobs today choose between heavy platforms (Temporal, Airflow) and simple queues that have no workflows. Conductor aims for the middle: durable workflows, retries, sagas and DAGs, with **Postgres as the only required dependency** and a setup that takes 5 minutes.

### Design principles

These rules apply to every phase. When a decision conflicts with one of them, the principle wins unless we deliberately revise it here.

1. **Postgres is the only required dependency.** Use advisory locks, `LISTEN/NOTIFY` and `SKIP LOCKED` instead of adding etcd, Kafka or Redis. Integrations with other systems must stay optional.
2. **At-least-once delivery, with honest documentation.** Tasks may run more than once. Give users idempotency keys and attempt IDs so their side effects can be made safe.
3. **Secure by default.** Use authentication, TLS and sandboxed execution by default. User input never reaches a shell unescaped.
4. **Easy to start, possible to scale.** A single binary runs everything for development. The same code scales out to separate services and Kubernetes.
5. **Observable by default.** Every task and workflow emits metrics, traces and logs out of the box.

### Status legend

✅ done · 🟡 in progress · ⬜ not started

---

## Phase 0: Foundation ✅ (done)

**Goal:** a working distributed task scheduler with saga workflows.

### Original build
- ✅ Three services: Scheduler (HTTP API), Coordinator (gRPC brain) and Workers (gRPC executors)
- ✅ PostgreSQL queue with atomic claiming via `SELECT … FOR UPDATE SKIP LOCKED`
- ✅ Task priorities (1–10), delayed or scheduled tasks, timeouts, and captured output
- ✅ Automatic retries with a configurable count and delay
- ✅ Worker registry kept up to date by heartbeats
- ✅ Saga workflows that run compensation steps in reverse order on failure (`trip_booking`, `trip_booking_fail`)
- ✅ Docker Compose stack and a CLI client

### Bug-fix round (PR #1, merged into `master` as `ceb2074`)
- ✅ Dispatch hands out tasks until the queue is empty, instead of 1 task per second for the whole cluster
- ✅ A requeued task clears `picked_at`, so it is no longer stuck for 5 minutes
- ✅ Tasks on dead workers, and tasks that run far past their timeout, are recovered through the retry and compensation path
- ✅ Real round-robin by worker ID, with one in-flight task per worker
- ✅ Guarded state transitions: late or duplicate worker reports are ignored and no longer wipe `started_at` or output
- ✅ Exponential retry back-off (`delay × 2^attempt`, capped at 1 hour)
- ✅ Workflow input is validated against shell injection
- ✅ Worker replicas get unique IDs and addresses, so scaling works
- ✅ Unit tests for worker selection and input validation
- ✅ Verified end to end on Docker with 3 workers: registration, a throughput burst (30×1s tasks in 10.4s), success and failure workflows, rejected injection, and recovery after killing a worker

### Known gaps carried forward
- ~~No authentication and no TLS~~ (resolved in Phase 1)
- A late report from a worker that was declared dead can still affect a later attempt of the same task (→ Phase 3, fencing)
- A single coordinator is a single point of failure (→ Phase 3)
- ~~Mixed CRLF and LF line endings; `gofmt` flags every file~~ (resolved in Phase 1)

---

## Phase 1: Production basics ✅ (done, PR #2)

**Goal:** every change is tested automatically, and the system is safe to expose on a network.
**Why first:** everything later builds on this. Without CI, each new feature risks breaking the fixes from Phase 0.

### 1.1 Repository hygiene
- ✅ Add `.gitattributes` (`* text=auto eol=lf`) and normalize line endings in one commit
- ✅ Run `gofmt` across the repo, add a `golangci-lint` config, and remove the obsolete `version:` key from `docker-compose.yml`
- ✅ Add a `Makefile` with `build`, `test`, `lint`, `e2e`, `up`, `down` and `proto` targets
- ✅ Add a `LICENSE` (Apache-2.0 recommended for infrastructure software: patent grant, enterprise-friendly)

### 1.2 Continuous integration
- ✅ GitHub Actions: build, vet, lint and unit tests on every PR
- ✅ **Automated end-to-end suite** (`test/e2e`, Go tests driving Docker Compose) that automates the five checks run by hand in Phase 0:
  - Workers register with unique IDs
  - Task lifecycle, delayed tasks and output capture
  - Throughput burst with even distribution across workers
  - Success and failure workflows, plus rejected injection
  - Killing a worker mid-task: the task is retried elsewhere
- ✅ Database-layer tests against a real Postgres (a throwaway database per test via `CONDUCTOR_TEST_DATABASE_URL`, rather than testcontainers-go)
- ⚠️ Branch protection on `master`: CI must pass before merge. **Needs a repo admin** to enable (Settings → Branches).

### 1.3 Schema and configuration
- ✅ Replace `setup.sql` in `docker-entrypoint-initdb.d` with versioned migrations applied at startup (a small embedded runner with an advisory lock instead of `golang-migrate`)
- ✅ Central config struct loaded from environment variables with defaults and validation (currently scattered `os.Getenv` calls)
- ✅ Structured logging with `log/slog`, including task and workflow IDs on every line
- ✅ Graceful shutdown on SIGTERM:
  - Worker stops accepting tasks, finishes or cancels the current one, then exits
  - Coordinator stops dispatching and closes connections
  - Scheduler calls `http.Server.Shutdown`

### 1.4 Security baseline
- ✅ API keys for the HTTP API (stored hashed, sent as `Authorization: Bearer`)
- ✅ TLS for gRPC, with optional mTLS between coordinator and workers, plus a shared worker registration token
- ✅ Add a `.dockerignore` (`.env`, `.git`, binaries) so secrets never enter the Docker build context
- ✅ Add a `.env.example` that lists every variable (including `OPENAI_API_KEY`) with placeholder values
- ✅ Limits on request size and task output size (for example 1 MB, truncated with a marker)
- ✅ API versioning: move routes under `/v1/`

### 1.5 Task cancellation
- ✅ New `CANCELLED` status, a `POST /v1/tasks/{id}/cancel` endpoint and a `CancelTask` worker RPC that kills the running command's process group
- ✅ Cancelling a workflow stops the current step and runs compensation for completed steps

**Exit criteria:** CI is green and required on PRs; the e2e suite covers all five Phase 0 checks; the API rejects unauthenticated requests; the stack shuts down cleanly with no lost tasks.

---

## Phase 2: A real scheduler ✅ (done, PR #3)

**Goal:** users define their own workflows without editing Go code, and tasks go beyond shell commands.

### 2.1 Workflows as data
- ✅ Workflow definitions in YAML or JSON, submitted via `POST /v1/workflow-definitions` and stored versioned in a `workflow_definitions` table
- ✅ **DAG support:** `depends_on` between steps, parallel branches and fan-in; compensation runs in reverse topological order
- ✅ Step outputs available to later steps (for example `steps.book_flight.output.booking_id`)
- ✅ **Pass inputs as environment variables, not by templating them into shell strings.** This removes the injection risk at its root, rather than relying on validation alone.
- ✅ Per-step settings for retries, timeout and back-off
- ✅ Definition validation: cycle detection, unknown references and schema checks, with clear error messages

### 2.2 Scheduling features
- ✅ Cron schedules (`schedules` table) with timezone and misfire policy (skip, run once, or catch up)
- ✅ Idempotency keys on submission (unique constraint), so duplicate submits return the existing task
- ✅ Priority aging, so low-priority tasks don't starve
- ✅ Named queues with concurrency limits and rate limits (a sliding window over recent dispatches, enforced in the pick query; no token-bucket state)
- ✅ Dead-letter view for permanently failed tasks, with a "requeue" action

### 2.3 Smarter workers
- ✅ Workers report capacity (slots) and labels in their heartbeat, and run multiple tasks concurrently
- ✅ Tasks declare requirements (`labels: {gpu: "true"}`), and the coordinator matches them to workers
- ✅ Worker registry persisted in a `workers` table (also needed for Phase 3 failover)

### 2.4 Task types (a pluggable executor interface)
- ✅ `shell`: the current behavior
- ✅ `http`: method, URL, headers and body, with success determined by status code
- ✅ `container`: run each task in its own Docker container with CPU and memory limits. This is the sandboxing answer for untrusted work.

### 2.5 Developer experience
- ✅ **Single-binary mode:** one `conductor` binary with subcommands; `conductor dev` runs everything in one process against one Postgres
- ✅ `conductorctl` CLI to replace the test client: submit, status, logs, cancel, workflows and schedules
- ✅ Go SDK (`pkg/client`) for submitting tasks and workflows from Go code

### 2.6 Multi-tenancy (basic)
- ✅ Namespaces: tasks, workflows, API keys and quotas scoped per namespace

**Exit criteria:** a new user can write a YAML DAG workflow with parallel steps, schedule it with cron, and watch it run with `conductorctl`, without touching Go code.

---

## Phase 3: Distributed-systems depth ✅ (done, PR #4)

**Goal:** no single point of failure, provable correctness under failure, and published performance numbers.

### 3.1 Coordinator high availability (Postgres-only)
- ✅ Run 2 or more coordinators. Leader election uses `pg_try_advisory_lock`; standbys wait and take over when the leader's session dies.
- ✅ The leader rebuilds its in-memory state (workers, in-flight tasks) from the database on takeover
- ✅ Workers and the API discover the current leader (standbys redirect; `pkg/coordclient` follows them), either by trying each coordinator or through a `leader` row in the database
- ✅ **Leader epoch:** every write the leader makes carries its epoch, so a deposed leader cannot change state

### 3.2 Fencing and attempt IDs
- ✅ Add an `attempt` column, incremented on every dispatch and sent to the worker
- ✅ Status reports must match the current attempt (`WHERE attempt = $n`). This closes the late-report gap from Phase 0.
- ✅ Expose the attempt ID to tasks (environment variable) so user code can de-duplicate side effects

### 3.3 Faster dispatch
- ✅ `LISTEN/NOTIFY` on task insert and retry wakes the dispatcher immediately instead of waiting for the 1s tick
- ✅ Batch claiming (`LIMIT n`) to cut round trips under load, with queue and namespace limits still enforced inside a batch; plus an indexed `dispatch_key` so a pick no longer sorts every queued task
- ✅ **Design decision record:** [ADR 0001](docs/adr/0001-dispatch-push-vs-pull.md). Scheduling stays in the leader (workers never get database credentials, and the per-task cost is Postgres work under either model); the transport moves to worker-opened streams in Phase 6
- ✅ Hot-path profiling and fixes: NOTIFY no longer serializes commits, single-statement fences, no worker STARTED round trip, a persistent connection pool, and a worker slot race

### 3.4 Benchmarks and chaos testing
- ✅ Benchmark harness (`cmd/conductor-bench`): throughput (tasks/s), dispatch latency p50/p99, and workflow end-to-end latency at 1, 10 and 50 workers
- ✅ Baseline numbers published in [BENCHMARKS.md](BENCHMARKS.md), with targets for the next phases
- ✅ Chaos suite (`make chaos`, nightly in [chaos.yml](.github/workflows/chaos.yml)) running a steady workload of tasks and sagas through:
  - Killing a worker, the leader coordinator, and both coordinators
  - A worker partitioned from the network
  - Postgres latency and a cut connection via Toxiproxy
  - A Postgres restart
- ✅ Invariant checks after the run: no task lost, none stuck, every task completed, every workflow terminal, and compensation run exactly where required (all three steps of every failing saga, no step of any succeeding one)

**Exit criteria:** killing the leader coordinator mid-workflow causes no lost or stuck work; chaos tests pass nightly; benchmark numbers are published.

---

## Phase 4: Observability and UI ✅ (done, PR #5)

**Goal:** operators can see what is happening and why, without reading container logs.

### 4.1 Metrics
- ✅ Prometheus `/metrics` on every service: queue depth per queue (ready, delayed, paused, running, oldest wait), dispatch latency, turnaround and run-time histograms, results by outcome (completed, failed, retried, stale), lost tasks, workers and slots, leadership, workflow outcomes and compensations, API requests by route
- ✅ Grafana with a provisioned 21-panel dashboard: `docker compose --profile observability up`
- ✅ Alert rules: no leader, no healthy workers, a queue backing up, a failure-rate spike, lost tasks

### 4.2 Tracing and logs
- ✅ OpenTelemetry tracing from the HTTP request through the coordinator and worker into the task. The traceparent is stored on the task row, so a trace survives the task's wait in the queue; workflow steps share their run's trace. Jaeger is in the observability profile.
- ✅ `TRACEPARENT` in task environments, so user code can join the trace
- ✅ Live task output: `GET /v1/tasks/{id}/logs?follow=true` and `conductorctl task logs -f` tail a running task, across retries. Changed from the plan: output is read from the worker only while someone watches, instead of workers pushing chunks to Postgres, which would add writes to every task (Postgres writes are the throughput limit, see Phase 3)
- ✅ Attempt history: each failed attempt's worker, timing, error and output are kept when the task retries (`GET /v1/tasks/{id}/attempts`)

### 4.3 Web dashboard
- ✅ Stack: a static app of plain JavaScript modules on the JSON API, embedded in the binary. See [ADR 0002](docs/adr/0002-dashboard-stack.md)
- ✅ Pages:
  - Overview: waiting and running work, last hour's outcomes as a strip chart, queues, recent failures, active workflows
  - Tasks: search, filter and page
  - Task detail: attempts, live output, error, outputs
  - **Workflow run graph:** a live "mimic diagram" with step states, the failed step, and compensation running back along the route
  - Workers, schedules and dead-letter queue
- ✅ Actions: cancel, requeue from the DLQ, pause or resume a queue, pause, resume or run a schedule
- ✅ Sign-in with an API key; admin keys switch namespaces

**Exit criteria:** a failed workflow can be diagnosed from the dashboard alone (which step failed, why, and what was compensated). ✅ The run page states all three in one banner, opens the failed step with its output, and draws the undo.

---

## Phase 5: AI-native durable execution ✅

**Goal:** make Conductor the reliable backbone for AI agents. AI steps are slow, flaky and expensive, which is exactly what durable workflows handle well.

**Ground rules:**
- **LLM output is never executed directly.** Generated workflows require validation and human approval, and run sandboxed.
- API keys live in environment variables or a secret store, never in task definitions or the database in plain text.
- AI features are opt-in per namespace. Logs are redacted (secrets, tokens) before being sent to any model.
- Providers sit behind an interface: OpenAI first, others pluggable.

### 5.1 LLM steps (core)
- ✅ `llm` task type: model, prompt template, input variables, and optional JSON-schema structured output
- ✅ Rate-limit-aware scheduling: honor provider request and token limits per queue, and back off on `429` using `Retry-After`
- ✅ Token and cost accounting per task, workflow and namespace, with budgets that stop or pause a workflow when exceeded
- ⬜ Response caching for identical prompts (optional; deliberately left out, see ADR 0003)

### 5.2 Human-in-the-loop
- ✅ `approval` step type: the workflow pauses, shows the pending decision in the UI and API, and continues or compensates on approve, reject or timeout
- ✅ Signals API: external systems can send data into a waiting workflow

### 5.3 Durable agents
- ✅ `agent` step: an LLM tool-calling loop where **each tool call is a checkpointed workflow task**. If the process crashes mid-agent, it resumes from the last completed tool call instead of starting over.
- ✅ Limits on step count, budget and wall-clock time per agent run

### 5.4 AI operations assistant
- ✅ **Failure explainer:** on permanent failure, an async job summarizes the redacted output and error, then shows a likely cause and a suggested fix in the UI
- ✅ **Error classifier:** transient vs permanent (start with rules, use an LLM as fallback). It advises retry decisions and records its confidence.
- ✅ **Natural language to workflow:** describe a workflow in plain English, get a YAML DAG back, run a dry-run validation, require human approval, then save it
- ✅ **Ask your cluster:** questions like "why did yesterday's runs fail?", answered from read-only task history

### 5.5 Quality
- ✅ Evaluation sets for the classifier and explainer (labelled failures), run in CI when prompts change (`ai-eval.yml`)

**Exit criteria:** a multi-step agent workflow survives killing its worker mid-run and resumes without repeating completed tool calls; costs are tracked and budgets enforced. ✅ `TestAgentSurvivesWorkerCrash` kills the worker mid-agent: only the interrupted tool call runs twice. Budgets are enforced per workflow, namespace and queue.

---

## Phase 6: Kubernetes and cloud-native ⬜

**Goal:** first-class deployment on Kubernetes, with autoscaling and strong isolation.
**When it is needed:** when running across multiple machines, autoscaling workers, or isolating each task in its own pod. Docker Compose remains the supported single-host option.

### 6.1 Deployment
- ⬜ Helm chart:
  - Coordinator Deployment (2 replicas, Phase 3 leader election)
  - Scheduler Deployment with a HorizontalPodAutoscaler
  - Worker Deployment
  - Postgres external, or via CloudNativePG
- ⬜ CI job that installs the chart on `kind` and runs the e2e suite against it
- ⬜ Production settings: PodDisruptionBudgets, NetworkPolicies, resource requests and limits, readiness and liveness probes

### 6.2 Autoscaling
- ⬜ KEDA `ScaledObject` using the Postgres scaler on queue depth: scale workers to zero when idle and burst when the queue grows

### 6.3 Kubernetes executor
- ⬜ `k8s` task type: each task runs as a Kubernetes Job or Pod with its own image, resources and service account (least-privilege RBAC)

### 6.4 Operator
- ⬜ Operator built with kubebuilder, with CRDs for `Workflow`, `WorkflowRun` and `Schedule`, so workflows can be managed with GitOps (`kubectl apply`, Argo CD)

### 6.5 Reference infrastructure
- ⬜ Terraform example for one managed cloud (cluster, managed Postgres, monitoring)
- ⬜ GitOps example with Argo CD

**Exit criteria:** `helm install` gives a working HA cluster; workers autoscale on queue depth; the e2e suite passes on Kubernetes in CI.

---

## Gate: open-source launch decision 🚦

**Before Phase 7, review honestly.** Go ahead only if **all** of the following are true:

- [ ] Phases 1–3 are complete and the Phase 4 dashboard is usable
- [ ] CI, e2e and nightly chaos tests have been green for at least 4 weeks
- [ ] Benchmarks are published and reproducible
- [ ] A security review is done (auth, TLS, sandboxing, injection, secrets)
- [ ] A person new to the project followed the quickstart and ran a DAG workflow in under 10 minutes without help
- [ ] At least 3 realistic example projects work (for example ETL pipeline, order saga, AI agent)
- [ ] There is a clear one-line answer to "why this instead of Temporal, River or Hatchet?"

If any item is not met, keep iterating; the launch can wait.

---

## Phase 7: Open-source launch ⬜

**Goal:** make the project easy to discover, trust, adopt and contribute to.

### 7.1 Naming (blocking)
- ⬜ **Rename the project.** "Conductor" is already a well-known open-source workflow engine (Netflix Conductor, now maintained as Conductor OSS / Orkes). Pick a unique name and check GitHub, the Go module path, domain availability and trademarks.
- ⬜ Update the module path, images, binaries and docs to the new name

### 7.2 Project essentials
- ⬜ README rewrite: pitch, 60-second demo GIF, quickstart, comparison table, architecture diagram
- ⬜ `CONTRIBUTING.md`, `CODE_OF_CONDUCT.md`, `SECURITY.md` (private vulnerability reporting), issue and PR templates
- ⬜ "Good first issue" labels on 10 or more starter tasks
- ⬜ Public roadmap (this file, trimmed) and `CHANGELOG.md`

### 7.3 Releases
- ⬜ Semantic versioning; tagging a release runs GoReleaser, producing multi-OS binaries and multi-arch images on GHCR
- ⬜ Signed releases and an SBOM
- ⬜ Upgrade guide and a database migration compatibility policy

### 7.4 Documentation site
- ⬜ Docs site (MkDocs Material or Docusaurus) covering concepts, quickstart, workflow YAML reference, API reference (OpenAPI), deployment (Compose, Helm), operations, AI features and FAQ
- ⬜ An honest comparison page: what Temporal, Airflow, River and Hatchet do better, and where this project fits

### 7.5 Launch and community
- ⬜ Launch blog post: "Building a durable workflow engine on just Postgres"
- ⬜ Share on Hacker News (Show HN), r/golang and r/selfhosted
- ⬜ GitHub Discussions for Q&A; triage cadence for issues and PRs
- ⬜ Follow-up posts: deep dives on SKIP LOCKED, sagas, fencing, and chaos-testing results

**Exit criteria:** v1.0.0 released; outside contributors have merged PRs; there are real users and reported issues.

---

## Recommended execution order

Phases describe *areas*. The order below gets the most value soonest and front-loads the risky work:

1. **Phase 1:** all of it (CI and e2e first)
2. **Phase 4.1:** metrics (cheap, and needed to measure everything after)
3. **Phase 2.1 and 2.5:** workflows as data, DAGs, single-binary mode, CLI
4. **Phase 5.1 and 5.4 (failure explainer):** a quick AI win, with the LLM task type proving the executor interface
5. **Phase 3:** HA, fencing, benchmarks, chaos
6. **Phase 2.2–2.4, 2.6:** remaining scheduler features
7. **Phase 4.2–4.3:** tracing, log streaming, dashboard
8. **Phase 5.2–5.5:** approvals, durable agents, the rest of AI
9. **Gate**, then **Phase 7:** launch
10. **Phase 6:** Kubernetes, which can come before or after the launch depending on user demand

---

## Phase overview

| Phase | Area | Status |
|---|---|---|
| 0 | Foundation and bug-fix round | ✅ Done |
| 1 | Production basics (CI, e2e, migrations, auth, TLS, cancellation) | ✅ Done |
| 2 | A real scheduler (YAML DAGs, cron, task types, CLI, SDK) | ✅ Done |
| 3 | Distributed-systems depth (HA, fencing, benchmarks, chaos) | ✅ Done |
| 4 | Observability and UI (metrics, tracing, dashboard) | ✅ Done |
| 5 | AI-native durable execution (LLM steps, agents, explainer) | ⬜ |
| 6 | Kubernetes and cloud-native (Helm, KEDA, operator) | ⬜ |
| 🚦 | Open-source launch gate | ⬜ |
| 7 | Open-source launch (rename, docs, releases, community) | ⬜ |
