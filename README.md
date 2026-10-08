# Conductor

[![CI](https://github.com/ChinmayNoob/conductor/actions/workflows/ci.yml/badge.svg)](https://github.com/ChinmayNoob/conductor/actions/workflows/ci.yml)

**A self-hosted, Postgres-only durable task and workflow engine.**

Run shell commands, HTTP calls and containers across a cluster of workers, with:
- retries with exponential back-off, timeouts and cancellation
- **DAG workflows** that undo completed steps when a later one fails (sagas)
- **cron schedules**, queues with concurrency and rate limits, and label-based routing
- namespaces with quotas
- a **web dashboard**, Prometheus metrics, OpenTelemetry tracing and live task output

Postgres is the only dependency.

![The dashboard's workflow run page: the failed step, why, and what was undone](docs/images/dashboard-workflow.png)

> **Status:** pre-1.0 and under active development. See [plan.md](plan.md) for the roadmap and [DEVLOG.md](DEVLOG.md) for how it's built, with diagrams.

## Architecture

```mermaid
flowchart LR
    CLI["conductorctl / Go SDK / curl"] -- "HTTP + API key" --> API
    subgraph cluster [Conductor cluster]
        API["api<br/>HTTP :8081"] -- "gRPC + cluster token (+mTLS)" --> COORD["coordinator (leader)<br/>gRPC :8080"]
        STANDBY["coordinator (standby)"] -. "redirects to leader" .-> COORD
        COORD -- "dispatch / cancel" --> W1["worker<br/>shell · http"]
        COORD -- "dispatch / cancel" --> W2["worker<br/>shell · http"]
        COORD -- "dispatch / cancel" --> W3["container-worker<br/>+ container"]
        W1 & W2 & W3 -- "heartbeats + results" --> COORD
    end
    API -- "reads" --> PG[("PostgreSQL")]
    COORD -- "queue, workflows, schedules" --> PG
    STANDBY -. "campaigns for leadership" .-> PG
```

| Component | Role |
|---|---|
| **api** | Stateless HTTP API. Authenticates API keys, reads state from Postgres, and sends commands to the coordinator. |
| **coordinator** | Tracks workers, claims tasks from Postgres with `FOR UPDATE SKIP LOCKED` (respecting queue limits and worker labels), dispatches them, retries failures, recovers tasks from dead workers, drives workflow DAGs, and fires cron schedules. Run two or more: one is elected leader through a Postgres advisory lock and the rest are hot standbys that take over within about a second. Every leader write is fenced by an epoch, and every result by an attempt number, so a deposed leader or a partitioned worker can't change anything. |
| **worker** | Runs tasks (shell, HTTP, or containers) in a clean environment. Cancellation kills everything a task started. Drains gracefully on shutdown. |

All three are subcommands of one binary, `conductor`, shipped as one Docker image.

## Quickstart

**With Docker Compose** (Docker and Go 1.24+):

```bash
git clone https://github.com/ChinmayNoob/conductor.git && cd conductor
docker compose up -d --build --scale worker=3

go build -o bin/conductorctl ./cmd/conductorctl
export CONDUCTOR_API_KEY=insecure-dev-api-key   # the development default

bin/conductorctl task submit -cmd 'echo hello from $(hostname)' -wait
bin/conductorctl workflow start order_pipeline -input '{"order_id": "A1"}' -wait
```

Then open the dashboard at http://localhost:8081 and sign in with the same key.

**Or as a single process** (only Postgres needed):

```bash
POSTGRES_PORT=5433 go run ./cmd/conductor dev    # coordinator + API + worker in one process
```

> The compose file uses insecure development secrets by default. Copy `.env.example` to `.env` and set `CONDUCTOR_CLUSTER_TOKEN` and `CONDUCTOR_API_KEY` before running anywhere but your own machine.

## Tasks

```bash
conductorctl task submit -cmd 'make backup' -priority 1 -retries 5 -timeout 600
conductorctl task submit -url https://example.com/hook -method POST -body '{"ok":true}'
conductorctl task submit -image alpine:3.20 -- cat /etc/alpine-release
conductorctl task list -status FAILED
conductorctl task cancel <id>         # kills the process if it's running
conductorctl dlq                      # permanently failed tasks
conductorctl task requeue <id>        # give one a fresh set of retries
```

| Field | Default | Meaning |
|---|---|---|
| `type` | `shell` | `shell` runs `command` with `sh -c`. `http` sends `http.{method,url,headers,body}`. `container` runs `container.{image,command,memory,cpus,network}`. |
| `env` | | Extra environment variables for the task |
| `labels` | | Only workers with all these labels run the task, e.g. `{"gpu": "true"}` |
| `queue` | `default` | Queues can have concurrency and rate limits (see below) |
| `priority` | 5 | 1 (highest) to 10. Waiting tasks gain priority over time, so nothing starves. |
| `max_retries` / `retry_delay_seconds` | 3 / 60 | Attempt *n* waits `delay × 2ⁿ` (max 1 hour) |
| `timeout_seconds` | 300 | The task is killed after this long |
| `delay_seconds` / `scheduled_at` | now | Run later |
| `idempotency_key` | | Submitting the same key twice returns the first task (HTTP 200, not 201) |

Tasks run in a **minimal environment**. Worker secrets such as the cluster token are never visible to them. Each task gets `CONDUCTOR_TASK_ID`, `CONDUCTOR_ATTEMPT` and `CONDUCTOR_OUTPUT`. Lines written to `$CONDUCTOR_OUTPUT` as `key=value` become the task's **outputs**.

Delivery is **at-least-once**: if a worker dies mid-task, the task is retried on another worker, so make side effects idempotent.

## Workflows

A workflow is a DAG of steps defined in YAML. Steps run as soon as their dependencies finish, so independent steps run **in parallel**. If a step fails, running steps are cancelled and completed steps are **compensated** (undone) in reverse dependency order.

```yaml
name: order_pipeline
inputs:
  order_id: {required: true}
  amount: {default: "100"}
steps:
  - name: validate
    run: echo "customer=cus_$INPUT_ORDER_ID" >> "$CONDUCTOR_OUTPUT"

  - name: reserve_stock
    depends_on: [validate]
    run: echo "reservation=res_$INPUT_ORDER_ID" >> "$CONDUCTOR_OUTPUT"
    compensate: echo "releasing $OUTPUT_RESERVATION"

  - name: charge_card
    depends_on: [validate]
    env:
      CUSTOMER: "${{ steps.validate.outputs.customer }}"
    run: echo "charging $INPUT_AMOUNT to $CUSTOMER"
    compensate: echo "refunding"

  - name: ship
    depends_on: [reserve_stock, charge_card]
    run: ./ship.sh
```

```mermaid
flowchart LR
    V[validate] --> R[reserve_stock] & C[charge_card]
    R & C --> S[ship]
    S -. "fails" .-> UC["↩ refund"] & UR["↩ release"]
```

```bash
conductorctl workflow apply -f my_workflow.yaml     # new version only if it changed
conductorctl workflow defs                          # list definitions
conductorctl workflow start order_pipeline -input '{"order_id":"A1"}' -wait
conductorctl workflow cancel <id>                   # cancel running steps, compensate the rest
```

- **Inputs** are exposed as `INPUT_<NAME>` environment variables. Declared inputs can be `required` or have a `default`.
- **Expressions** `${{ inputs.x }}`, `${{ steps.s.outputs.k }}` and `${{ workflow.id }}` may be used in `env`, HTTP fields and container commands, but **never in `run`**. Values reach shell commands only through environment variables, so inputs can't be interpreted as shell code.
- A compensation sees its own step's outputs as `OUTPUT_<KEY>`. It can be a string (a shell command) or a full action (`type: http`, …).
- Steps take `retries`, `retry_delay`, `timeout`, `priority`, `queue` and `labels`, and `defaults:` sets them for every step.
- Each run keeps a snapshot of the definition version it started with, so editing a workflow never affects runs in flight.

Bundled examples: `trip_booking`, `trip_booking_fail`, `trip_booking_slow` and `order_pipeline` (in [examples/workflows](examples/workflows)).

## Schedules

```bash
conductorctl schedule create nightly-report -cron '0 2 * * *' -tz Asia/Kolkata -cmd './report.sh'
conductorctl schedule create sync -cron '@every 5m' -workflow order_pipeline -input '{"order_id":"X"}'
conductorctl schedule list
conductorctl schedule trigger nightly-report   # fire now
conductorctl schedule pause nightly-report
```

Misfire policy (what happens to runs missed while the coordinator was down): `skip` (default), `run_once`, or `catch_up`. Each run time fires at most once, even across restarts.

## Queues, priorities and labels

```bash
conductorctl queue set emails -concurrency 5 -rate 100 -per 60   # ≤5 at once, ≤100/minute
conductorctl queue set emails -pause
conductorctl queue list
```

Workers advertise labels (`CONDUCTOR_WORKER_LABELS=gpu=true,region=eu`) plus the task types they support. Tasks requiring labels nobody has simply wait.

## Namespaces

Namespaces isolate teams. Each has its own tasks, workflows, schedules, queues and API keys, with optional quotas:

```bash
conductorctl namespace create team-a -max-pending 1000 -max-concurrency 20
conductorctl apikey create team-a-ci -namespace team-a
conductorctl -n team-a task list          # admin keys can act in any namespace
```

## HTTP API

Every `/v1` route needs `Authorization: Bearer <api key>`. Admin keys may add `X-Conductor-Namespace`. Errors look like `{"error": "..."}`.

| Area | Endpoints |
|---|---|
| Tasks | `POST /v1/tasks` · `GET /v1/tasks?status=&queue=&q=&before=` · `GET /v1/tasks/{id}` · `GET /v1/tasks/{id}/logs?follow=true` · `GET /v1/tasks/{id}/attempts` · `POST /v1/tasks/{id}/cancel` · `POST /v1/tasks/{id}/requeue` · `GET /v1/dead-letter` · `GET /v1/stats` · `GET /v1/stats/timeline` |
| Definitions | `PUT /v1/workflow-definitions` (YAML or JSON body) · `GET /v1/workflow-definitions` · `GET /v1/workflow-definitions/{name}?version=&format=yaml` |
| Runs | `POST /v1/workflows` · `GET /v1/workflows` · `GET /v1/workflows/{id}` · `POST /v1/workflows/{id}/cancel` |
| Schedules | `POST /v1/schedules` · `GET /v1/schedules` · `GET`/`DELETE /v1/schedules/{name}` · `POST /v1/schedules/{name}/{pause,resume,trigger}` |
| Queues | `GET /v1/queues` · `PUT`/`DELETE /v1/queues/{name}` |
| Admin | `GET /v1/workers` · `GET /v1/cluster` · `POST`/`GET /v1/namespaces` · `PUT /v1/namespaces/{name}` · `POST`/`GET /v1/api-keys` · `DELETE /v1/api-keys/{id}` |
| Health | `GET /health` (no auth) |

The Go SDK in [`pkg/client`](pkg/client) covers all of it:

```go
c := client.New("http://localhost:8081", os.Getenv("CONDUCTOR_API_KEY"))
wf, _ := c.StartWorkflow(ctx, client.WorkflowRequest{Workflow: "order_pipeline", Input: map[string]any{"order_id": "A1"}})
wf, _ = c.WaitForWorkflow(ctx, wf.ID)
```

## Dashboard and observability

**Dashboard** at `/ui` (`/` redirects there), built into the binary. Sign in with an API key; it acts with that key's permissions. It shows what is running and waiting, every task with its attempts and live output, workflow runs drawn as a live diagram of which step failed and what was undone, and workers, schedules and the dead-letter queue. You can cancel, requeue, pause or resume queues and schedules, and run schedules now. Design notes: [ADR 0002](docs/adr/0002-dashboard-stack.md).

**Live output** from the CLI or HTTP:

```bash
conductorctl task logs <id> -f      # follows a running task, across retries
conductorctl task attempts <id>     # every attempt, and why each one failed
```

**Metrics:** every process serves Prometheus metrics on `:9090` (`CONDUCTOR_METRICS_LISTEN`). **Tracing:** set `OTEL_EXPORTER_OTLP_ENDPOINT` to send OpenTelemetry traces; a task's trace runs from the HTTP request through the queue into the task, which gets `TRACEPARENT`. To run all of it locally:

```bash
OTEL_EXPORTER_OTLP_ENDPOINT=http://jaeger:4317 docker compose --profile observability up -d --build --scale worker=3
# Grafana http://localhost:3000 · Prometheus http://localhost:9091 · Jaeger http://localhost:16686
```

If a port is taken, set `GRAFANA_PORT`, `PROMETHEUS_PORT` or `JAEGER_PORT`.

The profile provisions a Grafana dashboard and [example alerts](observability/alerts.yml): no leader, no healthy workers, a queue backing up, a failure-rate spike, lost tasks.

## Security

- **API keys:** `CONDUCTOR_API_KEY` creates the first admin key on startup. Other keys are bound to one namespace. Only SHA-256 hashes are stored.
- **Cluster token:** every gRPC call between components carries `CONDUCTOR_CLUSTER_TOKEN`.
- **Mutual TLS:** set `CONDUCTOR_TLS_CERT`, `CONDUCTOR_TLS_KEY` and `CONDUCTOR_TLS_CA` (try `./scripts/gen-dev-certs.sh` with `docker-compose.tls.yml`).
- **Task isolation:** tasks run as an unprivileged user with a minimal environment. Container tasks need a worker with the Docker socket, which is opt-in (`docker compose --profile containers up -d`) because socket access is equivalent to root on the host.

## Configuration

All settings are environment variables; [.env.example](.env.example) lists them. The most important:

| Variable | Default | Purpose |
|---|---|---|
| `CONDUCTOR_CLUSTER_TOKEN` | (required) | Shared secret for gRPC between components |
| `CONDUCTOR_API_KEY` | | Bootstrap admin API key |
| `POSTGRES_HOST` / `PORT` / `USER` / `PASSWORD` / `DB` | `localhost` / `5432` / … | Database |
| `CONDUCTOR_COORDINATOR_ADDR` | `localhost:8080` | Where the API and workers reach the coordinator |
| `CONDUCTOR_WORKER_SLOTS` | `2` | Tasks a worker runs at once |
| `CONDUCTOR_WORKER_LABELS` | | e.g. `gpu=true,region=eu` |
| `CONDUCTOR_WORKER_PASS_ENV` | | Worker variables tasks may see, e.g. `AWS_REGION` |
| `CONDUCTOR_PRIORITY_AGING` | `60s` | Waiting tasks gain one priority level per interval (`0` disables it) |
| `CONDUCTOR_SHUTDOWN_TIMEOUT` | `25s` | How long a stopping worker lets running tasks finish |
| `CONDUCTOR_METRICS_LISTEN` | `:9090` | Prometheus metrics (`off` disables them) |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | | Send traces over OTLP/gRPC; standard `OTEL_*` variables apply |

## Development

```bash
make help        # list targets
make test        # unit tests
make lint        # golangci-lint (runs in Docker)
make up          # start the stack with 3 workers
make e2e         # end-to-end suite against the running stack
make chaos       # chaos suite: faults injected under load, then invariant checks
make bench       # benchmarks at 1, 10 and 50 workers
make proto       # regenerate gRPC code (protoc runs in Docker)
```

Database tests need a Postgres server; they create and drop a throwaway database per test:

```bash
CONDUCTOR_TEST_DATABASE_URL='postgres://postgres:postgres@localhost:5433/postgres?sslmode=disable' go test ./pkg/db/
```

CI runs lint, unit and database tests with the race detector, and the end-to-end suite over plaintext (with tracing and the observability stack on) and mutual TLS. The chaos suite runs nightly.

## Reliability and performance

- **High availability:** killing the leader coordinator mid-workflow loses nothing; a standby takes over in 0.5–1 s.
- **Chaos-tested:** a steady workload survives killed workers and coordinators, a network partition, Postgres latency, a cut database connection and a Postgres restart, with every task completed and every saga compensated exactly where it should be.
- **Benchmarks:** about 800 no-op tasks/s drained and ~5 ms idle dispatch on a laptop; see [BENCHMARKS.md](BENCHMARKS.md).
- **Design records:** [docs/adr](docs/adr/).

## License

[Apache 2.0](LICENSE)
