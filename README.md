# Conductor

[![CI](https://github.com/ChinmayNoob/conductor/actions/workflows/ci.yml/badge.svg)](https://github.com/ChinmayNoob/conductor/actions/workflows/ci.yml)

**A self-hosted, Postgres-only durable task and workflow engine.**

Conductor runs shell commands across a cluster of workers with priorities, retries with exponential back-off, timeouts, cancellation, and saga workflows that undo completed steps when a later one fails. Postgres is the only dependency.

> **Status:** pre-1.0 and under active development. See [plan.md](plan.md) for the roadmap and [DEVLOG.md](DEVLOG.md) for how it's built, with diagrams.

## Architecture

```mermaid
flowchart LR
    CLI["conductorctl / Go SDK / curl"] -- "HTTP + API key" --> API
    subgraph cluster [Conductor cluster]
        API["api<br/>HTTP :8081"] -- "gRPC + cluster token (+mTLS)" --> COORD["coordinator<br/>gRPC :8080"]
        COORD -- "dispatch / cancel" --> W1["worker"]
        COORD -- "dispatch / cancel" --> W2["worker"]
        COORD -- "dispatch / cancel" --> W3["worker …"]
        W1 & W2 & W3 -- "heartbeats + results" --> COORD
    end
    API -- "reads" --> PG[("PostgreSQL")]
    COORD -- "queue + state" --> PG
```

| Component | Role |
|---|---|
| **api** | Stateless HTTP API. Authenticates API keys, reads state from Postgres, and sends commands (submit, cancel) to the coordinator. |
| **coordinator** | Tracks workers by heartbeat, claims tasks from Postgres with `FOR UPDATE SKIP LOCKED`, dispatches them round-robin, retries failures, recovers tasks from dead workers, and drives workflows. |
| **worker** | Runs each task as a shell command with a timeout, in its own process group so cancellation kills everything it started. Drains gracefully on shutdown. |

All three are subcommands of one binary, `conductor`, shipped as one Docker image.

## Quickstart

Requirements: Docker, and Go 1.24+ for the CLI.

```bash
git clone https://github.com/ChinmayNoob/conductor.git
cd conductor
docker compose up -d --build --scale worker=3

go build -o bin/conductorctl ./cmd/conductorctl
export CONDUCTOR_API_KEY=insecure-dev-api-key   # the development default

bin/conductorctl task submit -cmd 'echo hello from $(hostname)' -wait
bin/conductorctl workflow start trip_booking -input '{"user_id":123,"amount":5000}' -wait
```

> The compose file uses insecure development secrets by default. Copy `.env.example` to `.env` and set `CONDUCTOR_CLUSTER_TOKEN` and `CONDUCTOR_API_KEY` before running anywhere but your own machine.

## Tasks

A task is a shell command. Options:

| Field | Default | Meaning |
|---|---|---|
| `data` | (required) | Command to run with `sh -c` |
| `priority` | 5 | 1 (highest) to 10 |
| `max_retries` | 3 | Retries after the first attempt fails |
| `retry_delay_seconds` | 60 | Base retry delay; attempt *n* waits `delay × 2ⁿ` (max 1 hour) |
| `timeout_seconds` | 300 | The command is killed after this long |
| `delay_seconds` / `scheduled_at` | now | Run later (relative seconds, or a Unix timestamp) |

```bash
conductorctl task submit -cmd 'make backup' -priority 1 -retries 5 -timeout 600
conductorctl task list -status FAILED
conductorctl task get <id>        # status, attempts, captured output
conductorctl task cancel <id>     # kills the process if it is running
```

Delivery is **at-least-once**: if a worker dies mid-task, the task is retried on another worker, so make side effects idempotent.

## Workflows (sagas)

A workflow is an ordered list of steps. Each step has a command and a **compensating** command that undoes it. If a step fails after its retries, Conductor compensates every completed step in reverse order.

```mermaid
flowchart LR
    F["Book Flight ✅"] --> H["Book Hotel ❌"] -. "skipped" .-> P["Charge Payment"]
    H -- "failure" --> CF["Cancel Flight ↩️"]
```

Built-in examples:

| Type | Behaviour |
|---|---|
| `trip_booking` | All three steps succeed |
| `trip_booking_fail` | Book Hotel fails; Book Flight is compensated |
| `trip_booking_slow` | Book Hotel takes 30s, which leaves time to try `workflow cancel` |

```bash
conductorctl workflow start trip_booking_fail -input '{"user_id":1,"amount":10}' -wait
conductorctl workflow cancel <id>   # cancels the running step, compensates completed ones
```

Workflow input values are substituted into commands, so they must be strings, numbers or booleans made of letters, digits and `_ . , : @ + -` (and not start with `-`). Anything else is rejected to prevent shell injection.

## HTTP API

Every `/v1` route needs `Authorization: Bearer <api key>`. Errors are returned as `{"error": "..."}`.

| Method | Path | Description |
|---|---|---|
| `GET` | `/health` | Liveness and database check (no auth) |
| `POST` | `/v1/tasks` | Submit a task |
| `GET` | `/v1/tasks?status=&limit=` | List recent tasks |
| `GET` | `/v1/tasks/{id}` | Get a task |
| `POST` | `/v1/tasks/{id}/cancel` | Cancel a task |
| `GET` | `/v1/stats` | Task counts by status |
| `POST` | `/v1/workflows` | Start a workflow: `{"type": "...", "input": {...}}` |
| `GET` | `/v1/workflows?limit=` | List workflows |
| `GET` | `/v1/workflows/{id}` | Get a workflow with its steps |
| `POST` | `/v1/workflows/{id}/cancel` | Cancel a workflow |
| `POST` | `/v1/api-keys` | Create a key (admin): `{"name": "...", "admin": false}` |
| `GET` | `/v1/api-keys` | List keys (admin) |
| `DELETE` | `/v1/api-keys/{id}` | Revoke a key (admin) |

```bash
curl -s -X POST localhost:8081/v1/tasks \
  -H "Authorization: Bearer $CONDUCTOR_API_KEY" \
  -d '{"data": "echo hi", "priority": 1}'
```

## Security

- **API keys:** `CONDUCTOR_API_KEY` creates the first admin key on startup. Create scoped keys with `conductorctl apikey create <name>`; only a SHA-256 hash is stored.
- **Cluster token:** every gRPC call between components carries `CONDUCTOR_CLUSTER_TOKEN`.
- **Mutual TLS:** set `CONDUCTOR_TLS_CERT`, `CONDUCTOR_TLS_KEY` and `CONDUCTOR_TLS_CA`. All components share one certificate whose name is `conductor`, so workers can be reached by IP. To try it:

  ```bash
  ./scripts/gen-dev-certs.sh
  docker compose -f docker-compose.yml -f docker-compose.tls.yml up -d --build
  ```
- Tasks run as an unprivileged user inside the worker container.

## Configuration

All settings are environment variables; [.env.example](.env.example) lists them with defaults. The most important:

| Variable | Default | Purpose |
|---|---|---|
| `CONDUCTOR_CLUSTER_TOKEN` | (required) | Shared secret for gRPC between components |
| `CONDUCTOR_API_KEY` | | Bootstrap admin API key |
| `POSTGRES_HOST` / `PORT` / `USER` / `PASSWORD` / `DB` | `localhost` / `5432` / … | Database |
| `CONDUCTOR_COORDINATOR_ADDR` | `localhost:8080` | Where the API and workers reach the coordinator |
| `CONDUCTOR_SHUTDOWN_TIMEOUT` | `25s` | How long a stopping worker lets running tasks finish |
| `CONDUCTOR_LOG_LEVEL` / `CONDUCTOR_LOG_FORMAT` | `info` / `text` | Logging (`json` for production) |

## Development

```bash
make help        # list targets
make test        # unit tests
make lint        # golangci-lint (runs in Docker)
make up          # start the stack with 3 workers
make e2e         # end-to-end suite against the running stack
make proto       # regenerate gRPC code (protoc runs in Docker)
```

Database tests need a Postgres server; they create and drop a throwaway database per test:

```bash
CONDUCTOR_TEST_DATABASE_URL='postgres://postgres:postgres@localhost:5433/postgres?sslmode=disable' go test ./pkg/db/
```

CI runs lint, unit and database tests with the race detector, and the end-to-end suite over both plaintext and mutual TLS.

## License

[Apache 2.0](LICENSE)
