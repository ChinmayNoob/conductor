# Conductor Engineering Log

This log explains how Conductor is being built, phase by phase: what changed, why, and how each change was verified. The roadmap lives in [plan.md](plan.md); this file is the story behind it.

```mermaid
timeline
    title Conductor so far
    section Phase 0 · Foundation
        Dec 2025 : Schema, gRPC, coordinator, workers, scheduler
        Feb 2026 : Saga workflows with compensation
        Oct 2026 : Bug-fix round (PR #1)
    section Phase 1 · Production basics
        Oct 2026 : One binary, migrations, auth, mTLS, cancellation, graceful drain, CI + e2e (PR #2)
    section Phase 2 · A real scheduler
        Oct 2026 : YAML DAG workflows, cron, queues, labels, task types, namespaces
```

---

## Contents

1. [Phase 0: Foundation and the bug-fix round](#phase-0-foundation-and-the-bug-fix-round)
2. [Phase 1: Production basics](#phase-1-production-basics)
3. [Phase 2: A real scheduler](#phase-2-a-real-scheduler)

---

## Phase 0: Foundation and the bug-fix round

### What existed

Three services talking over gRPC, with Postgres as the queue:

```mermaid
flowchart LR
    C[client] -- HTTP --> S["scheduler :8081"]
    S -- gRPC --> CO["coordinator :8080"]
    CO -- "SubmitTask" --> W1[worker] & W2[worker]
    W1 & W2 -- "heartbeat, status" --> CO
    CO <--> PG[(Postgres)]
    S <--> PG
```

### What was broken

A code review turned up seven bugs, plus one more found while reading the compose file:

| # | Bug | Effect |
|---|---|---|
| 1 | Dispatch picked **one task per second**, cluster-wide | 30 tasks took at least 30 seconds, however many workers ran |
| 2 | Requeue kept `picked_at` set | Tasks waited 5 minutes for the stale cleanup |
| 3 | Tasks on a dead worker stayed `STARTED` | Lost forever |
| 4 | "Round-robin" iterated a Go map | Random order; `IsBusy` unused |
| 5 | Workflow input pasted into `sh -c` | `{"user_id":"1; rm -rf /"}` ran |
| 6 | README promised exponential back-off | The code used a fixed delay |
| 7 | Compiled binary committed | 8.8 MB in git |
| 8 | Every replica had `WORKER_ID=1` | Scaling workers did nothing |

### Throughput, before and after

```mermaid
xychart-beta
    title "30 one-second tasks on 3 workers (seconds)"
    x-axis ["Before (1 dispatch/s)", "After PR #1", "Theoretical best"]
    y-axis "Seconds" 0 --> 35
    bar [30, 10.4, 10]
```

### Crash recovery, as verified by killing a worker

```mermaid
sequenceDiagram
    participant API
    participant C as Coordinator
    participant W1 as Worker A
    participant W2 as Worker B
    API->>C: submit "sleep 20"
    C->>W1: SubmitTask
    W1-->>C: STARTED
    Note over W1: docker kill 💥
    loop every 10s
        C->>C: heartbeat older than 30s?
    end
    C->>C: mark A unhealthy, fail its in-flight task
    C->>C: RetryTask (attempt 1, back-off)
    C->>W2: SubmitTask
    W2-->>C: COMPLETE "survived-crash"
```

---

## Phase 1: Production basics

**Goal:** every change is tested automatically, and the system is safe to put on a network.
**Result:** PR #2. Every Phase 1 item is done; see the scorecard at the end of this section.

### 1. One binary, clearer roles

The four Dockerfiles and three `main` packages became **one binary with subcommands**. The HTTP service was renamed `api` because it never scheduled anything, and the stale-task cleanup moved into the coordinator, which leaves the API completely stateless.

```mermaid
flowchart TB
    subgraph before [Before]
        direction LR
        b1["cmd/coordinator<br/>Dockerfile.coordinator"]
        b2["cmd/scheduler<br/>Dockerfile.scheduler<br/>+ cleanup loop"]
        b3["cmd/worker<br/>Dockerfile.worker"]
        b4["Dockerfile.postgres<br/>setup.sql via initdb"]
        b5["cmd/client"]
    end
    subgraph after [After]
        direction LR
        a1["conductor coordinator<br/>+ stale-task cleanup"]
        a2["conductor api<br/>(stateless)"]
        a3["conductor worker"]
        a4["postgres:16 image<br/>embedded migrations"]
        a5["conductorctl + pkg/client SDK"]
    end
    before ==> after
```

`pkg/app` owns each component's lifecycle (connect, migrate, serve, shut down), so `cmd/conductor/main.go` is tiny. That structure also makes Phase 2's single-process `dev` mode a few lines of code.

### 2. Schema migrations

`setup.sql` used to run once through Postgres' `initdb` hook, with no way to change the schema later. It is now an embedded, forward-only migration runner:

```mermaid
flowchart LR
    start([component starts]) --> lock["pg_advisory_lock<br/>(one migrator at a time)"]
    lock --> read[read schema_migrations]
    read --> loop{"next NNNN_*.sql<br/>not applied?"}
    loop -- yes --> tx["BEGIN · run file · record version · COMMIT"]
    tx --> loop
    loop -- no --> unlock[unlock] --> serve([serve])
```

- Migration `0001` is **idempotent** (`IF NOT EXISTS`, `DO $$ … duplicate_object`), so databases created by the old `setup.sql` upgrade in place. This was verified against the real volume from Phase 0.
- The coordinator and the API both migrate on startup; the advisory lock makes that safe. A test runs three migrators concurrently.

### 3. A transactional data layer

Every query takes a `context.Context`, and `db.WithTx` runs a function inside a transaction. Starting a workflow (create run, create steps, create the first task, link them) is now atomic, and Phase 2's DAG engine depends on this.

```go
err := s.db.WithTx(ctx, func(tx *db.DB) error {
    wf, _ := tx.CreateWorkflow(ctx, ...)
    // ...steps, first task, links: all or nothing
})
```

### 4. Security layers

```mermaid
flowchart LR
    U[user / CI] -- "① Bearer API key<br/>SHA-256 lookup, admin scope" --> API
    API -- "② cluster token<br/>constant-time compare" --> CO[coordinator]
    CO -- "② cluster token" --> W[worker]
    W -- "② cluster token" --> CO
    API -. "③ optional mTLS<br/>shared cert, SAN=conductor" .- CO
    CO -. "③ mTLS" .- W
    W --> T["④ task runs as uid 10001<br/>own process group, output capped"]
```

| Layer | Detail |
|---|---|
| ① API keys | `cnd_` + 256 random bits. Only a SHA-256 hash is stored (keys are random, so a slow KDF adds nothing). Admin keys manage keys. `CONDUCTOR_API_KEY` bootstraps the first one. |
| ② Cluster token | Required on **every** gRPC call in both directions, checked by interceptors with a constant-time comparison. Before this, anyone who could reach port 8080 could register a fake worker and receive every task. |
| ③ mTLS | Optional. All components share one cluster certificate with SAN `conductor`; clients verify that name rather than the dialed IP, so dynamic worker IPs just work. `scripts/gen-dev-certs.sh` plus `docker-compose.tls.yml` turn it on. |
| ④ Execution | Non-root container user, 1 MB output cap (the head and tail are kept, since errors usually appear at the end), 1 MB request cap, unknown JSON fields rejected. |

### 5. Cancellation

Killing `sh` alone isn't enough: `sh -c "sleep 60; echo"` leaves `sleep` running and holding the output pipe open. Workers start each task in **its own process group** and kill the whole group.

```mermaid
sequenceDiagram
    actor U as User
    participant A as API
    participant C as Coordinator
    participant DB as Postgres
    participant W as Worker
    U->>A: POST /v1/tasks/{id}/cancel
    A->>C: CancelTask
    C->>DB: UPDATE … SET CANCELLED<br/>WHERE status IN (QUEUED, STARTED)<br/>RETURNING the old row
    alt task was dispatched
        C->>W: CancelTask
        W->>W: kill(-pgid, SIGKILL)
        W-->>C: FAILED "task cancelled"
        C->>DB: guarded update matches 0 rows → ignored
    end
    C-->>A: cancelled
    A-->>U: 200 {"status":"CANCELLED"}
```

The guarded transitions from PR #1 (`WHERE status IN ('QUEUED','STARTED') AND picked_at IS NOT NULL`) are what make this safe: the killed process's failure report arrives *after* the cancel and changes nothing.

**Cancelling a workflow** sets `cancel_requested`, cancels the running step, and compensates the completed steps in reverse. The final status is `CANCELLED` rather than `FAILED`. If the running step finishes before the cancel lands, the completion handler sees the flag and compensates instead of starting the next step.

### 6. Graceful shutdown

```mermaid
sequenceDiagram
    participant D as docker stop
    participant W as Worker
    participant C as Coordinator
    D->>W: SIGTERM
    W->>W: draining = true (reject new tasks)
    W->>C: heartbeat {draining: true}
    C->>C: stop dispatching to this worker<br/>(but don't fail its tasks)
    Note over W: running task keeps going…
    W->>C: COMPLETE
    W->>W: all tasks done → exit 0
    Note over W,C: if CONDUCTOR_SHUTDOWN_TIMEOUT (25s) passes first:<br/>kill remaining tasks → report FAILED → retried elsewhere
```

Compose gives workers `stop_grace_period: 30s`, a little longer than the drain timeout.

### 7. The task lifecycle, now complete

```mermaid
stateDiagram-v2
    [*] --> QUEUED: submit
    QUEUED --> QUEUED: picked (picked_at set)
    QUEUED --> STARTED: worker reports STARTED
    STARTED --> COMPLETED: exit 0
    STARTED --> QUEUED: failed, retries left<br/>(back-off: delay × 2ⁿ)
    QUEUED --> QUEUED: dispatch failed / stale → picked_at cleared
    STARTED --> FAILED: failed, no retries left
    STARTED --> QUEUED: worker died / overdue → retry
    QUEUED --> CANCELLED: cancel
    STARTED --> CANCELLED: cancel (process group killed)
    COMPLETED --> [*]
    FAILED --> [*]
    CANCELLED --> [*]
```

### 8. Testing: three layers, all automated

```mermaid
flowchart TB
    subgraph unit ["Unit tests (go test)"]
        u1[worker selection round-robin]
        u2[input validation / injection]
        u3[output cap keeps head + tail]
        u4["gRPC token + mTLS<br/>(certs generated in-test)"]
    end
    subgraph dbt ["Database tests (real Postgres, DB per test)"]
        d1["SKIP LOCKED: 8 pickers × 50 tasks,<br/>no task claimed twice"]
        d2["back-off is exactly 10s → 20s → 40s"]
        d3[late reports can't change finished tasks]
        d4[concurrent migrations, cancel, stale/overdue, rollback, API keys]
    end
    subgraph e2e ["End-to-end (docker compose, 3 workers)"]
        e1[auth · lifecycle · delay · validation]
        e2[30-task burst: time + even spread]
        e3[workflows · compensation · injection]
        e4[cancel queued / running / workflow]
        e5["docker stop → drain · docker kill → recovery"]
    end
    unit --> dbt --> e2e
```

The e2e suite automates every check from Phase 0's manual verification. It finds the container running a task with `docker top`, so it can stop or kill precisely that worker.

### 9. CI pipeline

```mermaid
flowchart LR
    PR([push / PR]) --> L[lint<br/>golangci-lint v2]
    PR --> T["unit + DB tests<br/>-race, Postgres service"]
    PR --> E1["e2e · plaintext"]
    PR --> E2["e2e · mTLS<br/>certs generated in CI"]
    L & T & E1 & E2 --> G{all green?}
    G -- yes --> M([merge])
```

Running the e2e suite **over mTLS as well** matters: the secure configuration is the one most likely to break unnoticed, because developers rarely run it locally.

### 10. Results

Local run (Windows host, Docker Desktop, 3 workers):

| Suite | Result |
|---|---|
| Unit | ✅ coordinator, workflow, worker, security |
| Database (real Postgres) | ✅ 10 tests |
| E2E, plaintext | ✅ 14/14 in 102s (burst: 10.6s, split 10/10/10) |
| E2E, mutual TLS | ✅ 14/14 in 104s |
| golangci-lint | ✅ 0 issues |

### Design decisions

| Decision | Alternatives | Why |
|---|---|---|
| Hand-written migration runner (~100 lines) | golang-migrate | No new dependency, embedded in the binary, advisory-locked. Fits "Postgres is the only dependency". |
| DB tests use `CONDUCTOR_TEST_DATABASE_URL` | testcontainers-go | CI already has a Postgres service; avoids a heavy Docker-client dependency tree. |
| One shared cluster certificate | Per-component certs | Workers have dynamic IPs; verifying a fixed SAN keeps mTLS working without a cert per replica. |
| Cancel endpoint is `POST …/cancel` | `DELETE /tasks/{id}` | Cancelling doesn't delete anything; the task stays queryable. |
| Single binary pulled forward from Phase 2 | Keep 3 binaries | Every `main` was being rewritten anyway. |
| Shell tasks run as non-root | Root (old behaviour) | Least privilege; tasks needing root should use the container executor (Phase 2). |

### Phase 1 scorecard

| Item | Status |
|---|---|
| 1.1 Line endings, gofmt, lint config, Makefile, LICENSE (Apache-2.0) | ✅ |
| 1.2 CI, automated e2e, DB tests, (branch protection needs a repo admin) | ✅ / ⚠️ |
| 1.3 Migrations, config, slog, graceful shutdown | ✅ |
| 1.4 API keys, cluster token, mTLS, `.dockerignore`, `.env.example`, limits, `/v1` | ✅ |
| 1.5 Task and workflow cancellation | ✅ |

---

## Phase 2: A real scheduler

*In progress. This section is filled in as Phase 2 lands.*
