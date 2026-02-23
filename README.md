# Conductor - A Distributed Task Scheduler with Saga Workflows

## Features

- Distributed Architecture
- **Individual Task Execution** -- submit, dispatch, and execute single tasks
- **Multi-Step Workflows (Saga Pattern)** -- execute ordered sequences of tasks with automatic compensation (rollback) on failure
- Task Priorities
- Automatic Retries
- Configurable Timeouts
- Output Capture
- Delayed Scheduling
- Health Monitoring
- Atomic Task Claiming

## Known Issues / TODO

- Task cancellation
- Single point of failure (coordinator)
- No recurring tasks
- No task dependencies

## How to Run

```bash
git clone https://github.com/ChinmayNoob/conductor.git
cd conductor
docker-compose up --build
```

Open a **new terminal** and build the client:

```bash
go build -o client ./cmd/client
```

## Usage

### Individual Tasks

Submit a single task for immediate execution:

```bash
.\client -action=submit -cmd="echo hello"
```

Schedule a task to run after a delay:

```bash
.\client -action=schedule -cmd="echo hello" -delay=30
```

Check task status:

```bash
.\client -action=status -id=<task-id>
```

Run the full individual task test suite:

```bash
.\client -action=test
```

### Workflows (Saga Pattern)

Submit a workflow (e.g. the built-in `trip_booking` example):

```bash
.\client -action=workflow -type=trip_booking -input="{\"user_id\":123,\"amount\":5000}"
```

Check workflow status (shows all steps and their states):

```bash
.\client -action=workflow-status -id=<workflow-id>
```

Run the full workflow test (submits `trip_booking` and monitors it):

```bash
.\client -action=workflow-test
```

### How Workflows Work

A workflow is a sequence of steps where each step has an **execute** command and a **compensate** (undo) command. The coordinator runs steps one by one. If any step fails permanently, it walks backward through all completed steps and runs their compensation commands.

There are two built-in workflow types to demonstrate both scenarios:

---

**1. `trip_booking` -- Success Scenario (all steps pass)**

```bash
.\client -action=workflow -type=trip_booking -input="{\"user_id\":123,\"amount\":5000}"
```

| Step | Execute | Compensate |
|------|---------|------------|
| 1. Book Flight | `echo booking flight for user=123` | `echo cancelling flight for user=123` |
| 2. Book Hotel | `echo booking hotel for user=123` | `echo cancelling hotel for user=123` |
| 3. Charge Payment | `echo charging amount=5000 for user=123` | `echo refunding amount=5000 for user=123` |

Expected output when monitoring:

```
Book Flight  -> COMPLETED
Book Hotel   -> COMPLETED
Charge Payment -> COMPLETED
Workflow: COMPLETED
```

---

**2. `trip_booking_fail` -- Failure + Compensation Scenario (step 2 fails)**

```bash
.\client -action=workflow -type=trip_booking_fail -input="{\"user_id\":123,\"amount\":5000}"
```

This is identical to `trip_booking` except the "Book Hotel" step uses `exit 1` to simulate a failure. After retries are exhausted, the coordinator automatically compensates:

Expected output when monitoring:

```
Book Flight  -> COMPLETED
Book Hotel   -> FAILED
  -- Compensation begins --
Book Flight  -> COMPENSATING -> COMPENSATED  (runs "cancelling flight")
Workflow: FAILED (compensation done)
```

Step 3 (Charge Payment) never runs because step 2 failed first. Only step 1 needs compensation since it was the only completed step.

---

**Want to create your own failure scenario?** Edit `pkg/workflow/engine.go` and change any step's `CommandTemplate` to a command that exits non-zero. For example, changing step 3 to fail:

```go
CommandTemplate: "echo payment declined && exit 1",
```

This would cause steps 1 and 2 to be compensated in reverse order (Cancel Hotel, then Cancel Flight).

### HTTP API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/tasks` | Submit a task for immediate execution |
| POST | `/tasks/schedule` | Schedule a task for later |
| GET | `/tasks/status?id=<id>` | Get task status |
| GET | `/tasks/list` | List all tasks |
| GET | `/tasks/stats` | Get task statistics |
| POST | `/workflows` | Submit a workflow |
| GET | `/workflows/status?id=<id>` | Get workflow status with all steps |
| GET | `/workflows/list` | List all workflows |
| GET | `/health` | Health check |

## Architecture

```
┌─────────────┐     HTTP      ┌─────────────┐
│   Client    │──────────────▶│  Scheduler  │
└─────────────┘               │  (port 8081)│
                              └──────┬──────┘
                                     │
                                     ▼
┌─────────────┐               ┌─────────────┐
│  PostgreSQL │◀─────────────▶│ Coordinator │
│  (port 5432)│               │ (port 8080) │
└─────────────┘               └──────┬──────┘
                                     │ gRPC
                    ┌────────────────┼────────────────┐
                    ▼                ▼                ▼
              ┌──────────┐    ┌──────────┐    ┌──────────┐
              │ Worker 1 │    │ Worker 2 │    │ Worker N │
              └──────────┘    └──────────┘    └──────────┘
```

### Database Tables

| Table | Purpose |
|-------|---------|
| `tasks` | Individual task records (status, priority, retries, output) |
| `workflows` | Workflow instances (type, status, current_step, context) |
| `workflow_steps` | Steps within a workflow (linked to tasks and compensation tasks) |

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `POSTGRES_HOST` | `postgres` | Database host |
| `POSTGRES_PORT` | `5432` | Database port |
| `POSTGRES_DB` | `taskscheduler` | Database name |
| `POSTGRES_USER` | `postgres` | Database user |
| `POSTGRES_PASSWORD` | `postgres` | Database password |

## Documentation

- [Saga Design Document](docs/SAGA_DESIGN.md) -- explains the saga pattern, database schema, and architecture
- [Saga Changelog](docs/SAGA_CHANGELOG.md) -- lists every file and function added for the saga implementation
