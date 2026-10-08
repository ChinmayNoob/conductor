# Benchmarks

Baseline numbers for Conductor at the end of Phase 3, measured with
[`conductor-bench`](cmd/conductor-bench/main.go) and
[`scripts/bench.sh`](scripts/bench.sh) (`make bench`). Raw results:
[docs/benchmarks/2026-10-08-laptop.jsonl](docs/benchmarks/2026-10-08-laptop.jsonl).

> **Read these as a floor, not a ceiling.** Everything ran on one laptop
> (Intel Core Ultra 5 225U, Docker Desktop on Windows, a VM with 14 CPUs and
> 7.5 GB): Postgres, both coordinators, the API, up to 50 worker containers
> *and* the load generator, all competing for the same CPUs and the same
> virtualized disk. A dedicated Postgres would raise every number here.

## What is measured

Every task is a no-op shell command (`true`), so the numbers are Conductor's
own overhead: claiming, dispatching, recording results. All timestamps come
from the Postgres clock.

| Metric | How |
|---|---|
| **Ingest** | Tasks/s accepted by `POST /v1/tasks` from 32 concurrent clients |
| **Drain** | 4,000 tasks are submitted to a paused queue, which is then resumed: tasks/s from resume to the last completion |
| **Idle dispatch** | One task at a time on an idle cluster: submit → claimed |
| **Latency under load** | Tasks submitted at a fixed rate for 10 s (open loop). *Dispatch* is submit → claimed; *end to end* is submit → result recorded. Rates above 80% of drain are skipped |
| **Workflows** | 100 runs of a four-step diamond (a → b, c → d) started at once |

Each worker runs 4 tasks at a time (`CONDUCTOR_WORKER_SLOTS=4`), logging at
`warn` (at `info`, two log lines per task cost measurable throughput).

## Results

### Throughput

| Workers | Slots | Drain | Ingest | Idle dispatch p50 |
|---:|---:|---:|---:|---:|
| 1 | 4 | **151** tasks/s | 1,010/s | 6.4 ms |
| 10 | 40 | **808** tasks/s | 1,077/s | 4.7 ms |
| 50 | 200 | **697** tasks/s | 1,159/s | 4.4 ms |

With one worker, the 4 slots are the limit. By 10 workers the slots stop
mattering and the limit becomes Postgres. Going to 50 workers adds no
throughput and costs some, because the extra containers compete for the
same CPUs.

### Latency under load

Dispatch / end-to-end latency, p50 and p99, in milliseconds:

| Rate | 1 worker | 10 workers | 50 workers |
|---:|---|---|---|
| 50/s | 2.4 / 9.1 · 10 / 19 | 3.7 / 15 · 15 / 32 | 4.2 / 15 · 14 / 33 |
| 100/s | 4.7 / 15 · 15 / 31 | 6.3 / 21 · 20 / 49 | 6.0 / 23 · 19 / 47 |
| 200/s | over capacity | **9.4 / 34 · 27 / 90** | 59 / 519 · 189 / 815 |
| ~300–400/s | over capacity | 1,181 / 1,939 · 1,249 / 2,009 (overloaded) | 66 / 248 · 176 / 501 (309/s achieved) |

- Up to 200 tasks/s on 10 workers, a task reaches a worker in under **10 ms**
  (p50) and is done in under **30 ms**.
- With ingest and dispatch both running, the sustainable rate on this
  machine is somewhere between 200 and 400 tasks/s. Drain alone reaches
  800/s, but the API's inserts share the same Postgres. Past that point the
  queue grows and latency climbs steadily (the 10-worker 400/s row).
- The 50-worker rows are noisy: 50 containers plus the load generator
  saturate the VM. The 200/s point came out worse than the 309/s one; read
  both as "loaded but keeping up".

### Workflows

| Workers | p50 | p99 | Runs/s |
|---:|---:|---:|---:|
| 1 | 2,246 ms | 2,348 ms | 34 |
| 10 | 976 ms | 1,326 ms | 56 |
| 50 | 976 ms | 1,462 ms | 55 |

100 runs started together means 400 tasks over three dependency levels. On
one worker that is capacity-bound (400 tasks at 151/s); with more workers a
run takes about 1 s, roughly 300 ms per level including the reconcile that
creates the next step.

## How we got here

Phase 3 began at 139 tasks/s (4 workers, 8 slots). Profiling with
`pg_stat_statements`, Postgres wait events and connection-pool statistics
found the limits one at a time:

| Fix | Effect |
|---|---|
| `LISTEN/NOTIFY` instead of a 1 s poll | Idle dispatch: up to 1 s → ~5 ms |
| Indexed `dispatch_key` + batch claims | A claim no longer sorts every queued task |
| Leader's own writes skip `NOTIFY` (it was serializing commits) | Ingest 541 → 629/s |
| Keep all pooled connections (forking backends cost CPU) | Drain 392 → 547/s |
| Epoch fence inside the statement, not a 4-round-trip transaction | Fewer pool waits |
| Workers free a slot before reporting (fixed a reject-and-requeue race) | No spurious requeues |
| Coordinator records STARTED on accept (no worker round trip) | Dispatch p50 at steady load 1,607 → 96 ms |
| Benchmark split ingest from drain | Honest numbers: drain ~980/s on 50 slots |

The full story, with the dead ends, is in the [DEVLOG](DEVLOG.md#phase-3-distributed-systems-depth).

## Where the time goes now

Per task, the leader runs four writes: the insert, its share of a batch claim,
STARTED and COMPLETED. The API adds an API-key lookup and the coordinator a
namespace check. On this machine Postgres CPU is the limit. The next levers,
in expected order of payoff:

1. **Batch result writes:** group the COMPLETE reports that arrive within a
   few milliseconds into one statement (application-level group commit).
2. **Cache API keys and namespace settings** for a few seconds, removing two
   reads from every submission.
3. **Prepared statements** (or pgx) to stop re-planning the hot statements.
4. Fold STARTED into the claim when a worker's slot is already reserved.

## Targets

On dedicated hardware (a 4 vCPU / 16 GB Postgres, coordinators separate):

| Metric | Today (laptop) | Target |
|---|---|---|
| Drain | 808/s | ≥ 3,000/s |
| Sustained rate with p99 dispatch < 50 ms | 200/s | ≥ 1,000/s |
| Idle dispatch p50 | 4.4 ms | < 10 ms ✅ |
| Leader failover | 0.5–1 s | < 2 s ✅ |

Re-run `make bench` after any change to the hot path, and record the
environment next to the numbers.
