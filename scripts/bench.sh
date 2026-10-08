#!/usr/bin/env bash
# Runs conductor-bench against the Docker Compose stack at several cluster
# sizes and appends one JSON line per run to $OUT.
#
#   scripts/bench.sh                  # 1, 10 and 50 workers, 4 slots each
#   WORKERS="1 10" SLOTS=8 scripts/bench.sh
#
# Logging is turned down to warnings: at info level, two log lines per task
# cost a measurable share of throughput.
set -euo pipefail

WORKERS=${WORKERS:-"1 10 50"}
SLOTS=${SLOTS:-4}
TASKS=${TASKS:-4000}
WORKFLOWS=${WORKFLOWS:-100}
OUT=${OUT:-bench-results.jsonl}
export CONDUCTOR_API_KEY=${CONDUCTOR_API_KEY:-insecure-dev-api-key}

go build -o bin/conductor-bench ./cmd/conductor-bench
# Only the plain workers take part.
docker compose --profile containers stop container-worker >/dev/null 2>&1 || true

for n in $WORKERS; do
  echo "== $n workers x $SLOTS slots"
  CONDUCTOR_WORKER_SLOTS=$SLOTS CONDUCTOR_LOG_LEVEL=warn \
    docker compose up -d --build --scale worker="$n" --remove-orphans
  # Wait until every worker has registered, and stale ones have expired.
  for _ in $(seq 1 90); do
    got=$(curl -fsS -H "Authorization: Bearer $CONDUCTOR_API_KEY" localhost:8081/v1/workers 2>/dev/null |
      grep -o '"status":"healthy"' | wc -l || true)
    [ "$got" -eq "$n" ] && break
    sleep 2
  done
  sleep 5
  bin/conductor-bench -label "${n}w-${SLOTS}s" -tasks "$TASKS" -workflows "$WORKFLOWS" -out "$OUT"
done
