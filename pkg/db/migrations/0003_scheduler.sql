-- Phase 2: namespaces, queues, task types, labels, workflow definitions (DAGs),
-- cron schedules and a worker registry.

-- Namespaces isolate tenants: every task, workflow, definition, schedule,
-- queue and API key belongs to one.
CREATE TABLE IF NOT EXISTS namespaces (
    name TEXT PRIMARY KEY,
    max_pending_tasks INT CHECK (max_pending_tasks > 0),   -- submissions beyond this get HTTP 429
    max_concurrency INT CHECK (max_concurrency > 0),       -- tasks running at once
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
INSERT INTO namespaces (name) VALUES ('default') ON CONFLICT DO NOTHING;

ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS namespace TEXT NOT NULL DEFAULT 'default'
    REFERENCES namespaces(name) ON DELETE CASCADE;

-- Tasks: type-specific spec, environment, label requirements, queue,
-- idempotency, and step outputs.
ALTER TABLE tasks
    ADD COLUMN IF NOT EXISTS namespace TEXT NOT NULL DEFAULT 'default',
    ADD COLUMN IF NOT EXISTS queue TEXT NOT NULL DEFAULT 'default',
    ADD COLUMN IF NOT EXISTS type TEXT NOT NULL DEFAULT 'shell',
    ADD COLUMN IF NOT EXISTS spec JSONB,
    ADD COLUMN IF NOT EXISTS env JSONB NOT NULL DEFAULT '{}',
    ADD COLUMN IF NOT EXISTS requirements JSONB NOT NULL DEFAULT '{}',
    ADD COLUMN IF NOT EXISTS idempotency_key TEXT,
    ADD COLUMN IF NOT EXISTS workflow_id UUID REFERENCES workflows(id) ON DELETE SET NULL,
    ADD COLUMN IF NOT EXISTS outputs JSONB NOT NULL DEFAULT '{}',
    ADD COLUMN IF NOT EXISTS last_dispatched_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS worker_id BIGINT;

UPDATE tasks t SET workflow_id = s.workflow_id
FROM workflow_steps s
WHERE t.workflow_id IS NULL AND (s.task_id = t.id OR s.compensation_task_id = t.id);

CREATE UNIQUE INDEX IF NOT EXISTS idx_tasks_idempotency
    ON tasks (namespace, idempotency_key) WHERE idempotency_key IS NOT NULL;
-- Concurrency limits count dispatched tasks per queue.
CREATE INDEX IF NOT EXISTS idx_tasks_in_flight
    ON tasks (namespace, queue) WHERE picked_at IS NOT NULL AND status IN ('QUEUED', 'STARTED');
-- Rate limits count recent dispatches per queue.
CREATE INDEX IF NOT EXISTS idx_tasks_dispatched ON tasks (namespace, queue, last_dispatched_at);
CREATE INDEX IF NOT EXISTS idx_tasks_namespace_created ON tasks (namespace, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_tasks_workflow ON tasks (workflow_id);

-- Queues: optional limits per (namespace, queue). A task's queue doesn't need
-- a row here; missing means unlimited.
CREATE TABLE IF NOT EXISTS queues (
    namespace TEXT NOT NULL REFERENCES namespaces(name) ON DELETE CASCADE,
    name TEXT NOT NULL,
    concurrency_limit INT CHECK (concurrency_limit > 0),
    rate_limit INT CHECK (rate_limit > 0),                 -- dispatches per rate_period_seconds
    rate_period_seconds INT NOT NULL DEFAULT 1 CHECK (rate_period_seconds > 0),
    paused BOOLEAN NOT NULL DEFAULT false,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (namespace, name)
);

-- Versioned workflow definitions (the DAG spec, stored as JSON).
CREATE TABLE IF NOT EXISTS workflow_definitions (
    namespace TEXT NOT NULL REFERENCES namespaces(name) ON DELETE CASCADE,
    name TEXT NOT NULL,
    version INT NOT NULL,
    spec JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (namespace, name, version)
);

-- Each run keeps a snapshot of the definition it started with, so editing a
-- definition never changes runs in flight.
ALTER TABLE workflows
    ADD COLUMN IF NOT EXISTS namespace TEXT NOT NULL DEFAULT 'default',
    ADD COLUMN IF NOT EXISTS definition JSONB,
    ADD COLUMN IF NOT EXISTS definition_version INT,
    ADD COLUMN IF NOT EXISTS idempotency_key TEXT;
CREATE UNIQUE INDEX IF NOT EXISTS idx_workflows_idempotency
    ON workflows (namespace, idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_workflows_namespace_created ON workflows (namespace, created_at DESC);

ALTER TYPE step_status ADD VALUE IF NOT EXISTS 'SKIPPED';
ALTER TYPE step_status ADD VALUE IF NOT EXISTS 'COMPENSATION_FAILED';

-- Cron schedules that start a task or a workflow.
CREATE TABLE IF NOT EXISTS schedules (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    namespace TEXT NOT NULL REFERENCES namespaces(name) ON DELETE CASCADE,
    name TEXT NOT NULL,
    cron TEXT NOT NULL,
    timezone TEXT NOT NULL DEFAULT 'UTC',
    misfire_policy TEXT NOT NULL DEFAULT 'skip' CHECK (misfire_policy IN ('skip', 'run_once', 'catch_up')),
    target JSONB NOT NULL,                  -- {"task": {...}} or {"workflow": {...}}
    enabled BOOLEAN NOT NULL DEFAULT true,
    next_run_at TIMESTAMPTZ NOT NULL,
    last_run_at TIMESTAMPTZ,
    last_run_id UUID,                       -- the task or workflow it last started
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (namespace, name)
);
CREATE INDEX IF NOT EXISTS idx_schedules_due ON schedules (next_run_at) WHERE enabled;

-- Workers as last reported, for visibility. The coordinator's in-memory view
-- is authoritative for dispatching.
CREATE TABLE IF NOT EXISTS workers (
    id BIGINT PRIMARY KEY,
    address TEXT NOT NULL,
    labels JSONB NOT NULL DEFAULT '{}',
    slots INT NOT NULL DEFAULT 1,
    running INT NOT NULL DEFAULT 0,
    status TEXT NOT NULL,                   -- healthy, draining, unhealthy
    first_seen TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
