-- Initial schema. Idempotent so databases created by the old setup.sql
-- (before migrations existed) upgrade cleanly.

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

DO $$ BEGIN
    CREATE TYPE task_status AS ENUM ('QUEUED', 'STARTED', 'COMPLETED', 'FAILED');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

CREATE TABLE IF NOT EXISTS tasks (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    data TEXT NOT NULL,
    status task_status DEFAULT 'QUEUED',

    scheduled_at TIMESTAMP DEFAULT NOW(),
    picked_at TIMESTAMP,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    failed_at TIMESTAMP,

    priority INT DEFAULT 5 CHECK (priority >= 1 AND priority <= 10),

    max_retries INT DEFAULT 3,
    retry_count INT DEFAULT 0,
    retry_delay_seconds INT DEFAULT 60,

    timeout_seconds INT DEFAULT 300,

    output TEXT,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_tasks_priority_scheduled ON tasks (priority ASC, scheduled_at ASC)
WHERE status = 'QUEUED' AND picked_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks (status);
CREATE INDEX IF NOT EXISTS idx_tasks_scheduled_at ON tasks (scheduled_at);

-- Saga / workflow support

DO $$ BEGIN
    CREATE TYPE workflow_status AS ENUM ('RUNNING', 'COMPENSATING', 'COMPLETED', 'FAILED');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE step_status AS ENUM ('PENDING', 'RUNNING', 'COMPLETED', 'FAILED', 'COMPENSATING', 'COMPENSATED');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

CREATE TABLE IF NOT EXISTS workflows (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    type TEXT NOT NULL,
    status workflow_status DEFAULT 'RUNNING',
    current_step INT DEFAULT 0,
    context JSONB DEFAULT '{}',
    error_message TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS workflow_steps (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    workflow_id UUID NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
    step_number INT NOT NULL,
    name TEXT NOT NULL,
    task_id UUID REFERENCES tasks(id),
    compensation_task_id UUID REFERENCES tasks(id),
    status step_status DEFAULT 'PENDING',
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE (workflow_id, step_number)
);

CREATE INDEX IF NOT EXISTS idx_workflows_status ON workflows (status);
CREATE INDEX IF NOT EXISTS idx_workflow_steps_workflow ON workflow_steps (workflow_id, step_number);
CREATE INDEX IF NOT EXISTS idx_workflow_steps_task ON workflow_steps (task_id);
CREATE INDEX IF NOT EXISTS idx_workflow_steps_compensation_task ON workflow_steps (compensation_task_id);
