CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TYPE task_status AS ENUM ('QUEUED','STARTED','COMPLETED','FAILED');

CREATE TABLE tasks (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    data TEXT NOT NULL,
    status task_status DEFAULT 'QUEUED',

    -- //scheduling
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

CREATE INDEX idx_tasks_priority_scheduled ON tasks (priority ASC, scheduled_at ASC)
WHERE status = 'QUEUED' AND picked_at IS NULL;
CREATE INDEX idx_tasks_status ON tasks (status);
CREATE INDEX idx_tasks_scheduled_at ON tasks(scheduled_at);
