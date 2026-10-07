-- Cancellation for tasks and workflows.
ALTER TYPE task_status ADD VALUE IF NOT EXISTS 'CANCELLED';
ALTER TYPE workflow_status ADD VALUE IF NOT EXISTS 'CANCELLED';
ALTER TYPE step_status ADD VALUE IF NOT EXISTS 'CANCELLED';

ALTER TABLE tasks ADD COLUMN IF NOT EXISTS cancelled_at TIMESTAMP;
ALTER TABLE workflows ADD COLUMN IF NOT EXISTS cancel_requested BOOLEAN NOT NULL DEFAULT false;

-- API keys for the HTTP API. Only a SHA-256 hash of each key is stored.
CREATE TABLE IF NOT EXISTS api_keys (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name TEXT NOT NULL,
    key_hash BYTEA NOT NULL UNIQUE,
    prefix TEXT NOT NULL,           -- first characters of the key, for display
    is_admin BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    revoked_at TIMESTAMPTZ
);
