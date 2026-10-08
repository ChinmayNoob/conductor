-- Steps that wait for a person (approval) or an outside system (signal)
-- instead of running a task. While waiting the step is RUNNING with no task;
-- a decision resolves it like a finished task: approved or signalled means
-- COMPLETED, rejected or timed out means FAILED, cancelled means CANCELLED.
ALTER TABLE workflow_steps
    ADD COLUMN IF NOT EXISTS wait_kind TEXT,            -- approval or signal
    ADD COLUMN IF NOT EXISTS wait_signal TEXT,          -- the signal's name
    ADD COLUMN IF NOT EXISTS wait_message TEXT,         -- what a person is asked
    ADD COLUMN IF NOT EXISTS wait_deadline TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS wait_on_timeout TEXT,      -- approve, reject (approval); fail (signal)
    ADD COLUMN IF NOT EXISTS decision TEXT,             -- COMPLETED, FAILED or CANCELLED
    ADD COLUMN IF NOT EXISTS decision_error TEXT,
    ADD COLUMN IF NOT EXISTS decided_by TEXT,
    ADD COLUMN IF NOT EXISTS decided_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS step_outputs JSONB;

CREATE INDEX IF NOT EXISTS idx_workflow_steps_waiting ON workflow_steps (wait_deadline)
    WHERE wait_kind IS NOT NULL AND decision IS NULL;

-- Signals sent to a run. One that arrives before its step starts waits here.
CREATE TABLE IF NOT EXISTS workflow_signals (
    id BIGSERIAL PRIMARY KEY,
    workflow_id UUID NOT NULL REFERENCES workflows(id),
    name TEXT NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}',
    sent_by TEXT,
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    consumed_by UUID REFERENCES workflow_steps(id)
);
CREATE INDEX IF NOT EXISTS idx_workflow_signals_pending ON workflow_signals (workflow_id, name)
    WHERE consumed_by IS NULL;
