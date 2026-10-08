-- Durable agents: a model calling tools in a loop, where every model call
-- and every tool call is its own task. The conversation and progress live
-- here, so a crash resumes from the last finished call.
CREATE TABLE IF NOT EXISTS agent_runs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    namespace TEXT NOT NULL,
    workflow_id UUID NOT NULL REFERENCES workflows(id),
    step_id UUID NOT NULL REFERENCES workflow_steps(id),
    spec JSONB NOT NULL,                     -- the resolved agent spec
    messages JSONB NOT NULL DEFAULT '[]',    -- the conversation so far
    turn INT NOT NULL DEFAULT 1,             -- the current model turn
    phase TEXT NOT NULL DEFAULT 'think',     -- think (model call) or act (tool calls)
    tool_calls INT NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'RUNNING',  -- RUNNING, COMPLETED, FAILED, CANCELLED
    answer TEXT,
    error_message TEXT,
    deadline TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_agent_runs_running ON agent_runs (updated_at) WHERE status = 'RUNNING';

ALTER TABLE workflow_steps ADD COLUMN IF NOT EXISTS agent_run_id UUID REFERENCES agent_runs(id);

-- A task's place in an agent run: its turn, and either the model call
-- (role llm) or one tool call (role tool).
ALTER TABLE tasks
    ADD COLUMN IF NOT EXISTS agent_run_id UUID REFERENCES agent_runs(id),
    ADD COLUMN IF NOT EXISTS agent_turn INT,
    ADD COLUMN IF NOT EXISTS agent_role TEXT,
    ADD COLUMN IF NOT EXISTS tool_call_id TEXT;
CREATE INDEX IF NOT EXISTS idx_tasks_agent_run ON tasks (agent_run_id, agent_turn) WHERE agent_run_id IS NOT NULL;
