-- Explanations of failed tasks, from the operations assistant. One per task:
-- the explanation of its latest failure. The classification says whether a
-- retry is likely to help; the cause and fix are the model's reading of the
-- (redacted) error and output.
CREATE TABLE IF NOT EXISTS task_explanations (
    task_id UUID PRIMARY KEY REFERENCES tasks(id),
    attempt INT NOT NULL,
    class TEXT NOT NULL,           -- transient, permanent, needs_attention, unknown
    confidence DOUBLE PRECISION NOT NULL,
    source TEXT NOT NULL,          -- rules or model
    cause TEXT NOT NULL DEFAULT '',
    fix TEXT NOT NULL DEFAULT '',
    model TEXT NOT NULL DEFAULT '',
    input_tokens BIGINT NOT NULL DEFAULT 0,
    output_tokens BIGINT NOT NULL DEFAULT 0,
    cost_usd DOUBLE PRECISION NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);
