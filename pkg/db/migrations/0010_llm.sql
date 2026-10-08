-- Language model usage and limits (Phase 5).
--
-- Usage accumulates on the task across attempts: a failed or stale attempt
-- still spent tokens. Workflow and namespace spend are sums over tasks.
ALTER TABLE tasks
    ADD COLUMN IF NOT EXISTS llm_model TEXT,
    ADD COLUMN IF NOT EXISTS input_tokens BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS output_tokens BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS cost_usd NUMERIC(14, 6) NOT NULL DEFAULT 0;

-- A queue may cap the model tokens its tasks use per minute (dispatch waits).
ALTER TABLE queues ADD COLUMN IF NOT EXISTS tokens_per_minute BIGINT;

-- A namespace may cap daily model spend (new llm tasks are refused), and
-- opts in to the AI assistant (failure explanations and the like), which
-- sends redacted task output to the model provider.
ALTER TABLE namespaces
    ADD COLUMN IF NOT EXISTS max_llm_tokens_per_day BIGINT,
    ADD COLUMN IF NOT EXISTS max_llm_cost_per_day NUMERIC(14, 6),
    ADD COLUMN IF NOT EXISTS ai_assist BOOLEAN NOT NULL DEFAULT false;
