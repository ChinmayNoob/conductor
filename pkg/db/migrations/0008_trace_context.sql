-- The W3C traceparent of whoever submitted a task or started a workflow. A
-- task waits here between submission and dispatch, so this is how its
-- dispatch and run spans join the submitter's trace. Workflow steps take the
-- run's.
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS trace_parent TEXT;
ALTER TABLE workflows ADD COLUMN IF NOT EXISTS trace_parent TEXT;
