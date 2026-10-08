-- Phase 3: coordinator leader election, fencing, attempt IDs, and
-- notifications that wake the dispatcher.

-- Every dispatch of a task gets a new attempt number. Status reports must
-- carry the current one, so a report from an older dispatch (e.g. a worker
-- that was presumed dead) can't change a newer attempt.
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS attempt INT NOT NULL DEFAULT 0;

-- The single leader row. A coordinator becomes leader by holding a Postgres
-- advisory lock, then bumps epoch. Every write the leader makes first checks
-- the epoch (with a share lock on this row), so a deposed leader is fenced
-- off the moment its successor's bump commits.
CREATE TABLE IF NOT EXISTS coordinator_leader (
    id INT PRIMARY KEY CHECK (id = 1),
    epoch BIGINT NOT NULL DEFAULT 0,
    coordinator_id TEXT,
    address TEXT,
    elected_at TIMESTAMPTZ,
    heartbeat_at TIMESTAMPTZ
);
INSERT INTO coordinator_leader (id) VALUES (1) ON CONFLICT DO NOTHING;

-- Every running coordinator, leader or standby, for visibility.
CREATE TABLE IF NOT EXISTS coordinators (
    id TEXT PRIMARY KEY,
    address TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Wake the leader's dispatcher when a task becomes runnable (inserted,
-- retried, requeued) or a queue changes, instead of waiting for its next
-- poll. Postgres collapses identical notifications within a transaction.
CREATE OR REPLACE FUNCTION conductor_notify_tasks() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('conductor_tasks', '');
    RETURN NULL;
END $$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION conductor_notify_schedules() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('conductor_schedules', '');
    RETURN NULL;
END $$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS tasks_runnable_notify ON tasks;
CREATE TRIGGER tasks_runnable_notify
    AFTER INSERT OR UPDATE ON tasks
    FOR EACH ROW
    WHEN (NEW.status = 'QUEUED' AND NEW.picked_at IS NULL)
    EXECUTE FUNCTION conductor_notify_tasks();

DROP TRIGGER IF EXISTS queues_notify ON queues;
CREATE TRIGGER queues_notify
    AFTER INSERT OR UPDATE ON queues
    FOR EACH STATEMENT
    EXECUTE FUNCTION conductor_notify_tasks();

DROP TRIGGER IF EXISTS schedules_notify ON schedules;
CREATE TRIGGER schedules_notify
    AFTER INSERT OR UPDATE ON schedules
    FOR EACH STATEMENT
    EXECUTE FUNCTION conductor_notify_schedules();
