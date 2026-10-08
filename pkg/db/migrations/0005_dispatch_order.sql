-- Phase 3: an indexable dispatch order.
--
-- Priority aging ("a waiting task gains one priority level per interval")
-- orders tasks exactly like a virtual deadline:
--
--     dispatch_key = scheduled_at + priority * aging_interval
--
-- Unlike the old ORDER BY expression, which depended on NOW() and forced a
-- sort of every queued task on every pick, this is a stored column with a
-- partial index: a pick walks the index and stops at the first eligible
-- task. With aging disabled, a 100-year interval gives strict priority order.

CREATE TABLE IF NOT EXISTS scheduler_settings (
    id INT PRIMARY KEY CHECK (id = 1),
    priority_aging_seconds DOUBLE PRECISION NOT NULL DEFAULT 60
);
INSERT INTO scheduler_settings (id) VALUES (1) ON CONFLICT DO NOTHING;

CREATE OR REPLACE FUNCTION conductor_dispatch_key(priority INT, scheduled_at TIMESTAMP)
RETURNS TIMESTAMP AS $$
    SELECT scheduled_at + make_interval(secs => priority *
        CASE WHEN s.priority_aging_seconds > 0 THEN s.priority_aging_seconds ELSE 3153600000 END)
    FROM scheduler_settings s WHERE s.id = 1
$$ LANGUAGE sql STABLE;

CREATE OR REPLACE FUNCTION conductor_set_dispatch_key() RETURNS trigger AS $$
BEGIN
    NEW.dispatch_key := conductor_dispatch_key(NEW.priority, NEW.scheduled_at);
    RETURN NEW;
END $$ LANGUAGE plpgsql;

ALTER TABLE tasks ADD COLUMN IF NOT EXISTS dispatch_key TIMESTAMP;

DROP TRIGGER IF EXISTS tasks_dispatch_key ON tasks;
CREATE TRIGGER tasks_dispatch_key
    BEFORE INSERT OR UPDATE OF priority, scheduled_at ON tasks
    FOR EACH ROW EXECUTE FUNCTION conductor_set_dispatch_key();

UPDATE tasks SET dispatch_key = conductor_dispatch_key(priority, scheduled_at)
WHERE status = 'QUEUED';

CREATE INDEX IF NOT EXISTS idx_tasks_dispatch ON tasks (dispatch_key)
    WHERE status = 'QUEUED' AND picked_at IS NULL;
-- For finding when the next delayed task comes due.
CREATE INDEX IF NOT EXISTS idx_tasks_due ON tasks (scheduled_at)
    WHERE status = 'QUEUED' AND picked_at IS NULL;
DROP INDEX IF EXISTS idx_tasks_priority_scheduled;
