-- NOTIFY serializes commits: Postgres holds a database-wide lock from the
-- moment a notifying transaction starts to commit until its WAL is flushed.
-- With a notification on every task insert, task creation ran one commit at a
-- time.
--
-- The leader coordinator makes almost every write that makes a task runnable
-- (submissions, workflow steps, schedule runs, retries), and it wakes its own
-- dispatcher after each one. Its sessions set conductor.local_wake = on, and
-- the triggers skip the notification for them. Writes from anywhere else (the
-- API, an operator's SQL) still notify.

DROP TRIGGER IF EXISTS tasks_runnable_notify ON tasks;
CREATE TRIGGER tasks_runnable_notify
    AFTER INSERT OR UPDATE ON tasks
    FOR EACH ROW
    WHEN (NEW.status = 'QUEUED' AND NEW.picked_at IS NULL
          AND current_setting('conductor.local_wake', true) IS DISTINCT FROM 'on')
    EXECUTE FUNCTION conductor_notify_tasks();

DROP TRIGGER IF EXISTS schedules_notify ON schedules;
CREATE TRIGGER schedules_notify
    AFTER INSERT OR UPDATE ON schedules
    FOR EACH STATEMENT
    WHEN (current_setting('conductor.local_wake', true) IS DISTINCT FROM 'on')
    EXECUTE FUNCTION conductor_notify_schedules();
