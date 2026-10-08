-- Earlier attempts of a task. The task row always describes its latest
-- attempt; when an attempt fails and the task is retried (or a failed task
-- is requeued), the attempt's outcome is kept here first, so "why did it
-- fail the first time?" stays answerable. Written only on failure paths.
CREATE TABLE IF NOT EXISTS task_attempts (
    task_id UUID NOT NULL REFERENCES tasks(id),
    attempt INT NOT NULL,
    worker_id BIGINT,
    started_at TIMESTAMP,
    finished_at TIMESTAMP NOT NULL,
    status TEXT NOT NULL,
    error_message TEXT,
    output TEXT,
    PRIMARY KEY (task_id, attempt)
);
