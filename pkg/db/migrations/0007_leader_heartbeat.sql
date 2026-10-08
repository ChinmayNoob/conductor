-- The leader's heartbeat moves off the coordinator_leader row. Every fenced
-- write holds a share lock on that row, so an UPDATE of it waited for all of
-- them, and every fenced write that arrived meanwhile queued behind the
-- UPDATE: each heartbeat stalled the hot path, and under database latency
-- the heartbeat timed out and the leader stepped down for no reason.
--
-- The heartbeat records the epoch it belongs to, so a deposed leader's late
-- heartbeat can't make a newer leader look alive, or vice versa.
-- coordinator_leader.heartbeat_at is no longer written.
CREATE TABLE IF NOT EXISTS leader_heartbeat (
    id INT PRIMARY KEY CHECK (id = 1),
    epoch BIGINT NOT NULL DEFAULT 0,
    heartbeat_at TIMESTAMPTZ
);
INSERT INTO leader_heartbeat (id, epoch, heartbeat_at)
SELECT 1, epoch, heartbeat_at FROM coordinator_leader WHERE id = 1
ON CONFLICT DO NOTHING;
