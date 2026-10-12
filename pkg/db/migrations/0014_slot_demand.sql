-- How many task slots a pool of workers could use right now: tasks it
-- could run that are due (not delayed, not in a paused queue) or about to
-- start, plus tasks running on its workers. pool_labels are the labels its
-- workers advertise; a dedicated pool also passes its exclusive labels,
-- which tasks must require. Autoscalers such as KEDA's postgresql scaler
-- divide this by the slots per worker:
--
--   SELECT conductor_slot_demand('{"pool": "gpu", "gpu": "true", "type.shell": "true"}', '{"pool": "gpu"}')
--
-- Keeping the query here, versioned with the schema, means autoscaler
-- settings never depend on table layout. SECURITY DEFINER lets a database
-- role with no table access (only EXECUTE, which PUBLIC has) call it.
CREATE OR REPLACE FUNCTION conductor_slot_demand(pool_labels JSONB, exclusive JSONB DEFAULT '{}')
RETURNS BIGINT
LANGUAGE sql STABLE SECURITY DEFINER SET search_path FROM CURRENT AS $$
    SELECT
        (SELECT count(*)
         FROM tasks t
         LEFT JOIN queues q ON q.namespace = t.namespace AND q.name = t.queue
         WHERE t.status = 'QUEUED'
           AND t.requirements <@ pool_labels
           AND t.requirements @> exclusive
           AND (t.picked_at IS NOT NULL
                OR (t.scheduled_at <= NOW() AND NOT COALESCE(q.paused, false))))
      + (SELECT count(*)
         FROM tasks t
         JOIN workers w ON w.id = t.worker_id
         WHERE t.status = 'STARTED' AND w.labels @> pool_labels)
$$;
