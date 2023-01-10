-- SQL migration to move sqlq primitive functions to custom user-defined functions

-- Function: sqlq.dequeue_job()
--
-- SQLQ.DEQUEUE_JOB() dequeues a job from one of the given queues, in a way that respects individual queues' concurrency.
-- It dequeues a job and move it to the 'running' state (also updating other relevant fields).
--
-- The function can result in a serialization error if run with REPEATABLE READ / SERIALIZABLE isolation level, if two
-- concurrent processes try to dequeue from the same queues at the same time. This is to protect the queues' concurrency guarantees.
CREATE FUNCTION sqlq.dequeue_job(queues TEXT[], jobTypes TEXT[]) RETURNS SETOF sqlq.jobs AS $$
    WITH queues AS (
        SELECT name, concurrency, priority FROM sqlq.queues WHERE name = ANY ($1)
    ), running (name, count) AS (
        SELECT queue, COUNT(*) FROM sqlq.jobs, queues
            WHERE jobs.queue = queues.name AND status = 'running'
        GROUP BY queue
    ), queue_with_capacity AS (
        SELECT queues.name, queues.priority FROM queues LEFT OUTER JOIN running USING(name)
            WHERE (concurrency IS NULL OR (concurrency - COALESCE(running.count, 0) > 0))
    ), dequeued(id) AS (
        SELECT job.id FROM sqlq.jobs job, queue_with_capacity q
            WHERE job.status = 'pending'
              AND (job.last_queued_at+make_interval(secs => job.run_after/1e9)) <= NOW() -- value in run_after is stored as nanoseconds
              AND job.queue = q.name
              AND (ARRAY_LENGTH($2, 1) IS NULL OR job.typename = ANY($2))
        ORDER BY q.priority DESC, job.priority DESC, job.created_at
        LIMIT 1
    )
    UPDATE sqlq.jobs
        SET status = 'running', started_at = NOW(), last_keepalive = NOW(), attempt = attempt + 1
    FROM dequeued dq
        WHERE jobs.id = dq.id
    RETURNING jobs.*
$$ LANGUAGE SQL;
