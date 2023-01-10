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

-- Function: sqlq.mark_success
--
-- SQLQ.MARK_SUCCESS() transitions the job to 'success' state and mark it as completed.
CREATE FUNCTION sqlq.mark_success(id INTEGER, expectedState SQLQ.JOB_STATES)
RETURNS SETOF sqlq.jobs AS $$
BEGIN
    RETURN QUERY UPDATE sqlq.jobs SET status = 'success', completed_at = NOW()
        WHERE jobs.id = $1 AND status = $2
    RETURNING *;
END;
$$ LANGUAGE plpgsql VOLATILE;

-- Function: sqlq.mark_failed
--
-- SQLQ.MARK_FAILED() transitions the job to ERROR state and mark it as completed. If the error is retryable
-- (and there are still attempts left), we transition it to PENDING to be picked up again by a worker.
CREATE FUNCTION sqlq.mark_failed(id INTEGER, expectedState SQLQ.JOB_STATES, retry BOOLEAN = false, run_after BIGINT = 0)
RETURNS SETOF sqlq.jobs AS $$
BEGIN
    IF retry THEN
        RETURN QUERY
            UPDATE sqlq.jobs SET status = 'pending', last_queued_at = NOW(), run_after = $4
                WHERE jobs.id = $1 AND status = $2 RETURNING *;
    ELSE
        RETURN QUERY
            UPDATE sqlq.jobs SET status = 'errored', completed_at = NOW()
                WHERE jobs.id = $1 AND status = $2 RETURNING *;
    END IF;
END;
$$ LANGUAGE plpgsql VOLATILE;

-- Function: sqlq.reap()
--
-- SQLQ.REAP() reaps any zombie process, processes where state is 'running' but the job hasn't pinged in a while, in the given queues.
-- It moves any job with remaining attempts back to the queue while dumping all others in to the errored state.
CREATE FUNCTION sqlq.reap(queues TEXT[]) RETURNS integer AS $$
DECLARE
    count INTEGER;
BEGIN
    WITH dead AS (
	    SELECT id, attempt, max_retries FROM sqlq.jobs
	        WHERE status = 'running' AND queue = ANY($1) AND (NOW() > last_keepalive + make_interval(secs => keepalive_interval / 1e9))
	)
    UPDATE sqlq.jobs
        SET status = (CASE WHEN dead.attempt < dead.max_retries THEN 'pending'::sqlq.job_states ELSE 'errored'::sqlq.job_states END)
    FROM dead WHERE jobs.id = dead.id;

    GET DIAGNOSTICS count = ROW_COUNT;
    RETURN count;
END;
$$ LANGUAGE plpgsql VOLATILE;
