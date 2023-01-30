-- Migration to change  jobs primary key to UUID and all tables dependant of it.
-- We create an extension to use a random UUID as default each time we create the column.
-- This migration accepts data loss because we are only using it internally for now.

-- The solution is to drop the existing tables and all their dependencies  and 
-- recreate them with the UUID changes .
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Dropping the tables also drops the associated functions 
DROP TABLE IF EXISTS sqlq.jobs CASCADE;
DROP TABLE IF EXISTS sqlq.job_logs CASCADE;

CREATE TABLE sqlq.jobs
(
    id            UUID PRIMARY KEY DEFAULT public.gen_random_uuid() NOT NULL, -- unique identifier for the job in the system
    queue         TEXT            NOT NULL,                     -- the queue this job belongs to
    typename      TEXT            NOT NULL,                     -- typename of the job registered with the runtime
    status        sqlq.job_states NOT NULL DEFAULT ('pending'), -- state of a job in its state machine
    priority      INT             NOT NULL DEFAULT (1),         -- the job's priority within the queue

    -- job's input and output
    parameters    JSON            NULL,
    result        JSON            NULL,
    max_retries   INT                        DEFAULT (1),       -- max number of attempts before considering a job as failed
    attempt       INT                        DEFAULT (0),       -- current attempt of the job
    last_queued_at TIMESTAMPTZ NOT NULL DEFAULT (now()),        -- when the job was last enqueued (including retries)
    created_at    TIMESTAMPTZ     NOT NULL DEFAULT (now()),     -- when the job was created
    started_at    TIMESTAMPTZ,                                  -- when the job last transitioned from PENDING -> RUNNING
    completed_at  TIMESTAMPTZ,                                  -- when the job transitioned into a completion state (SUCCESS / ERROR)

    -- Column keepalive_interval is the interval in seconds at which the worker
    --is expected to send the pings. Defaults to 30 seconds
    keepalive_interval BIGINT NOT NULL DEFAULT (30 * 1e9), 

    last_keepalive TIMESTAMPTZ,                                 -- Column last_keepalive stores the last ping's time

    -- Seconds after which the task must be executed (regardless of available capacity)
    -- This is used to implement "future execution" of task and exponential back-off retry policies.
    run_after     BIGINT                   DEFAULT (0),

    -- Seconds to retain the output of the job after it has completed before its cleared.
    -- A jobs is cleared by the runtime as soon as NOW() > (completed_at + retention_ttl) WHERE completed_at IS NOT NULL
    retention_ttl BIGINT          NOT NULL DEFAULT (0),

    -- Queue references sqlq.queue entry.
    -- We have an ON DELETE CASCADE to remove a job when the queue is deleted.
    FOREIGN KEY (queue) REFERENCES sqlq.queues (name) ON DELETE CASCADE
);

CREATE TABLE sqlq.job_logs
(
    id        UUID PRIMARY KEY DEFAULT public.gen_random_uuid() NOT NULL, -- unique identifier for the job_log in the system
    job       UUID DEFAULT public.gen_random_uuid() NOT NULL, -- job this log belongs to
    logged_at TIMESTAMPTZ DEFAULT (now()), -- time at which this message was logged into the database
    level     sqlq.log_level,              -- log level for this message
    message   TEXT,                        -- text content of the log message

    -- Job this log belongs to.
    FOREIGN KEY (job) REFERENCES sqlq.jobs (id) ON DELETE CASCADE
);

-- Index to assist most usual query types involving a queue, a job's type and its current status.
-- The index is verified as being used by postgres while executing the dequeue query.
CREATE INDEX ix_jobs_queue_type_status ON sqlq.jobs (queue, typename, status);

-- Index: ix_logs_job
-- Index on job_logs.job to assist in fetching logs for a given job.
CREATE INDEX ix_logs_job ON sqlq.job_logs (job);

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
CREATE FUNCTION sqlq.mark_success(id UUID, expectedState SQLQ.JOB_STATES)
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
CREATE FUNCTION sqlq.mark_failed(id UUID, expectedState SQLQ.JOB_STATES, retry BOOLEAN = false, run_after BIGINT = 0)
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
