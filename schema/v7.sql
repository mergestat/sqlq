-- SQL migration to add introspection functions

-- Function: sqlq.job_logs()
--
-- sqlq.job_logs() return all logs emitted by the job until time of invocation.
CREATE FUNCTION sqlq.job_logs(id BIGINT)
-- @formatter:off
    RETURNS TABLE (ts TIMESTAMPTZ, level SQLQ.LOG_LEVEL, msg TEXT) AS
-- @formatter:on
$$
BEGIN
    RETURN QUERY
        SELECT log.logged_at, log.level, log.message
        FROM sqlq.job_logs log
        WHERE log.job = id;
END;
$$ LANGUAGE plpgsql STABLE;

-- Type: sqlq.queue_info
--
-- Used as return type of sqlq.queue_info(text) function below.
CREATE TYPE sqlq.queue_info AS
(
    pending BIGINT, -- # of tasks in pending state
    running BIGINT, -- # of tasks in running state
    success BIGINT, -- # of tasks in success state
    errored BIGINT  -- # of tasks in errored state
);

-- Function: sqlq.queue_info()
--
-- sqlq.queue_info() return details about the given queue.
CREATE FUNCTION sqlq.queue_info(name TEXT) RETURNS sqlq.queue_info AS
$$
DECLARE
    out sqlq.queue_info%rowtype;
BEGIN
    SELECT INTO out SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) AS pending,
                    SUM(CASE WHEN status = 'running' THEN 1 ELSE 0 END) AS running,
                    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) AS success,
                    SUM(CASE WHEN status = 'errored' THEN 1 ELSE 0 END) AS errored
    FROM sqlq.jobs
    WHERE queue = name;

    RETURN out;
END;
$$ LANGUAGE plpgsql STABLE;
