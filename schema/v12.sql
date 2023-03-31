-- SQL migration to update sqlq.reap routine and make queues filter optional

-- Function: sqlq.reap()
--
-- SQLQ.REAP() reaps any zombie process, processes where state is 'running' but the job hasn't pinged in a while (determined by the `keepalive_interval` defined on the job), in the given queues.
-- It moves any job with remaining attempts back to the queue while dumping all others in to the errored state.
CREATE OR REPLACE FUNCTION sqlq.reap(queues TEXT[]) RETURNS integer AS $$
DECLARE
    count INTEGER;
BEGIN
    WITH dead AS (
        SELECT id, attempt, max_retries FROM sqlq.jobs
        WHERE status = 'running'
          AND (ARRAY_LENGTH($1, 1) IS NULL OR queue = ANY($1))
          AND (NOW() > last_keepalive + make_interval(secs => keepalive_interval / 1e9))
    )
    UPDATE sqlq.jobs
    SET status = (CASE WHEN dead.attempt < dead.max_retries THEN 'pending'::sqlq.job_states ELSE 'errored'::sqlq.job_states END)
    FROM dead WHERE jobs.id = dead.id;

    GET DIAGNOSTICS count = ROW_COUNT;
    RETURN count;
END;
$$ LANGUAGE plpgsql VOLATILE;