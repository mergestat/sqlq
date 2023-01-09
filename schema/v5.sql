-- SQL migration to add indexes where appropriate

-- Index: ix_jobs_queue_type_status
--
-- Index to assist most usual query types involving a queue, a job's type and its current status.
-- The index is verified as being used by postgres while executing the dequeue query.
CREATE INDEX ix_jobs_queue_type_status ON sqlq.jobs (queue, typename, status);

-- Index: ix_logs_job
-- Index on job_logs.job to assist in fetching logs for a given job.
CREATE INDEX ix_logs_job ON sqlq.job_logs (job);
