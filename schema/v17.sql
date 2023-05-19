-- SQL migration to add index to job_logs to improve performance when looking for jobs with a certail level

CREATE INDEX IF NOT EXISTS ix_job_logs_job_level ON sqlq.job_logs (job, level);
