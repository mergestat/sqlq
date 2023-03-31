-- SQL migration to create a 16-bit, cycling sequence allocator to define insert order between sqlq.job_log entries

-- A 16-bit smallint sequence that cycles.
CREATE SEQUENCE sqlq.job_log_ordering AS smallint CYCLE;

-- Add new position column in the logs table.
ALTER TABLE sqlq.job_logs ADD COLUMN position SMALLINT NOT NULL DEFAULT nextval('sqlq.job_log_ordering');
