-- SQL migration to add support for storing job logs

-- Type: sqlq.log_level
-- Enumeration of supported log levels.
CREATE TYPE sqlq.log_level AS ENUM ('debug', 'info', 'warn', 'error');

-- Table: sqlq.job_logs
-- Job logs store the log messages emitted by the job handler routine. Writes to this table are often
-- batched together by the runtime (sometimes across different job routines), to make it more performant.
--
-- Use Postgres' Async Commit (https://www.postgresql.org/docs/current/wal-async-commit.html) functionality
-- to further improve write performance.
CREATE TABLE sqlq.job_logs
(
    job       BIGINT,                      -- job this log belongs to
    logged_at TIMESTAMPTZ DEFAULT (now()), -- time at which this message was logged into the database
    level     sqlq.log_level,              -- log level for this message
    message   TEXT,                        -- text content of the log message

    -- Job this log belongs to.
    FOREIGN KEY (job) REFERENCES sqlq.jobs (id) ON DELETE CASCADE
);
