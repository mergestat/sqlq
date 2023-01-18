-- SQL migration to add support job keep-alive

-- Column keepalive_interval is the interval in seconds at which the worker
-- is expected to send the pings. Defaults to 30 seconds.
ALTER TABLE sqlq.jobs
    ADD COLUMN keepalive_interval BIGINT NOT NULL DEFAULT (30 * 1e9);

-- Column last_keepalive stores the last ping's time
ALTER TABLE sqlq.jobs
    ADD COLUMN last_keepalive TIMESTAMPTZ;
