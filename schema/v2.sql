-- Migration to add support for retries

ALTER TABLE sqlq.jobs
    ADD COLUMN max_retries INT DEFAULT (1); -- max number of attempts before considering a job as failed

ALTER TABLE sqlq.jobs
    ADD COLUMN attempt INT DEFAULT (0); -- current attempt of the job

ALTER TABLE sqlq.jobs
    ADD COLUMN last_queued_at TIMESTAMPTZ NOT NULL DEFAULT (now()); -- when the job was last enqueued (including retries)
