
CREATE EXTENSION IF NOT EXISTS pgcrypto;

BEGIN;
ALTER TABLE sqlq.job_logs
DROP COLUMN job;

ALTER TABLE sqlq.jobs
DROP COLUMN id;

ALTER TABLE sqlq.jobs
ADD COLUMN id uuid PRIMARY KEY DEFAULT public.gen_random_uuid() NOT NULL;

ALTER TABLE sqlq.job_logs
ADD COLUMN job  uuid DEFAULT public.gen_random_uuid() NOT NULL;

AlTER TABLE sqlq.job_logs
ADD CONSTRAINT job FOREIGN KEY (job) REFERENCES sqlq.jobs (id) ON DELETE CASCADE;

COMMIT;