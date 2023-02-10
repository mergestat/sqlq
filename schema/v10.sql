-- Migration to alter the type of sqlq.jobs parameters and
-- result.
BEGIN;
ALTER TABLE sqlq.jobs
ALTER COLUMN parameters TYPE jsonb;

ALTER TABLE sqlq.jobs
ALTER COLUMN result TYPE jsonb;

COMMIT;
