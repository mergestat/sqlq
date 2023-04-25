-- SQL migration to change default job priority

-- current default is set to 1; change it to 10 since we've also changed the meaning of order (lower number equals higher priority)
ALTER TABLE sqlq.jobs ALTER COLUMN priority DROP DEFAULT;
ALTER TABLE sqlq.jobs ALTER COLUMN priority SET DEFAULT 10;
