-- Migration to setup core relations and schema for sqlq

-- Everything we own is created under "sqlq" schema.
CREATE SCHEMA IF NOT EXISTS sqlq;

-- Table: sqlq.queues
-- Queue is a grouping primitive in sqlq. All jobs that are enqueued belong to
-- a queue. Jobs are dequeued from a queue based on priority and concurrency settings of a queue.
CREATE TABLE sqlq.queues
(
    name        TEXT PRIMARY KEY NOT NULL,             -- queues are identified by their names which (obviously) must be unique within a system
    description TEXT,                                  -- short human-readable description of the queue; for use by any human interface
    concurrency INT,                                   -- how many concurrent executions a queue support across all workers; -1 to disable
    priority    INT              NOT NULL DEFAULT (1), -- priority amongst queue; used to differentiate between, say, critical & non-critical queues
    created_at  TIMESTAMPTZ      NOT NULL DEFAULT (now())
);

-- Table: sqlq.job_states
-- Jobs states is an enumeration of all possible states a job can be in.
-- It is used to implement a job's state machine.
CREATE TABLE sqlq.job_states
(
    name        TEXT PRIMARY KEY,
    description TEXT
);

-- Table: sqlq.jobs
-- Jobs represent a job / task to execute in background, asynchronously. Jobs are enqueued to a queue
-- and are picked by a worker (that can handle the defined typename) based on the priority of the job.
--
-- Each job has a defined state machine and the runtime transitions a job through different states.
-- A job can also take in a (optional) json input, and can (optionally) emit output as json.
-- A job's state and input / output are retained until it's retention period after which the runtime clears it (hard delete!).
--
-- TODO(@riyaz): implement support for keep-alive pings from the worker
-- TODO(@riyaz): implement support for job timeout
CREATE TABLE sqlq.jobs
(
    id            BIGSERIAL PRIMARY KEY,                        -- unique identifier for the job in the system
    queue         TEXT            NOT NULL,                     -- the queue this job belongs to
    typename      TEXT            NOT NULL,                     -- typename of the job registered with the runtime
    status        TEXT            NOT NULL DEFAULT ('pending'), -- state of a job in its state machine
    priority      INT             NOT NULL DEFAULT (1),         -- the job's priority within the queue

    -- job's input and output
    parameters    JSON            NULL,
    result        JSON            NULL,

    created_at    TIMESTAMPTZ     NOT NULL DEFAULT (now()),     -- when the job was first enqueued (not including retries)
    started_at    TIMESTAMPTZ,                                  -- when the job last transitioned from PENDING -> RUNNING
    completed_at  TIMESTAMPTZ,                                  -- when the job transitioned into a completion state (SUCCESS / ERROR)

    -- Seconds after which the task must be executed (regardless of available capacity)
    -- This is used to implement "future execution" of task and exponential back-off retry policies.
    run_after     INTERVAL SECOND          DEFAULT ('0'::interval),

    -- TODO(@riyaz): add support for retries
    --
    -- max_retries   INT                   DEFAULT (1),
    -- attempt       INT                   DEFAULT (0),

    -- Seconds to retain the output of the job after it has completed before its cleared.
    -- A jobs is cleared by the runtime as soon as NOW() > (completed_at + retention_ttl) WHERE completed_at IS NOT NULL
    retention_ttl INTERVAL SECOND NOT NULL DEFAULT ('0'::interval),

    -- Queue references sqlq.queue entry.
    -- We have an ON DELETE CASCADE to remove a job when the queue is deleted.
    --
    -- @riyaz: not much useful since we have soft-delete but still good to have
    FOREIGN KEY (queue) REFERENCES sqlq.queues (name) ON DELETE CASCADE,

    -- Status references on of the statuses defined in sqlq.job_states.
    FOREIGN KEY (status) REFERENCES sqlq.job_states (name)
);

-- seed sqlq.job_states with the default states
INSERT INTO sqlq.job_states (name, description)
VALUES ('pending', 'The job is enqueued and is ready to be picked by a worker'),
       ('running', 'The job is picked by a worker and is executing'),
       ('success', 'The job has successfully completed execution'),
       ('errored', 'The job execution has failed');