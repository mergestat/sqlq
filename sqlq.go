package sqlq

import (
	"context"
	"database/sql"
	"github.com/jackc/pgconn"
	"github.com/pkg/errors"
)

// Enqueue enqueues a new job in the given queue based on provided description.
//
// The queue is created if it doesn't already exist. By default, a job is created in 'pending' state.
// It returns the newly created job instance.
func Enqueue(db *sql.DB, queue Queue, desc *JobDescription) (_ *Job, err error) {
	var ctx = context.Background()

	var tx *sql.Tx
	if tx, err = db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelDefault}); err != nil {
		return nil, errors.Wrap(err, "failed to start transaction")
	}
	defer tx.Rollback()

	const upsertQueue = `INSERT INTO sqlq.queues (name) VALUES ($1) ON CONFLICT (name) DO NOTHING`
	if _, err = tx.ExecContext(ctx, upsertQueue, queue); err != nil {
		return nil, errors.Wrap(err, "failed to create queue")
	}

	var rows *sql.Rows
	const createJob = `INSERT INTO sqlq.jobs (queue, typename, parameters, run_after, retention_ttl) VALUES ($1, $2, $3, $4, $5) RETURNING (id)`
	if rows, err = tx.QueryContext(ctx, createJob, queue, desc.typeName, desc.parameters, desc.runAfter, desc.retentionTTL); err != nil {
		return nil, errors.Wrap(err, "failed to enqueue job")
	}
	defer rows.Close()

	if !rows.Next() { // there must be exactly one row of output!
		return nil, errors.Errorf("failed to enqueue job")
	}

	// TODO(@riyaz): scan all the job fields
	var job = &Job{}
	if err = rows.Scan(&job.ID); err != nil {
		return nil, errors.Wrap(err, "failed to scan enqueued job")
	}

	if err = tx.Commit(); err != nil {
		return nil, errors.Wrap(err, "failed to enqueue job")
	}

	return job, nil
}

// Dequeue dequeues a single job from one of the provided queues.
//
// It takes into account several factors such as a queue's concurrency
// and priority settings as well a job's priority. It ensures that by
// executing this job, the queue's concurrency setting would not be violated.
//
// TODO(@riyaz): support filtering by job types as well
func Dequeue(db *sql.DB, queues []Queue) (_ *Job, err error) {
	var ctx = context.Background()
	var tx *sql.Tx

retry:
	if tx, err = db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable}); err != nil {
		return nil, errors.Wrap(err, "failed to start transaction")
	}

	const dequeue = `
WITH queues AS (
    SELECT name, concurrency, priority FROM sqlq.queues WHERE name = ANY ($1)
), running (name, count) AS (
    SELECT queue, COUNT(*) FROM sqlq.jobs, queues  
		WHERE jobs.queue = queues.name AND status = 'running'
	GROUP BY queue
), queue_with_capacity AS (
    SELECT queues.name, queues.priority FROM queues LEFT OUTER JOIN running USING(name)
        WHERE (concurrency IS NULL OR (concurrency - COALESCE(running.count, 0) > 0))
), dequeued(id) AS (
    SELECT job.id FROM sqlq.jobs job, queue_with_capacity q
        WHERE job.status = 'pending' AND job.queue = q.name
    ORDER BY q.priority DESC, job.priority DESC, job.created_at
    LIMIT 1
)
UPDATE sqlq.jobs 
	SET status = 'running'
FROM dequeued dq
	WHERE jobs.id = dq.id
RETURNING jobs.id, jobs.queue, jobs.typename
`
	var rows *sql.Rows
	if rows, err = tx.QueryContext(ctx, dequeue, queues); err != nil {
		_ = tx.Rollback()

		// if it's a serialization error than retry the dequeue operation
		var postgresError *pgconn.PgError
		if errors.As(err, &postgresError) && postgresError.Code == "40001" {
			goto retry
		}

		return nil, errors.Wrap(err, "failed to dequeue task")
	}

	var job *Job = nil
	if has := rows.Next(); has {
		job = new(Job)

		// TODO(@riyaz): scan all the job fields
		if err = rows.Scan(&job.ID, &job.Queue, &job.TypeName); err != nil {
			_, _ = rows.Close(), tx.Rollback()

			// if it's a serialization error than retry the dequeue operation
			var postgresError *pgconn.PgError
			if errors.As(err, &postgresError) && postgresError.Code == "40001" {
				goto retry
			}

			return nil, errors.Wrap(err, "failed to scan dequeued job")
		}
	}

	_ = rows.Close() // we are done with the cursor

	if err = tx.Commit(); err != nil {
		// we might have dequeued a task, but we can still fail to commit
		// if this is a serialization error we retry the dequeue operation
		_ = tx.Rollback()

		var postgresError *pgconn.PgError
		if errors.As(err, &postgresError) && postgresError.Code == "40001" {
			goto retry
		}

		return nil, errors.Wrap(err, "failed to dequeue job")
	}

	return job, nil
}
