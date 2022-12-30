package sqlq

import (
	"context"
	"database/sql"

	"github.com/blockloop/scan"
	"github.com/jackc/pgconn"
	"github.com/pkg/errors"
)

// Connection is a utility abstraction over sql connection / pool / transaction objects.
// This is so that the semantics of the operation (should we roll back on error?) are
// defined by the caller of the routine and not decided by the routine itself.
//
// This allows user to run Enqueue() (and other) operations in, say, a common transaction,
// with the rest of the business logic. It ensures that the job is only queued if the business
// logic finishes successfully and commits, else the Enqueue() is also rolled back.
type Connection interface {
	// QueryContext executes a query that returns rows.
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)

	// ExecContext executes a query that doesn't return rows.
	ExecContext(context.Context, string, ...interface{}) (sql.Result, error)
}

// Enqueue enqueues a new job in the given queue based on provided description.
//
// The queue is created if it doesn't already exist. By default, a job is created in 'pending' state.
// It returns the newly created job instance.
func Enqueue(cx Connection, queue Queue, desc *JobDescription) (_ *Job, err error) {
	var ctx = context.Background()

	const upsertQueue = `INSERT INTO sqlq.queues (name) VALUES ($1) ON CONFLICT (name) DO NOTHING`
	if _, err = cx.ExecContext(ctx, upsertQueue, queue); err != nil {
		return nil, errors.Wrap(err, "failed to create queue")
	}

	var rows *sql.Rows
	const createJob = `INSERT INTO sqlq.jobs (queue, typename, parameters, run_after, retention_ttl) VALUES ($1, $2, $3, $4, $5) RETURNING *`
	if rows, err = cx.QueryContext(ctx, createJob, queue, desc.typeName, desc.parameters, desc.runAfter, desc.retentionTTL); err != nil {
		return nil, errors.Wrap(err, "failed to enqueue job")
	}

	var job Job
	if err = scan.RowStrict(&job, rows); err != nil {
		if err == sql.ErrNoRows { // there must be exactly one row of output!
			return nil, errors.Errorf("failed to enqueue job")
		}

		return nil, errors.Wrap(err, "failed to scan enqueued job")
	}

	return &job, nil
}

// DequeueFilters are a set of optional filters that can be used with Dequeue()
type DequeueFilters struct {
	typeName []string
}

func WithTypeName(names []string) func(*DequeueFilters) {
	return func(filters *DequeueFilters) {
		filters.typeName = append(filters.typeName, names...)
	}
}

// Dequeue dequeues a single job from one of the provided queues.
//
// It takes into account several factors such as a queue's concurrency
// and priority settings as well a job's priority. It ensures that by
// executing this job, the queue's concurrency setting would not be violated.
func Dequeue(db *sql.DB, queues []Queue, filterFuncs ...func(*DequeueFilters)) (_ *Job, err error) {
	var ctx = context.Background()
	var tx *sql.Tx

	var filters = &DequeueFilters{}
	for _, fn := range filterFuncs {
		fn(filters)
	}

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
        WHERE job.status = 'pending' 
          AND job.queue = q.name 
          AND (ARRAY_LENGTH($2::text[], 1) IS NULL OR job.typename = ANY($2))
    ORDER BY q.priority DESC, job.priority DESC, job.created_at
    LIMIT 1
)
UPDATE sqlq.jobs 
	SET status = 'running'
FROM dequeued dq
	WHERE jobs.id = dq.id
RETURNING jobs.*
`
	var rows *sql.Rows
	if rows, err = tx.QueryContext(ctx, dequeue, queues, filters.typeName); err != nil {
		_ = tx.Rollback()

		// if it's a serialization error than retry the dequeue operation
		var postgresError *pgconn.PgError
		if errors.As(err, &postgresError) && postgresError.Code == "40001" {
			goto retry
		}

		return nil, errors.Wrap(err, "failed to dequeue task")
	}

	var job Job
	if err = scan.Row(&job, rows); err != nil {
		if err == sql.ErrNoRows { // no jobs in the queues
			_ = tx.Commit() // close the transaction
			return nil, nil
		}

		_ = tx.Rollback() // in any other case we roll back the transaction

		// if it's a serialization error than retry the dequeue operation
		var postgresError *pgconn.PgError
		if errors.As(err, &postgresError) && postgresError.Code == "40001" {
			goto retry
		}

		return nil, errors.Wrap(err, "failed to scan dequeued job")
	}

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

	return &job, nil
}

// Success transitions the job to SUCCESS state and mark it as completed.
func Success(cx Connection, job *Job) (err error) {
	var ctx = context.Background()

	var rows *sql.Rows
	const markCompleted = `UPDATE sqlq.jobs SET status = $3, completed_at = NOW() WHERE id = $1 AND status = $2 RETURNING *`
	if rows, err = cx.QueryContext(ctx, markCompleted, job.ID, job.Status, StateSuccess); err != nil {
		return errors.Wrap(err, "failed to mark job as completed")
	}

	if err = scan.Row(job, rows); err != nil {
		if err == sql.ErrNoRows { // job wasn't in the given state?
			return errors.Wrapf(ErrJobStateMismatch, "expected job to be in %s", job.Status)
		}
		return errors.Wrap(err, "failed to read back values")
	}

	return nil
}

// Error transitions the job to ERROR state and mark it as completed. If the error is retryable (and there are still attempts left),
// we bump the attempt count and transition it to PENDING to be picked up again by a worker.
func Error(cx Connection, job *Job, _ error) (err error) {
	var ctx = context.Background()

	// TODO(@riyaz): implement support for retries

	var rows *sql.Rows
	const markCompleted = `UPDATE sqlq.jobs SET status = $3, completed_at = NOW() WHERE id = $1 AND status = $2 RETURNING *`
	if rows, err = cx.QueryContext(ctx, markCompleted, job.ID, job.Status, StateErrored); err != nil {
		return errors.Wrap(err, "failed to mark job as completed")
	}

	if err = scan.Row(job, rows); err != nil {
		if err == sql.ErrNoRows { // job wasn't in the given state?
			return errors.Wrapf(ErrJobStateMismatch, "expected job to be in %s", job.Status)
		}
		return errors.Wrap(err, "failed to read back values")
	}

	return nil
}
