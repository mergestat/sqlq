package sqlq

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"math/rand"
	"strings"
	"time"

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

var (
	// ErrSkipRetry must be used by the job handler routine to signal
	// that the job must not be retried (even when there are attempts left)
	ErrSkipRetry = errors.New("sqlq: skip retry")
)

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

	var i = 0
	var columns, placeholders, args = make([]string, 0, 6), make([]string, 0, 6), make([]interface{}, 0, 6)
	var add = func(col string, arg interface{}) {
		columns, placeholders, args, i = append(columns, col), append(placeholders, fmt.Sprintf("$%d", i+1)), append(args, arg), i+1
	}

	add("queue", queue)
	add("typename", desc.typeName)
	add("parameters", desc.parameters)

	if desc.maxRetries != nil {
		add("max_retries", desc.maxRetries)
	}

	if desc.runAfter != nil {
		add("run_after", desc.runAfter)
	}

	if desc.retentionTTL != nil {
		add("retention_ttl", desc.retentionTTL)
	}

	if desc.keepAlive != nil {
		add("keepalive_interval", desc.keepAlive)
	}

	var rows *sql.Rows
	var createJob = `INSERT INTO sqlq.jobs (` + strings.Join(columns, ",") + `) VALUES (` + strings.Join(placeholders, ",") + `) RETURNING *`
	if rows, err = cx.QueryContext(ctx, createJob, args...); err != nil {
		return nil, errors.Wrap(err, "failed to enqueue job")
	}

	var job Job
	if err = scanJob(rows, &job); err != nil {
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

	var rows *sql.Rows
	const dequeue = `SELECT * FROM sqlq.dequeue_job($1, $2)`
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
	if err = scanJob(rows, &job); err != nil {
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

// Cancelled transitions the job to CANCELLED state and mark it as completed
func Cancelled(cx Connection, job *Job) (err error) {
	var ctx = context.Background()
	var rows *sql.Rows

	const cancelJob = `
	UPDATE sqlq.jobs
		SET status = 'cancelled',
		completed_at = NOW()
	WHERE id =$1 AND status = 'cancelling'
	RETURNING *`

	if rows, err = cx.QueryContext(ctx, cancelJob, job.ID); err != nil {
		return errors.Wrapf(err, "failed to executed cancel operation")
	}

	if err = scanJob(rows, job); err != nil {
		if err == sql.ErrNoRows { // job wasn't in the given state?
			return errors.Wrapf(ErrJobStateMismatch, "expected job to be in %s", job.Status)
		}
		return errors.Wrap(err, "failed to read back values")
	}

	return nil
}

// IsCancelled checks the current job state searching for a cancelling
// state, if so returns true
func IsCancelled(cx Connection, job *Job) (b bool, err error) {
	var ctx = context.Background()
	var res *sql.Rows
	var result bool

	const isCancelled = `SELECT * FROM sqlq.check_job_status($1, $2)`
	if res, err = cx.QueryContext(ctx, isCancelled, job.ID, "cancelling"); err != nil {
		return false, errors.Wrapf(err, "failed to checking job status")
	}

	if res.Next() {
		if err = res.Scan(&result); err != nil {
			return false, errors.Wrapf(err, "failed to scan")
		}
	}

	return result, nil
}

// Success transitions the job to SUCCESS state and mark it as completed.
func Success(cx Connection, job *Job) (err error) {
	var ctx = context.Background()

	var rows *sql.Rows
	const markCompleted = `SELECT * FROM sqlq.mark_success($1, $2)`
	if rows, err = cx.QueryContext(ctx, markCompleted, job.ID, job.Status); err != nil {
		return errors.Wrap(err, "failed to mark job as completed")
	}

	if err = scanJob(rows, job); err != nil {
		if err == sql.ErrNoRows { // job wasn't in the given state?
			return errors.Wrapf(ErrJobStateMismatch, "expected job to be in %s", job.Status)
		}
		return errors.Wrap(err, "failed to read back values")
	}

	return nil
}

// Error transitions the job to ERROR state and mark it as completed. If the error is retryable (and there are still attempts left),
// we bump the attempt count and transition it to PENDING to be picked up again by a worker.
func Error(cx Connection, job *Job, userError error) (err error) {
	var ctx = context.Background()

	var retry = job.Attempt < job.MaxRetries && !errors.Is(userError, ErrSkipRetry)
	var runAfter = time.Duration(int(math.Pow(float64(job.Attempt), 4))+10+rand.Intn(30)*(job.Attempt+1)) * time.Second

	var rows *sql.Rows
	const updateState = `SELECT * FROM sqlq.mark_failed($1, $2, $3, $4)`
	if rows, err = cx.QueryContext(ctx, updateState, job.ID, job.Status, retry, runAfter); err != nil {
		return errors.Wrap(err, "failed to mark job as completed")
	}

	if err = scanJob(rows, job); err != nil {
		if err == sql.ErrNoRows { // job wasn't in the given state?
			return errors.Wrapf(ErrJobStateMismatch, "expected job to be in %s", job.Status)
		}
		return errors.Wrap(err, "failed to read back values")
	}

	return nil
}

// Reap reaps any zombie process, processes where state is 'running' but the job hasn't pinged in a while, in the given queues.
// It moves any job with remaining attempts back to the queue while dumping all others in to the errored state.
func Reap(cx Connection, queues []Queue) (n int64, err error) {
	var ctx = context.Background()

	var rows *sql.Rows
	const deathReaper = `SELECT * FROM sqlq.reap($1)`
	if rows, err = cx.QueryContext(ctx, deathReaper, queues); err != nil {
		return 0, errors.Wrapf(err, "failed to reap zombie processes")
	}
	defer rows.Close()

	if rows.Next() {
		if err = rows.Scan(&n); err != nil {
			return 0, errors.Wrapf(err, "failed to reap zombie processes")
		}
	}

	return n, rows.Err()
}
