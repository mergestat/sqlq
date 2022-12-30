package sqlq

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"io"
	"time"

	"github.com/pkg/errors"
)

var (
	// ErrJobStateMismatch is returned if an optimistic locking failure occurs when transitioning
	// a job between states. It usually indicates that some other has updated the state of the job
	// after we have read it from the store.
	ErrJobStateMismatch = errors.New("job state mismatch")
)

// Queue represents a named group / queue where jobs can be pushed / enqueued.
type Queue string

// ensure
//go:generate stringer -type=JobState -linecomment -trimprefix Job -output job_state.go

// JobState represents the status a job is in and serves as the basis of a job's state machine.
//
//	                   +-----------+
//	Enqueue()----------|  PENDING  |----------------+
//	                   +-----------+                |
//	                         |                      |
//	                         |                      |
//	                   +-----|-----+            +---|----+
//	                   |  RUNNING  |------------| Retry? |
//	                   +-----------+            +--------+
//	                         |                      |
//	                         |                      |
//	                         |                      |
//	                   +-----|-----+            +---|-----+
//	                   |  SUCCESS  |            | ERRORED |
//	                   +-----------+            +---------+
//
// A job starts in the PENDING state when it is Enqueue()'d. A worker would than pick it up and transition it to RUNNING.
// If the job finishes without any error, it is moved to SUCCESS. If an error occurs and runtime determines that the
// job can be retried, it will move it back to the PENDING state, else it will move it to the ERRORED state.
type JobState uint

func (i JobState) Value() (driver.Value, error) { return i.String(), nil }

func (i *JobState) Scan(src interface{}) error {
	if val, ok := src.(string); !ok {
		return errors.New("jobs state must be a string")
	} else {
		switch val {
		case "pending":
			*i = StatePending
		case "running":
			*i = StateRunning
		case "success":
			*i = StateSuccess
		case "errored":
			*i = StateErrored
		default:
			*i = StateInvalid
		}
	}
	return nil
}

const (
	StateInvalid JobState = iota // invalid
	StatePending                 // pending
	StateRunning                 // running
	StateSuccess                 // success
	StateErrored                 // errored
)

// JobDescription describes a job to be enqueued. Note that it is just a set of options (that closely resembles) for a job,
// and not an actual instance of a job. It is used by Enqueue() to create a new Job.
type JobDescription struct {
	typeName     string
	parameters   []byte
	runAfter     time.Duration
	retentionTTL time.Duration
}

// NewJobDesc creates a new JobDescription for a job with given typename and with the provided opts.
func NewJobDesc(typeName string, opts ...func(*JobDescription)) *JobDescription {
	var jd = &JobDescription{typeName: typeName}
	for _, fn := range opts {
		fn(jd)
	}
	return jd
}

// WithParameters is used to pass additional parameters / arguments to the job. Note that params must be a JSON-encoded value.
func WithParameters(params []byte) func(*JobDescription) {
	return func(desc *JobDescription) { desc.parameters = params }
}

// WithRetention sets the retention policy for a job. A completed job will be cleaned up after its retention policy expires.
func WithRetention(dur time.Duration) func(*JobDescription) {
	return func(desc *JobDescription) { desc.retentionTTL = dur }
}

// Job represents an instance of a task / job in a queue.
type Job struct {
	ID       int      `db:"id"`
	Queue    Queue    `db:"queue"`
	TypeName string   `db:"typename"`
	Priority int      `db:"priority"`
	Status   JobState `db:"status"`

	Parameters []byte `db:"parameters"`
	Result     []byte `db:"result"`

	CreatedAt   time.Time    `db:"created_at"`
	StartedAt   sql.NullTime `db:"started_at"`
	CompletedAt sql.NullTime `db:"completed_at"`

	RunAfter     time.Duration `db:"run_after"`
	RetentionTTL time.Duration `db:"retention_ttl"`

	// reference to runtime services; might not be available all the time
	resultWriter *resultWriter
}

type resultWriter struct {
	cx  Connection
	job *Job

	buf bytes.Buffer // to store intermediate writes
}

func (r *resultWriter) Write(p []byte) (n int, err error) { return r.buf.Write(p) }

func (r *resultWriter) Close() error {
	const storeResult = `UPDATE sqlq.jobs SET result = $2 WHERE id = $1 RETURNING result`
	rows, err := r.cx.QueryContext(context.Background(), storeResult, r.job.ID, r.buf.Bytes())
	if err != nil {
		return errors.Wrapf(err, "failed to save job result")
	}
	defer func() { _ = rows.Close() }()

	if rows.Next() {
		if err = rows.Scan(&r.job.Result); err != nil {
			return errors.Wrapf(err, "failed to save job result")
		}
	}
	return nil
}

// ResultWriter returns an io.WriteCloser that can be used to save the result of a job.
// User must close the writer to actually save the data. A result writer is only available
// when running in the context of a runtime. Trying to call ResultWriter() in any other context
// would cause a panic().
func (job *Job) ResultWriter() io.WriteCloser {
	if job.resultWriter == nil {
		// user should not be using ResultWriter() outside a runtime context
		panic("sqlq: result writer not set for the job")
	}
	return job.resultWriter
}

// AttachResultWriter attaches a result writer to the given job,
// using the provided Connection as the backend to write to.
func AttachResultWriter(cx Connection, job *Job) *Job {
	job.resultWriter = &resultWriter{cx: cx, job: job}
	return job
}
