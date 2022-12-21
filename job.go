package sqlq

import "time"

type Queue string

type JobState string

const (
	StatePending = JobState("pending")
	StateRunning = JobState("running")
	StateSuccess = JobState("success")
	StateErrored = JobState("errored")
)

// JobDescription describes a job to be enqueued. Note that it is just a set of options (that closely resembles) for a job,
// and not an actual instance of a job. It is used by Enqueue() to create a new Job.
type JobDescription struct {
	typeName     string
	parameters   []byte
	runAfter     time.Duration
	retentionTTL time.Duration
}

func NewJobDesc(typeName string, opts ...func(*JobDescription)) *JobDescription {
	var jd = &JobDescription{typeName: typeName}
	for _, fn := range opts {
		fn(jd)
	}
	return jd
}

func WithParameters(params []byte) func(*JobDescription) {
	return func(desc *JobDescription) { desc.parameters = params }
}

func WithRetention(dur time.Duration) func(*JobDescription) {
	return func(desc *JobDescription) { desc.retentionTTL = dur }
}

type Job struct {
	ID       int
	Queue    Queue
	TypeName string
	Priority int
	Status   JobState

	Parameters []byte
	Result     []byte

	CreatedAt   time.Time
	StartedAt   time.Time
	CompletedAt time.Time

	RunAfter     time.Duration
	RetentionTTL time.Duration
}
