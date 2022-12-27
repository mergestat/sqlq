// Package embed provides an embeddable runtime for sqlq workers.
//
// It enables users to implement a long-living, background service that
// can either be embedded in the same application or packaged as a separate executable.
package embed

import (
	"context"
	"database/sql"
	"github.com/mergestat/sqlq"
	"github.com/pkg/errors"
	"runtime"
	"sync"
	"time"
)

// Handler is the user-provided implementation of the job's business logic.
type Handler interface {

	// Process implements the job's logic to process the given job.
	//
	// It should return nil if the processing of the job is successfully.
	//
	// If Process() returns an error or panics, the job is marked as failed.
	Process(ctx context.Context, job *sqlq.Job) error
}

type HandlerFunc func(ctx context.Context, job *sqlq.Job) error

func (fn HandlerFunc) Process(ctx context.Context, job *sqlq.Job) error { return fn(ctx, job) }

type WorkerConfig struct {
	// Maximum number of concurrent processing of tasks this worker should handle.
	//
	// If set to a zero or negative value, NewWorker will overwrite the value
	// to the number of CPUs usable by the current process.
	Concurrency int

	// List of queues from where the worker will the tasks.
	// User must provide one or more queues to watch.
	Queues []sqlq.Queue
}

// workerState describes the server's running state. Used to prevent running multiple instances.
type workerState int

const (
	// workerStateNew represents a new server. it is the default state
	// in which server starts and then transitions to active status.
	workerStateNew workerState = iota

	// workerStateActive indicates the server is up and active
	workerStateActive

	// workerStateStopped indicates the is up but no longer processing new tasks.
	workerStateStopped

	// workerStateClosed indicates the server has been shutdown.
	workerStateClosed
)

// ErrWorkerClosed indicates that the worker has already been closed
var ErrWorkerClosed = errors.New("sqlq: worker closed")

// Worker is the main service where you'd register handlers for different job types,
// and it'd (at configured time intervals) pull jobs from the queue to execute.
//
// A worker also owns and manages lifecycle of other critical services, like, reaper and janitor.
// It provides an execution environment for a job, and manages critical things such as log shipping, keep-alive etc.
type Worker struct {
	db          *sql.DB            // connection to the underlying database
	handlers    map[string]Handler // collection of all registered handlers
	queues      []sqlq.Queue       // List of queues to pull jobs from
	concurrency int                // configured worker concurrency

	// TODO(@riyaz): find a better pattern to implement service shutdown
	stopProcessor func(time.Duration)

	// server's state describe the current status of the server
	state struct {
		mu    sync.Mutex
		value workerState
	}
}

// NewWorker creates a new instance of worker, configures it using the provided options and return it.
func NewWorker(db *sql.DB, config WorkerConfig) (*Worker, error) {
	var worker = &Worker{db: db, handlers: make(map[string]Handler)}

	if len(config.Queues) == 0 {
		return nil, errors.New("provide list of queues to watch")
	}

	if config.Concurrency <= 0 { // override concurrency if not provided
		config.Concurrency = runtime.NumCPU()
	}

	worker.queues = config.Queues
	worker.concurrency = config.Concurrency

	return worker, nil
}

// Start starts background execution of worker. It schedules all background goroutines and other services to run.
// It starts pulling job from the configured queues and start processing them.
func (worker *Worker) Start() error {
	{ // transition into active state
		worker.state.mu.Lock()
		defer worker.state.mu.Unlock()

		switch worker.state.value {
		case workerStateActive:
			return errors.Errorf("sqlq: worker is already running")
		case workerStateStopped:
			return errors.Errorf("sqlq: worker is stopped and waiting for shutdown")
		case workerStateClosed:
			return ErrWorkerClosed
		}
		worker.state.value = workerStateActive
	}

	worker.stopProcessor = process(worker)
	return nil
}

// Shutdown attempts to gracefully shut the server down, waiting for timeout before forcefully terminating tasks.
// After Shutdown() is called, no new tasks are fetched from the queue to be processed.
func (worker *Worker) Shutdown(timeout time.Duration) error {
	{ // transition into closed state
		worker.state.mu.Lock()

		if worker.state.value == workerStateNew || worker.state.value == workerStateClosed {
			worker.state.mu.Unlock()
			return nil // server isn't running; nothing to do
		}

		worker.state.value = workerStateClosed
		worker.state.mu.Unlock()
	}

	worker.stopProcessor(timeout)
	return nil
}

func (worker *Worker) Register(typeName string, handler Handler) {
	worker.handlers[typeName] = handler
}
