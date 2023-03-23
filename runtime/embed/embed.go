// Package embed provides an embeddable runtime for sqlq workers.
//
// It enables users to implement a long-living, background service that
// can either be embedded in the same application or packaged as a separate executable.
package embed

import (
	"database/sql"
	"runtime"
	"sync"
	"time"

	"github.com/mergestat/sqlq"
	"github.com/pkg/errors"
)

type WorkerConfig struct {
	// Maximum number of concurrent processing of tasks this worker should handle.
	//
	// If set to a zero or negative value, NewWorker will overwrite the value
	// to the number of CPUs usable by the current process.
	Concurrency int

	// List of queues from where the worker will retrieve jobs.
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

	// workerStateClosed indicates the server has been shutdown.
	workerStateClosed
)

// ErrWorkerClosed indicates that the worker has already been closed
var ErrWorkerClosed = errors.New("sqlq: worker closed")

// ErrWorkerRunning indicated that the worker has already started running, and no further changes can be made
var ErrWorkerRunning = errors.New("sqlq: worker running")

// Worker is the main service where you'd register handlers for different job types,
// and it'd (at configured time intervals) pull jobs from the queue to execute.
//
// A worker also owns and manages lifecycle of other critical services, like, reaper and janitor.
// It provides an execution environment for a job, and manages critical things such as log shipping, keep-alive etc.
type Worker struct {
	db          *sql.DB                 // connection to the underlying database
	handlers    map[string]sqlq.Handler // collection of all registered handlers
	queues      []sqlq.Queue            // List of queues to pull jobs from
	concurrency int                     // configured worker concurrency

	// TODO(@riyaz): find a better pattern to implement service shutdown
	stopProcessor func(time.Duration) error
	stopLogger    func() error
	stopReaper    func() error

	// server's state describe the current status of the server
	state struct {
		mu    sync.Mutex
		value workerState
	}
}

// NewWorker creates a new instance of worker, configures it using the provided options and return it.
func NewWorker(db *sql.DB, config WorkerConfig) (*Worker, error) {
	var worker = &Worker{db: db, handlers: make(map[string]sqlq.Handler)}

	if config.Concurrency <= 0 { // override concurrency if not provided
		config.Concurrency = runtime.NumCPU()
	}

	worker.queues = config.Queues
	worker.concurrency = config.Concurrency

	return worker, nil
}

// Start starts background execution of worker. It schedules all background goroutines and other services to run.
// It starts pulling job from the configured queues and starts processing them.
func (worker *Worker) Start() error {
	{ // transition into active state
		worker.state.mu.Lock()
		defer worker.state.mu.Unlock()

		switch worker.state.value {
		case workerStateActive:
			return ErrWorkerRunning
		case workerStateClosed:
			return ErrWorkerClosed
		}
		worker.state.value = workerStateActive
	}

	// create a new, shared logging backend
	var loggingBackend, stopLogger = logger(worker.db)
	worker.stopLogger = stopLogger

	worker.stopReaper = reaper(worker)
	worker.stopProcessor = process(worker, loggingBackend)
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

	_ = worker.stopProcessor(timeout)
	_ = worker.stopLogger()
	_ = worker.stopReaper()
	return nil
}

// Register registers the given function as the handler for given job type.
func (worker *Worker) Register(typeName string, handler sqlq.Handler) error {
	worker.state.mu.Lock()
	defer worker.state.mu.Unlock()

	if worker.state.value != workerStateNew {
		if worker.state.value == workerStateClosed {
			return ErrWorkerClosed
		}
		return errors.Wrap(ErrWorkerRunning, "cannot register new job type")
	}

	worker.handlers[typeName] = handler
	return nil
}
