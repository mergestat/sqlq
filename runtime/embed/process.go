package embed

import (
	"context"
	"github.com/mergestat/sqlq"
	"github.com/pkg/errors"
	"time"
)

func process(worker *Worker) (shutdown func(timeout time.Duration)) {
	var err error

	// Channels used to communicate with the processor goroutines below
	//
	// quit is used to unblock the primary goroutine waiting on semaphore
	// abort is used to terminate any handler routine that has not completed even after timeout for shutdown has expired
	var quit, abort = make(chan struct{}), make(chan struct{})
	var sema = make(chan struct{}, worker.concurrency) // use a semaphore to limit worker concurrency

	go func() {
		for {
			select {
			case <-quit:
				return

			// acquire a token to process a job
			case sema <- struct{}{}:
				var dequeued *sqlq.Job

				// TODO(@riyaz): filter jobs by limiting to types implemented by the worker
				if dequeued, err = sqlq.Dequeue(worker.db, worker.queues); err != nil {
					// TODO(@riyaz): should we fail? or log & continue?
					<-sema // release semaphore token
					continue
				}

				if dequeued == nil {
					// sleep for sometime before polling again
					time.Sleep(time.Second) // TODO(@riyaz): maybe this should be made configurable?

					<-sema // release semaphore token
					continue
				}

				// launch another goroutine to supervise the task in background
				// we launch one more goroutine within this where the actual handler is invoked
				// TODO(@riyaz): handle all suppressed error here
				go func(job *sqlq.Job) {
					defer func() { <-sema }() // release semaphore token

					// prepare context for job handler
					jobContext, cancel := context.WithCancel(context.Background()) // TODO(@riyaz): create a custom context with access to runtime services
					defer cancel()

					// check context before starting routine
					select {
					case <-jobContext.Done():
						_ = sqlq.Error(worker.db, job, jobContext.Err())
						return
					default:
					}

					fn, ok := worker.handlers[job.TypeName]
					if !ok {
						// type not supported. this should be removed once we support filtering by type in sqlq.Dequeue()
						_ = sqlq.Error(worker.db, job, errors.Errorf("type(%s) not implemented by the worker", job.TypeName))
						return
					}

					// run user defined handler inside a wrapper to catch any panics which might cause the worker to crash
					var result = make(chan error, 1)
					go func() { result <- panicWrap(func() error { return fn.Process(jobContext, job) }) }()

					select {
					// TODO(@riyaz): to implement job timeouts, add a case here that calls cancel()
					case <-abort:
						// TODO(@riyaz): should this be re-queued? explicitly? or let reaper find it and clean it up?
						_ = sqlq.Error(worker.db, job, errors.New("job aborted"))
					case <-jobContext.Done():
						_ = sqlq.Error(worker.db, job, jobContext.Err())
					case resultError := <-result:
						if resultError != nil {
							_ = sqlq.Error(worker.db, job, resultError)
						} else {
							_ = sqlq.Success(worker.db, job)
						}
					}
				}(dequeued)
			}
		}
	}()

	return func(timeout time.Duration) {
		close(quit) // unblock processor waiting on semaphore

		// forcefully terminate jobs that are still running after timeout
		time.AfterFunc(timeout, func() { close(abort) })

		// block until all routines have released their tokens
		for i := 0; i < cap(sema); i++ {
			sema <- struct{}{}
		}
	}
}

func panicWrap(fn func() error) (err error) {
	defer func() {
		if recoverError := recover(); err == nil {
			err = errors.Errorf("sqlq: recovered from panic: %v", recoverError)
		}
	}()

	return fn()
}
