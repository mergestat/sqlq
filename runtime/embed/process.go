package embed

import (
	"context"
	"github.com/mergestat/sqlq"
	"github.com/pkg/errors"
	logger "log"
	"os"
	"time"
)

func process(worker *Worker) (shutdown func(timeout time.Duration)) {
	// TODO(@riyaz): replace with user-supplied logger
	var log = logger.New(os.Stderr, "sqlq: ", logger.LstdFlags)
	var err error

	// Channels used to communicate with the processor goroutines below
	//
	// quit is used to unblock the primary goroutine waiting on semaphore
	// abort is used to terminate any handler routine that has not completed even after timeout for shutdown has expired
	var quit, abort = make(chan struct{}), make(chan struct{})
	var sema = make(chan struct{}, worker.concurrency) // use a semaphore to limit worker concurrency

	// prepare list of supported job types
	var supportedTypes = make([]string, 0, len(worker.handlers))
	for name := range worker.handlers {
		supportedTypes = append(supportedTypes, name)
	}

	go func() {
		for {
			select {
			case <-quit:
				log.Printf("received signal to terminate")
				return

			// acquire a token to process a job
			case sema <- struct{}{}:
				var dequeued *sqlq.Job

				// TODO(@riyaz): filter jobs by limiting to types implemented by the worker
				if dequeued, err = sqlq.Dequeue(worker.db, worker.queues, sqlq.WithTypeName(supportedTypes)); err != nil {
					log.Printf("failed to dequeue job: %#v", err)
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
				go func(job *sqlq.Job) {
					defer func() { <-sema }() // release semaphore token

					// prepare context for job handler
					jobContext, cancel := context.WithCancel(context.Background()) // TODO(@riyaz): create a custom context with access to runtime services
					defer cancel()

					// check context before starting routine
					select {
					case <-jobContext.Done():
						if err = sqlq.Error(worker.db, job, jobContext.Err()); err != nil {
							log.Printf("failed to mark job as errored: %#v", err)
						}
						return
					default:
					}

					fn, ok := worker.handlers[job.TypeName]
					if !ok {
						// THIS SHOULD NOT HAPPEN UNDER ANY CIRCUMSTANCE.
						// Best way deemed to handle this is to crash fast and loud.
						panic(errors.Errorf("sqlq: type (%q) not supported but got picked", job.TypeName))
					}

					// run user defined handler inside a wrapper to catch any panics which might cause the worker to crash
					var result = make(chan error, 1)
					go func() {
						job = sqlq.AttachResultWriter(worker.db, job)
						result <- panicWrap(func() error { return fn.Process(jobContext, job) })
					}()

					select {
					// TODO(@riyaz): to implement job timeouts, add a case here that calls cancel()
					case <-abort:
						// TODO(@riyaz): should this be re-queued? explicitly? or let reaper find it and clean it up?
						if err = sqlq.Error(worker.db, job, errors.New("job aborted")); err != nil {
							log.Printf("failed to mark job as errored: %#v", err)
						}
					case <-jobContext.Done():
						if err = sqlq.Error(worker.db, job, jobContext.Err()); err != nil {
							log.Printf("failed to mark job as errored: %#v", err)
						}
					case resultError := <-result:
						if resultError != nil {
							if err = sqlq.Error(worker.db, job, resultError); err != nil {
								log.Printf("failed to mark job as errored: %#v", err)
							}
						} else {
							if err = sqlq.Success(worker.db, job); err != nil {
								log.Printf("failed to mark job as success: %#v", err)
							}
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
		if recoverError := recover(); recoverError != nil && err == nil {
			err = errors.Errorf("sqlq: recovered from panic: %v", recoverError)
		}
	}()

	return fn()
}
