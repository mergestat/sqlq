package embed

import (
	"context"
	stdlog "log"
	"os"
	"sync"
	"time"

	"github.com/mergestat/sqlq"
	"github.com/pkg/errors"
)

func process(worker *Worker, loggingBackend sqlq.LogBackend) (shutdown func(timeout time.Duration) error) {
	// TODO(@riyaz): replace with user-supplied logger
	var log = stdlog.New(os.Stderr, "sqlq: ", stdlog.LstdFlags)
	var err error

	// Channels used to communicate with the processor goroutines below
	//
	// quit is used to unblock the primary goroutine waiting on semaphore
	// abort is used to terminate any handler routine that has not completed even after timeout for shutdown has expired
	var quit, abort = make(chan struct{}), make(chan struct{})
	var sema = make(chan struct{}, worker.concurrency) // use a semaphore to limit worker concurrency

	//var cancels = make(map[int]context.CancelFunc) // collection of context.CancelFunc for all active jobs
	var cancels sync.Map

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
					// cancelled is used to terminate the job routine that have a status equal to cancelling
					var cancelled = make(chan struct{})

					defer func() { <-sema }() // release semaphore token

					// prepare context for job handler
					jobContext, cancel := context.WithCancel(context.Background())

					cancels.Store(job.ID, cancel)
					defer func() { cancel(); cancels.Delete(job.ID) }()

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

					// we run a polling checking every 3 secongs whether the job have a
					// cancelling status if so we change it's status to cancelled.
					go func() {
						ticker := time.NewTicker(3 * time.Second)
						for {
							select {
							case <-jobContext.Done():
								ticker.Stop()
								return
							case <-ticker.C:
								var isCancelled bool
								if isCancelled, err = sqlq.IsCancelled(worker.db, job); err != nil {
									log.Printf("failed to : %#v", err)
								}

								if isCancelled {
									close(cancelled)
								}
							}
						}
					}()

					// run user defined handler inside a wrapper to catch any panics which might cause the worker to crash
					var result = make(chan error, 1)
					go func() {
						job = sqlq.AttachResultWriter(worker.db, job)
						job = sqlq.AttachLogger(loggingBackend, job)
						job = sqlq.AttachPinger(worker.db, job)

						result <- panicWrap(func() error { return fn.Process(jobContext, job) })
					}()

					select {
					// TODO(@riyaz): to implement job timeouts, add a case here that calls cancel()
					case <-abort:
						// TODO(@riyaz): should this be re-queued? explicitly? or let reaper find it and clean it up?
						if err = sqlq.Error(worker.db, job, errors.New("job aborted")); err != nil {
							log.Printf("failed to mark job as errored: %#v", err)
						}

					case <-cancelled:
						if err = sqlq.Cancelled(worker.db, job); err != nil {
							log.Printf("failed to mark job as cancelled: %#v", err)
						}
						// We need to cancel the context after marking the job as cancelled, otherwise it would
						// cause the user's routine to return an error which would then mark the job as errored rather than cancelled
						cancel()

					// case <-jobContext.Done():
					// 		This case is removed as handling context cancellation is a responsibility of the user-defined routine.
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

	return func(timeout time.Duration) error {
		close(quit) // unblock processor waiting on semaphore

		// forcefully terminate jobs that are still running after timeout
		var timer = time.AfterFunc(timeout, func() { close(abort) })

		// send a graceful shutdown signal to all active jobs by cancelling their contexts.
		// this provides an opportunity to the user routine to wrap up whatever it is doing and exit.
		cancels.Range(func(_, fn interface{}) bool { fn.(context.CancelFunc)(); return true })

		// block until all routines have released their tokens
		for i := 0; i < cap(sema); i++ {
			sema <- struct{}{}
		}

		timer.Stop() // everyone has finished, cancel the forceful abort operation
		return nil
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
