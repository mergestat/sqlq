package sqlq

import "context"

// Handler is the user-provided implementation of the job's business logic.
type Handler interface {

	// Process implements the job's logic to process the given job.
	//
	// It should return nil if the processing of the job is successful.
	//
	// If Process() returns an error or panics, the job is marked as failed.
	Process(ctx context.Context, job *Job) error
}

type HandlerFunc func(ctx context.Context, job *Job) error

func (fn HandlerFunc) Process(ctx context.Context, job *Job) error { return fn(ctx, job) }
