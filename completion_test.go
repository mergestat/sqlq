package sqlq_test

import (
	. "github.com/mergestat/sqlq"
	"github.com/pkg/errors"
	"math/rand"
	"testing"
	"time"
)

func TestSuccess(t *testing.T) {
	var err error

	var upstream = MustOpen(PostgresUrl)
	defer upstream.Close()

	if _, err = Enqueue(upstream, "test/success", NewAdditionJob(rand.Int(), rand.Int())); err != nil {
		t.Fatal(err)
	}

	var job *Job
	if job, err = Dequeue(upstream, []Queue{"test/success"}); err != nil {
		t.Fatal(err)
	}

	if job.Status != StateRunning {
		t.Fatalf("dequeued job must be in running state")
	}

	if err = Success(upstream, job); err != nil {
		t.Fatalf("failed to mark job as success: %v", err)
	}

	if job.Status != StateSuccess {
		t.Fatalf("completed job must be in StateSuccess; got=%s", job.Status)
	}
}

func TestError(t *testing.T) {
	var err error

	var upstream = MustOpen(PostgresUrl)
	defer upstream.Close()

	if _, err = Enqueue(upstream, "test/error", NewAdditionJob(rand.Int(), rand.Int())); err != nil {
		t.Fatal(err)
	}

	var job *Job
	if job, err = Dequeue(upstream, []Queue{"test/error"}); err != nil {
		t.Fatal(err)
	}

	if job.Status != StateRunning {
		t.Fatalf("dequeued job must be in running state")
	}

	if err = Error(upstream, job, errors.New("test")); err != nil {
		t.Fatalf("failed to mark job as failed: %v", err)
	}

	if job.Status != StateErrored {
		t.Fatalf("completed job must be in StateErrored; got=%s", job.Status)
	}
}

func TestRetry(t *testing.T) {
	var err error

	var upstream = MustOpen(PostgresUrl)
	defer upstream.Close()

	if _, err = Enqueue(upstream, "test/retry", NewJobDesc("retryable", WithMaxRetries(2))); err != nil {
		t.Fatal(err)
	}

	var job *Job
	if job, err = Dequeue(upstream, []Queue{"test/retry"}); err != nil {
		t.Fatal(err)
	}
	lastId := job.ID

	if job.Status != StateRunning {
		t.Fatalf("dequeued job must be in running state")
	}

	if job.TypeName != "retryable" {
		t.Fatalf("dequeued job must be of type %q", "retryable")
	}

	if err = Error(upstream, job, errors.New("test")); err != nil {
		t.Fatalf("failed to mark job as failed: %v", err)
	}

	if job.Status != StatePending {
		t.Fatalf("job must be in StatePending; got=%s", job.Status)
	}

	time.Sleep(job.RunAfter)

	if job, err = Dequeue(upstream, []Queue{"test/retry"}); err != nil {
		t.Fatal(err)
	} else if job == nil {
		t.Fatalf("got no job back!")
	}

	if job.ID != lastId {
		t.Fatalf("got a different job!")
	}
}
