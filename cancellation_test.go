package sqlq_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/mergestat/sqlq"
	"github.com/pkg/errors"

	"github.com/mergestat/sqlq/runtime/embed"
)

func TestCancelled(t *testing.T) {
	var err error

	var upstream = MustOpen(PostgresUrl)

	defer upstream.Close()

	job, _ := sqlq.Enqueue(upstream, "embed/cancelled", NewCancelledTask())

	_ = setConcurrencyAndPriority(upstream, "embed/cancelled", 1, 10)

	var worker, _ = embed.NewWorker(upstream, embed.WorkerConfig{
		Queues: []sqlq.Queue{"embed/cancelled"},
	})

	_ = worker.Register("cancelled", cancelledJob())

	if err := worker.Start(); err != nil {
		t.Fatalf("failed to start worker: %v", err)
	}

	time.Sleep(10 * time.Second)

	if _, err := upstream.Exec("UPDATE sqlq.jobs SET status ='cancelling' WHERE id = $1", job.ID); err != nil {
		t.Fatal(err)
	}

	// wait for the routine to execute
	time.Sleep(10 * time.Second)
	var isCancelled bool

	if isCancelled, err = sqlq.IsCancelled(upstream, job); err != nil {
		t.Fatal(err)
	}

	if !isCancelled {
		t.Fail()
	}

	if err := worker.Shutdown(5 * time.Second); err != nil {
		t.Fatalf("failed to shutdown worker: %v", err)
	}
}

func NewCancelledTask() *sqlq.JobDescription {
	return sqlq.NewJobDesc("cancelled")
}

func cancelledJob() sqlq.HandlerFunc {
	return func(ctx context.Context, job *sqlq.Job) error {
		time.Sleep(10 * time.Second)
		return nil
	}
}

func Waiter(wg *sync.WaitGroup) sqlq.HandlerFunc {
	return func(ctx context.Context, job *sqlq.Job) error {
		select {
		case <-time.After(10 * time.Second):
			wg.Done()
			return errors.New("test")
		}
	}
}

func TestLeak(t *testing.T) {
	var upstream = MustOpen(PostgresUrl)
	defer upstream.Close()

	//upstream.SetMaxOpenConns(2)
	t.Logf("stats: %+v", upstream.Stats())
	var wg sync.WaitGroup

	for i := 0; i < 20; i++ {
		wg.Add(1)
		_, _ = sqlq.Enqueue(upstream, "embed/leak", sqlq.NewJobDesc("wait"))
	}

	var worker, _ = embed.NewWorker(upstream, embed.WorkerConfig{Queues: []sqlq.Queue{"embed/leak"}, Concurrency: 4})
	_ = worker.Register("wait", Waiter(&wg))

	if err := worker.Start(); err != nil {
		t.Fatalf("failed to start worker: %v", err)
	}

	// wait for the routine to execute
	t.Logf("stats: %+v", upstream.Stats())
	wg.Wait()

	if err := worker.Shutdown(10 * time.Second); err != nil {
		t.Fatalf("failed to shutdown worker: %v", err)
	}

	t.Logf("stats: %+v", upstream.Stats())
}
