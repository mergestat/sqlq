package embed_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"github.com/mergestat/sqlq"
	"github.com/mergestat/sqlq/runtime/embed"
	"github.com/pkg/errors"
	"sync"
	"testing"
	"time"
)

func WaitGroupNotifier(wg *sync.WaitGroup) sqlq.HandlerFunc {
	return func(_ context.Context, _ *sqlq.Job) error { wg.Done(); return nil }
}

func NewNotifyTask() *sqlq.JobDescription {
	return sqlq.NewJobDesc("notify")
}

func ArgumentTest(expected int) sqlq.HandlerFunc {
	return func(_ context.Context, job *sqlq.Job) error {
		var received int
		if err := json.Unmarshal(job.Parameters, &received); err != nil {
			return err
		}

		if received != expected {
			return errors.Errorf("expected: %d\treceived: %d", expected, received)
		}

		return nil
	}
}

func NewArgumentTest(i int) *sqlq.JobDescription {
	var b, _ = json.Marshal(&i)
	return sqlq.NewJobDesc("argument", sqlq.WithParameters(b))
}

func AdditionTest() sqlq.HandlerFunc {
	return func(ctx context.Context, job *sqlq.Job) error {
		var args struct{ A, B int }
		if err := json.Unmarshal(job.Parameters, &args); err != nil {
			return err
		}

		var result = args.A + args.B

		var writer = job.ResultWriter()
		if err := json.NewEncoder(writer).Encode(&result); err != nil {
			return err
		}

		return writer.Close()
	}
}

func NewAdditionTest(a, b int) *sqlq.JobDescription {
	var args = struct{ A, B int }{A: a, B: b}
	var buf, _ = json.Marshal(&args)

	return sqlq.NewJobDesc("addition", sqlq.WithParameters(buf))
}

func TestEmbedBasic(t *testing.T) {
	var upstream = MustOpen(PostgresUrl)
	defer upstream.Close()

	for i := 0; i < 10; i++ {
		_, _ = sqlq.Enqueue(upstream, "embed/default", NewNotifyTask())
	}
	_ = setConcurrencyAndPriority(upstream, "embed/default", 1, 10)

	var worker, _ = embed.NewWorker(upstream, embed.WorkerConfig{
		Queues: []sqlq.Queue{"embed/default"},
	})

	var wg sync.WaitGroup
	wg.Add(10)

	_ = worker.Register("notify", WaitGroupNotifier(&wg))

	if err := worker.Start(); err != nil {
		t.Fatalf("failed to start worker: %v", err)
	}

	// wait for the routine to execute
	wg.Wait()

	if err := worker.Shutdown(5 * time.Second); err != nil {
		t.Fatalf("failed to shutdown worker: %v", err)
	}
}

func TestEmbedWithParams(t *testing.T) {
	var upstream = MustOpen(PostgresUrl)
	defer upstream.Close()

	_, _ = sqlq.Enqueue(upstream, "embed/default", NewArgumentTest(100))
	var worker, _ = embed.NewWorker(upstream, embed.WorkerConfig{
		Queues: []sqlq.Queue{"embed/default"},
	})

	_ = worker.Register("argument", ArgumentTest(100))

	if err := worker.Start(); err != nil {
		t.Fatalf("failed to start worker: %v", err)
	}

	time.Sleep(time.Second)

	if err := worker.Shutdown(5 * time.Second); err != nil {
		t.Fatalf("failed to shutdown worker: %v", err)
	}
}

func TestEmbedWithResult(t *testing.T) {
	// ❗️need to manually confirm this test by checking the database❗️

	var upstream = MustOpen(PostgresUrl)
	defer upstream.Close()

	_, _ = sqlq.Enqueue(upstream, "embed/default", NewAdditionTest(100, 200))
	var worker, _ = embed.NewWorker(upstream, embed.WorkerConfig{
		Queues: []sqlq.Queue{"embed/default"},
	})

	_ = worker.Register("addition", AdditionTest())

	if err := worker.Start(); err != nil {
		t.Fatalf("failed to start worker: %v", err)
	}

	time.Sleep(time.Second)

	if err := worker.Shutdown(5 * time.Second); err != nil {
		t.Fatalf("failed to shutdown worker: %v", err)
	}
}

func TestEmbedWithLogger(t *testing.T) {
	var upstream = MustOpen(PostgresUrl)
	defer upstream.Close()

	var wg sync.WaitGroup
	wg.Add(1)

	var logger = sqlq.HandlerFunc(func(_ context.Context, job *sqlq.Job) error {
		var log = job.Logger()

		for i := 0; i < 1000; i++ {
			log.Infof("i: %d", i)
		}

		wg.Done()
		return nil
	})

	_, _ = sqlq.Enqueue(upstream, "embed/default", sqlq.NewJobDesc("logger"))
	var worker, _ = embed.NewWorker(upstream, embed.WorkerConfig{
		Queues: []sqlq.Queue{"embed/default"},
	})

	_ = worker.Register("logger", logger)

	if err := worker.Start(); err != nil {
		t.Fatalf("failed to start worker: %v", err)
	}

	wg.Wait()

	if err := worker.Shutdown(5 * time.Second); err != nil {
		t.Fatalf("failed to shutdown worker: %v", err)
	}
}

func setConcurrencyAndPriority(db *sql.DB, queue sqlq.Queue, c, p int) error {
	result, err := db.Exec("UPDATE sqlq.queues SET concurrency = $2, priority = $3 WHERE queues.name = $1", queue, c, p)
	if affected, _ := result.RowsAffected(); affected == 0 {
		return errors.Errorf("no queue with name = %q", queue)
	}

	return err
}
