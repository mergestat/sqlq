package embed_test

import (
	"context"
	"database/sql"
	"github.com/mergestat/sqlq"
	"github.com/mergestat/sqlq/runtime/embed"
	"github.com/pkg/errors"
	"sync"
	"testing"
	"time"
)

func WaitGroupNotifier(wg *sync.WaitGroup) embed.HandlerFunc {
	return func(_ context.Context, _ *sqlq.Job) error { wg.Done(); return nil }
}

func NewNotifyTask() *sqlq.JobDescription {
	return sqlq.NewJobDesc("notify")
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

func setConcurrencyAndPriority(db *sql.DB, queue sqlq.Queue, c, p int) error {
	result, err := db.Exec("UPDATE sqlq.queues SET concurrency = $2, priority = $3 WHERE queues.name = $1", queue, c, p)
	if affected, _ := result.RowsAffected(); affected == 0 {
		return errors.Errorf("no queue with name = %q", queue)
	}

	return err
}
