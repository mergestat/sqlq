package sqlq_test

import (
	"database/sql"
	. "github.com/mergestat/sqlq"
	"github.com/pkg/errors"
	"testing"
)

func setConcurrencyAndPriority(db *sql.DB, queue Queue, c, p int) error {
	result, err := db.Exec("UPDATE sqlq.queues SET concurrency = $2, priority = $3 WHERE queues.name = $1", queue, c, p)
	if affected, _ := result.RowsAffected(); affected == 0 {
		return errors.Errorf("no queue with name = %q", queue)
	}

	return err
}

func TestDequeue(t *testing.T) {
	var upstream = MustOpen(PostgresUrl)
	defer upstream.Close()

	var queues = []Queue{"critical", "default"}
	for i := 0; i < 10; i++ {
		if _, err := Enqueue(upstream, queues[i%2], NewAdditionJob(100, 200)); err != nil {
			t.Fatalf("failed to enqueue job: %v", err)
		}
	}

	if err := setConcurrencyAndPriority(upstream, "critical", 1, 10); err != nil {
		t.Fatalf("failed to update queue concurrency: %v", err)
	}

	if err := setConcurrencyAndPriority(upstream, "default", 2, 1); err != nil {
		t.Fatalf("failed to update queue concurrency: %v", err)
	}

	for i := 1; i <= 3; i++ {
		job, err := Dequeue(upstream, queues)
		if err != nil || job == nil {
			t.Fatalf("failed to dequeue") // shouldn't error out and must return a job
		}
		t.Logf("dequeued: %#v", job)

		if i == 1 {
			// for the first run, it MUST pull from the critical queue
			if job.Queue != "critical" {
				t.Fatalf("expected first job to be from 'critical' queue: got=%q", job.Queue)
			}

			continue
		}

		// for the 2nd and 3rd run, it MUST pull from the default queue
		if job.Queue != "default" {
			t.Fatalf("expected job to be from 'default' queue: got=%q", job.Queue)
		}
	}

	// since all queues are running at max concurrency, next Dequeue() should return no job
	if job, err := Dequeue(upstream, queues); err != nil || job != nil {
		t.FailNow()
	}
}
