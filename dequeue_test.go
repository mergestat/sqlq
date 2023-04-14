package sqlq_test

import (
	"database/sql"
	"sync"
	"testing"
	"time"

	. "github.com/mergestat/sqlq"
	"github.com/pkg/errors"
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

	if err := setConcurrencyAndPriority(upstream, "critical", 1, 0); err != nil {
		t.Fatalf("failed to update queue concurrency: %v", err)
	}

	if err := setConcurrencyAndPriority(upstream, "default", 2, 1); err != nil {
		t.Fatalf("failed to update queue concurrency: %v", err)
	}

	for i := 1; i <= 3; i++ {
		job, err := Dequeue(upstream, queues)
		if err != nil || job == nil {
			t.Fatalf("failed to dequeue: %#v", err) // shouldn't error out and must return a job
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

func TestDequeueConcurrent(t *testing.T) {
	var upstream = MustOpen(PostgresUrl)
	defer upstream.Close()

	const queue Queue = "compute-intensive"

	for i := 0; i < 5; i++ {
		if _, err := Enqueue(upstream, queue, NewAdditionJob(100, 200)); err != nil {
			t.Fatalf("failed to enqueue job: %v", err)
		}
	}

	if err := setConcurrencyAndPriority(upstream, queue, 2, 1); err != nil {
		t.Fatalf("failed to update queue concurrency: %v", err)
	}

	var wg sync.WaitGroup
	var result = make(chan *Job, 2)
	for i := 0; i < 5; i++ {
		// this is as close to "real-time" race-condition as we can get in an automated test scenario
		wg.Add(1)
		go func() {
			defer wg.Done()
			time.Sleep(time.Second * 1)
			if job, err := Dequeue(upstream, []Queue{queue}); err != nil {
				t.Errorf("failed to dequeue: %#v", err)
			} else {
				result <- job
			}
		}()
	}

	// close result channel after all results are written to it
	go func() { wg.Wait(); close(result) }()

	var allowed = 2
	for job := range result {
		if allowed == 0 && job != nil {
			t.Errorf("dequeued more jobs than allowed by concurrency")
		} else if job != nil {
			allowed -= 1 // decrease count of allowed jobs
		}
	}

	// wait for all Dequeue() to finish so that we do not close the database connection while they are running.
	// doing so leaves un-related error messages in the test's logs.
	wg.Wait()
}
