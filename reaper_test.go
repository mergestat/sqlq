package sqlq_test

import (
	"context"
	. "github.com/mergestat/sqlq"
	"testing"
	"time"
)

func TestReap(t *testing.T) {
	var upstream = MustOpen(PostgresUrl)
	defer upstream.Close()

	var err error
	var jd = NewJobDesc("blocker", WithMaxRetries(2), WithKeepAlive(2*time.Second))
	if _, err = Enqueue(upstream, "test/reaper", jd); err != nil {
		t.Fatalf("failed to enqueue: %#v", err)
	}

	var job *Job
	if job, err = Dequeue(upstream, []Queue{"test/reaper"}, WithTypeName([]string{"blocker"})); err != nil {
		t.Fatalf("failed to dequeue: %#v", err)
	}

	time.Sleep(job.KeepAlive) // wait for the keepalive time to pass

	var n int64
	if n, err = Reap(upstream, []Queue{"test/reaper"}); err != nil {
		t.Fatalf("failed to reap zombie processes: %#v", err)
	}

	if n != 1 {
		t.Fatalf("was expecting to reap %d job(s); reaped %d job(s)", 1, n)
	}
}

func TestJobPinger(t *testing.T) {
	var upstream = MustOpen(PostgresUrl)
	defer upstream.Close()

	var err error
	var jd = NewJobDesc("pinger", WithMaxRetries(2), WithKeepAlive(5*time.Second))
	if _, err = Enqueue(upstream, "test/pinger", jd); err != nil {
		t.Fatalf("failed to enqueue: %#v", err)
	}

	var job *Job
	if job, err = Dequeue(upstream, []Queue{"test/pinger"}, WithTypeName([]string{"pinger"})); err != nil {
		t.Fatalf("failed to dequeue: %#v", err)
	}

	var ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	job = AttachPinger(upstream, job)
	go func(job *Job) { _ = job.SendKeepAlive(ctx, job.KeepAlive) }(job)

	time.AfterFunc(1*time.Minute, cancel)

	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(10 * time.Second):
			if n, err := Reap(upstream, []Queue{"test/pinger"}); err != nil {
				t.Fatalf("failed to reap: %#v", err)
			} else if n != 0 {
				t.Fatalf("job reaped!")
			}
		}
	}
}
