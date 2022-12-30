package sqlq_test

import (
	"encoding/json"
	"fmt"
	. "github.com/mergestat/sqlq"
	"math/rand"
	"testing"
	"time"
)

func NewAdditionJob(a, b int) *JobDescription {
	var input = struct{ A, B int }{A: a, B: b}
	var params, _ = json.Marshal(&input)

	return NewJobDesc("addition", WithParameters(params), WithRetention(24*time.Hour))
}

func TestEnqueue(t *testing.T) {
	var upstream = MustOpen(PostgresUrl)
	defer upstream.Close()

	var queues = []Queue{"a", "b", "c", "d"}

	for i := 0; i < 10; i++ {
		var queue = queues[rand.Int()%len(queues)]

		t.Run(fmt.Sprintf("enqueue-%d", i), func(t *testing.T) {
			time.Sleep(100 * time.Millisecond)
			if job, err := Enqueue(upstream, queue, NewAdditionJob(rand.Int(), rand.Int())); err != nil {
				t.Fatal(err)
			} else {
				t.Logf("enqueued: %#v", job)
			}
		})
	}
}

func TestEnqueue_NoEnqueueOnRollback(t *testing.T) {
	var upstream = MustOpen(PostgresUrl)
	defer upstream.Close()

	var tx, _ = upstream.Begin()
	if _, err := Enqueue(tx, "enqueue/rollback", NewAdditionJob(1, 2)); err != nil {
		t.Fatalf("failed to enqueue job: %v", err)
	}
	_ = tx.Rollback()

	if job, err := Dequeue(upstream, []Queue{"enqueue/rollback"}); err != nil {
		t.Fatalf("failed to dequeue job: %v", err)
	} else if job != nil {
		t.Fatalf("expected job to be nil; got=%#v", job)
	}
}
