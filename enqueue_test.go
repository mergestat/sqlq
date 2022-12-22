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
			if _, err := Enqueue(upstream, queue, NewAdditionJob(rand.Int(), rand.Int())); err != nil {
				t.Fatal(err)
			}
		})
	}
}
