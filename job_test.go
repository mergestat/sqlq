package sqlq_test

import (
	"bytes"
	"encoding/json"
	. "github.com/mergestat/sqlq"
	"testing"
)

func TestJob_ResultWriter(t *testing.T) {
	type Result = struct{ Msg string }

	var err error
	var upstream = MustOpen(PostgresUrl)
	defer upstream.Close()

	if _, err = Enqueue(upstream, "test/job", NewJobDesc("job/result-writer")); err != nil {
		t.Fatal(err)
	}

	var job *Job
	if job, err = Dequeue(upstream, []Queue{"test/job"}); err != nil {
		t.Fatal(err)
	}

	var writer = AttachResultWriter(upstream, job).ResultWriter()
	if err = json.NewEncoder(writer).Encode(&Result{Msg: "hello"}); err != nil {
		t.Fatal(err)
	}

	if err = writer.Close(); err != nil {
		t.Fatal(err)
	}

	var r Result
	if err = json.NewDecoder(bytes.NewReader(job.Result)).Decode(&r); err != nil {
		t.Fatal(err)
	}

	if r.Msg != "hello" {
		t.FailNow()
	}
}
