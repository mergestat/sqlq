package sqlq_test

import (
	"bytes"
	"encoding/json"
	"fmt"
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

func TestJob_Logger(t *testing.T) {
	var testingBackend = func(t *testing.T) LogBackendAdapter {
		return func(job *Job, level LogLevel, p string) (int, error) {
			t.Helper()
			var msg = fmt.Sprintf("level=%s\tjob=%d\tmsg=%s", level, job.ID, p)
			t.Log(msg)
			return len(msg), nil
		}
	}

	var err error
	var upstream = MustOpen(PostgresUrl)
	defer upstream.Close()

	if _, err = Enqueue(upstream, "test/job", NewJobDesc("job/logger")); err != nil {
		t.Fatal(err)
	}

	var job *Job
	if job, err = Dequeue(upstream, []Queue{"test/job"}); err != nil {
		t.Fatal(err)
	}

	job = AttachLogger(testingBackend(t), job)
	job.Logger().Infof("hello %q", "world")
}
