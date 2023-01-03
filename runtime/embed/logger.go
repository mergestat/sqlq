package embed

import (
	"database/sql"
	"github.com/mergestat/sqlq"
	"sync"
	"time"
)

// logger returns a new instance of a sqlq.LogBackend
func logger(db *sql.DB) (sqlq.LogBackendAdapter, func() error) {
	// event is the type of the payload shared on the internal buffer channel
	type event = struct {
		job   int
		level sqlq.LogLevel
		msg   string
	}

	// by default, all log messages are pushed into buffer
	var buffer = make(chan *event, 1000)
	var bufferLock sync.Mutex

	// flush flushes the buffer are write all entries to the database
	// in a single, batched transaction.
	var flush = func() (err error) {
		if !bufferLock.TryLock() {
			return nil // someone else is writing; do nothing
		}
		defer bufferLock.Unlock()

		var tx *sql.Tx
		if tx, err = db.Begin(); err != nil {
			return err
		}
		defer tx.Rollback()

		// enable asynchronous commit
		// see: https://www.postgresql.org/docs/current/wal-async-commit.html
		if _, err = tx.Exec("SET LOCAL synchronous_commit TO OFF"); err != nil {
			return err
		}

		var stmt *sql.Stmt
		const insertLog = `INSERT INTO sqlq.job_logs (job, level, message) VALUES ($1, $2, $3)`
		if stmt, err = tx.Prepare(insertLog); err != nil {
			return err
		}
		defer stmt.Close()

		// Read all buffered events and write them to the database.
		//
		// This is bit flaky as any new events from concurrent log operations
		// would keep this loop running.
		//
		// TODO(@riyaz): test and figure out if this could lead to a problem.
		for {
			select {
			case evt := <-buffer:
				_, _ = stmt.Exec(evt.job, evt.level.String(), evt.msg)
			default:
				// if reading from buffer blocks, it means that we've reached
				// end of buffer and can now commit.
				return tx.Commit()
			}
		}
	}

	var quit = make(chan struct{}) // used to terminate goroutine below
	var done = func() error { close(quit); return flush() }

	// periodically flush the buffer so that we do not have to wait for
	// the buffer to fill completely before writing logs.
	go func() {
		for {
			select {
			case <-quit:
				return
			case <-time.After(5 * time.Second):
				_ = flush()
			}
		}
	}()

	// sqlq.LogBackend implementation that sends log to sqlq.job_logs table
	var backend = func(job *sqlq.Job, level sqlq.LogLevel, msg string) (int, error) {
		for {
			select {
			case buffer <- &event{job: job.ID, level: level, msg: msg}:
				return len(msg), nil
			default:
				_ = flush()
			}
		}
	}

	return backend, done
}
