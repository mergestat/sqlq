package embed

import (
	"database/sql"
	"github.com/mergestat/sqlq"
	"time"
)

// reaper() is the runtime service that reaps zombie processes
// that got killed / are stuck and haven't pinged back in a while.
func reaper(worker *Worker) (shutdown func() error) {
	// reap wraps sqlq.Reap() and run it inside a transaction
	var reap = func() (err error) {
		var tx *sql.Tx
		if tx, err = worker.db.Begin(); err != nil {
			return err
		}
		defer func() { _ = tx.Rollback() }()

		if _, err = sqlq.Reap(tx, worker.queues); err != nil {
			return err
		}

		return tx.Commit()
	}

	var quit = make(chan struct{}, 1)
	var done = func() error { close(quit); return reap() }

	// we run the reaper process asynchronously, in the background,
	// and clear up zombies every 5 seconds.
	go func() {
		for {
			select {
			case <-quit:
				return // received signal to terminate
			case <-time.After(5 * time.Second):
				_ = reap()
			}
		}
	}()

	return done
}
