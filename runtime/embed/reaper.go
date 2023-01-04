package embed

import (
	"database/sql"
	"github.com/mergestat/sqlq"
	"time"
)

func reaper(worker *Worker) (shutdown func() error) {
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

	go func() {
		for {
			select {
			case <-quit:
				return
			case <-time.After(5 * time.Second):
				_ = reap()
			}
		}
	}()

	return done
}
