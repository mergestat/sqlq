package sqlq

import (
	"context"
	"database/sql"
	"github.com/pkg/errors"
	"time"
)

type Pinger interface {
	Ping(context.Context) error
}

type pingFunc func(context.Context) error

func (fn pingFunc) Ping(ctx context.Context) error { return fn(ctx) }

// make a ping function for the given job
func pingFn(cx Connection, job *Job) Pinger {
	return pingFunc(func(ctx context.Context) (err error) {
		const ping = `UPDATE sqlq.jobs SET last_keepalive = NOW() WHERE id = $1 AND status = $2 RETURNING *`

		var rows *sql.Rows
		if rows, err = cx.QueryContext(ctx, ping, job.ID, job.Status); err != nil {
			return errors.Wrapf(err, "failed to send ping")
		}

		if err = scanJob(rows, job); err != nil {
			if err == sql.ErrNoRows { // job wasn't in the given state?
				return errors.Wrapf(ErrJobStateMismatch, "expected job to be in %s", job.Status)
			}
			return errors.Wrap(err, "failed to send ping")
		}

		return nil
	})
}

// SendKeepAlive is a utility method that sends periodic keep-alive pings, every d duration.
//
// This function starts an infinite for-loop that periodically sends ping using job.Pinger().
// Call this function in a background goroutine, like:
//
//	go job.SendKeepAlive(ctx, job.KeepAlive)
//
// To stop sending pings, cancel the context and the function will return.
func (job *Job) SendKeepAlive(ctx context.Context, d time.Duration) error {
	var ticker = time.NewTicker(d)
	for {
		select {
		case <-ctx.Done():
			ticker.Stop()
			return ctx.Err()
		case <-ticker.C:
			if err := job.Pinger().Ping(ctx); err != nil {
				return err
			}
		}
	}
}
