// Package schema provides sql schema migrations for sqlq.
package schema

import (
	"database/sql"
	"embed"
	"github.com/pkg/errors"
	"io"
	"io/fs"
)

//go:embed *.sql
var migrations embed.FS // embedded migration scripts

// Apply applies any pending schema migration on the database.
func Apply(c *sql.DB) (err error) {
	var tx *sql.Tx
	if tx, err = c.Begin(); err != nil {
		return errors.Wrapf(err, "failed to start new transaction")
	}
	defer func() { _ = tx.Rollback() }()

	// for each file in the directory, we read+apply it returning error if any
	err = fs.WalkDir(migrations, ".", func(path string, entry fs.DirEntry, _ error) error {
		if entry.IsDir() {
			return nil
		}

		// TODO(@riyaz): skip migration if already applied

		var file, _ = migrations.Open(path)
		var buf, _ = io.ReadAll(file)

		if _, e := tx.Exec(string(buf)); e != nil {
			return errors.Wrapf(e, "failed to apply migration(%s)", entry.Name())
		}

		// TODO(@riyaz): mark the migration as applied

		return nil
	})

	if err != nil {
		return err
	}

	return errors.Wrapf(tx.Commit(), "failed to commit migration")
}

// Teardown tears down the sql migrations and drop the default schema
func Teardown(c *sql.DB) error { _, err := c.Exec("DROP SCHEMA sqlq CASCADE"); return err }
