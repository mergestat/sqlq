// Package schema provides sql schema migrations for sqlq.
package schema

import (
	"database/sql"
	"embed"
	"sort"

	"github.com/pkg/errors"
)

//go:embed *.sql
var root embed.FS // embedded migration scripts

func setup(tx *sql.Tx) error {
	const createTable = `CREATE TABLE IF NOT EXISTS sqlq_migrations (id BIGSERIAL PRIMARY KEY, name TEXT UNIQUE, version INT, applied_on TIMESTAMPTZ DEFAULT now())`
	_, err := tx.Exec(createTable)
	return err
}

// Apply applies any pending schema migration on the database.
func Apply(c *sql.DB) (err error) {
	var tx *sql.Tx
	if tx, err = c.Begin(); err != nil {
		return errors.Wrapf(err, "failed to start new transaction")
	}
	defer func() { _ = tx.Rollback() }()

	if err = setup(tx); err != nil {
		return errors.Wrapf(err, "failed to perform setup")
	}

	var migrations = ReadMigrations(root)
	sort.Stable(migrations) // sort in ascending order of version number

	var currentVersion = 0
	const fetchCurrentVersion = "SELECT version FROM sqlq_migrations ORDER BY applied_on DESC, version DESC LIMIT 1"

	var rows *sql.Rows
	if rows, err = tx.Query(fetchCurrentVersion); err != nil && err != sql.ErrNoRows {
		return errors.Wrap(err, "failed to fetch latest migration version")
	}

	if rows.Next() {
		if err = rows.Scan(&currentVersion); err != nil && err != sql.ErrNoRows {
			return errors.Wrap(err, "failed to fetch latest migration version")
		}
	}

	if err = rows.Err(); err != nil {
		return errors.Wrap(err, "failed to iterate over rows")
	}

	for _, mg := range migrations {
		if mg.Version() <= currentVersion {
			continue // already applied. skip this migration
		}

		if err = mg.Apply(tx); err != nil {
			return errors.Wrapf(err, "failed to apply migration(%s)", mg.Name())
		}

		const recordMigration = "INSERT INTO sqlq_migrations(name, version) VALUES ($1, $2)"
		if _, err = tx.Exec(recordMigration, mg.Name(), mg.Version()); err != nil {
			return errors.Wrapf(err, "failed to record migration: name=%s\tversion=%d", mg.Name(), mg.Version())
		}
	}

	return errors.Wrapf(tx.Commit(), "failed to commit migration")
}

// Teardown tears down the sql migrations and drop the default schema
func Teardown(c *sql.DB) (err error) {
	if _, err = c.Exec("DROP SCHEMA sqlq CASCADE"); err == nil {
		_, err = c.Exec("DROP TABLE sqlq_migrations")
	}
	return err
}
