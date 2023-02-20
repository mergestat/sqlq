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

	if err = applySetup(c); err != nil {
		return err
	}

	var migrations = ReadMigrations(root)
	sort.Stable(migrations) // sort in ascending order of version number

	currentVersion, err := getCurrentMigrationVersion(c)
	if err != nil {
		return err
	}

	for _, mg := range migrations {

		if mg.Version() <= currentVersion {
			continue // already applied. skip this migration
		}

		if err = applyMigration(c, mg); err != nil {
			return errors.Wrapf(err, "failed to apply migration: name=%s\tversion=%d", mg.Name(), mg.Version())
		}

	}

	return nil
}

// getCurrentMigrationVersion gets the current version of our migrations
func getCurrentMigrationVersion(c *sql.DB) (int, error) {
	const fetchCurrentVersion = "SELECT version FROM sqlq_migrations ORDER BY applied_on DESC, version DESC LIMIT 1"
	var currentVersion int
	var err error
	var rows *sql.Rows

	if rows, err = c.Query(fetchCurrentVersion); err != nil && err != sql.ErrNoRows {
		return 0, errors.Wrap(err, "failed to fetch latest migration version")
	}

	defer func() { _ = rows.Close() }()

	if rows.Next() {
		if err = rows.Scan(&currentVersion); err != nil && err != sql.ErrNoRows {
			return 0, errors.Wrap(err, "failed to fetch latest migration version")
		}
	}

	if err = rows.Err(); err != nil {
		return 0, errors.Wrap(err, "failed to iterate rows")
	}

	return currentVersion, errors.Wrapf(err, "failed getting current migration version")
}

// applySetup creates if needed a migrations table
func applySetup(c *sql.DB) error {
	var tx *sql.Tx
	var err error
	if tx, err = c.Begin(); err != nil {
		return errors.Wrapf(err, "failed to start new transaction")
	}
	defer func() { _ = tx.Rollback() }()

	if err = setup(tx); err != nil {
		return errors.Wrapf(err, "failed to perform setup")
	}
	return errors.Wrapf(tx.Commit(), "failed to commit setup")
}

// applyMigration applies a single embedded migration to the database
func applyMigration(c *sql.DB, mg *EmbeddedMigration) error {
	var tx *sql.Tx
	var err error
	if tx, err = c.Begin(); err != nil {
		return errors.Wrapf(err, "failed to start new transaction")
	}
	defer func() { _ = tx.Rollback() }()

	if err = mg.Apply(tx); err != nil {
		return errors.Wrapf(err, "failed to apply migration(%s)", mg.Name())
	}

	const recordMigration = "INSERT INTO sqlq_migrations(name, version) VALUES ($1, $2)"

	if _, err = tx.Exec(recordMigration, mg.Name(), mg.Version()); err != nil {
		return errors.Wrapf(err, "failed to record migration: name=%s\tversion=%d", mg.Name(), mg.Version())
	}
	return errors.Wrapf(tx.Commit(), "failed to commit migrations")
}

// Teardown tears down the sql migrations and drop the default schema
func Teardown(c *sql.DB) (err error) {
	if _, err = c.Exec("DROP SCHEMA sqlq CASCADE"); err == nil {
		_, err = c.Exec("DROP TABLE sqlq_migrations")
	}
	return err
}
