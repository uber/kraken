package migrations

import (
	"database/sql"

	"github.com/pressly/goose"
)

func init() {
	goose.AddMigration(up00001, down00001)
}

func up00001(tx *sql.Tx) error {
	_, err := tx.Exec(`
		CREATE TABLE IF NOT EXISTS writeback_task (
			namespace    text      NOT NULL,
			digest       blob      NOT NULL,
			created_at   timestamp DEFAULT CURRENT_TIMESTAMP,
			last_attempt timestamp NOT NULL,
			status       text      NOT NULL,
			failures     integer   NOT NULL,
			delay        integer   NOT NULL,
			PRIMARY KEY(namespace, digest)
		);
	`)
	return err
}

func down00001(tx *sql.Tx) error {
	_, err := tx.Exec(`DROP TABLE writeback_task;`)
	return err
}
