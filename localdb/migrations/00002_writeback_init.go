package migrations

import (
	"database/sql"

	"github.com/pressly/goose"
)

func init() {
	goose.AddMigration(up00002, down00002)
}

func up00002(tx *sql.Tx) error {
	_, err := tx.Exec(`
		CREATE TABLE IF NOT EXISTS writeback_task (
			namespace    text      NOT NULL,
			name         text      NOT NULL,
			created_at   timestamp DEFAULT CURRENT_TIMESTAMP,
			last_attempt timestamp NOT NULL,
			status       text      NOT NULL,
			failures     integer   NOT NULL,
			delay        integer   NOT NULL,
			PRIMARY KEY(namespace, name)
		);
	`)
	return err
}

func down00002(tx *sql.Tx) error {
	_, err := tx.Exec(`DROP TABLE writeback_task;`)
	return err
}
