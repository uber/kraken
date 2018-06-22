package migrations

import (
	"database/sql"

	"code.uber.internal/infra/kraken/core"
	"github.com/jmoiron/sqlx"
	"github.com/pressly/goose"
)

func init() {
	goose.AddMigration(up00002, down00002)
}

func up00002(tx *sql.Tx) error {
	// SQLite doesn't support modifying primary keys, so we have to drain the entire
	// table into a copy with a new primary key constraint, delete the old table, and
	// rename the copy.
	if _, err := tx.Exec(`
		CREATE TABLE writeback_task_copy (
			namespace    text      NOT NULL,
			name         text      NOT NULL,
			created_at   timestamp DEFAULT CURRENT_TIMESTAMP,
			last_attempt timestamp NOT NULL,
			status       text      NOT NULL,
			failures     integer   NOT NULL,
			delay        integer   NOT NULL,
			PRIMARY KEY(namespace, name)
		)
	`); err != nil {
		return err
	}

	rows, err := tx.Query(`SELECT * FROM writeback_task`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		task := make(map[string]interface{})
		if err := sqlx.MapScan(rows, task); err != nil {
			return err
		}
		var d core.Digest
		d.Scan(task["digest"])
		name := d.Hex()
		if _, err := tx.Exec(`
			INSERT INTO writeback_task_copy (
				namespace, name, created_at, last_attempt, status, failures, delay
			) VALUES (
				?, ?, ?, ?, ?, ?, ?
			)`,
			task["namespace"], name, task["created_at"], task["last_attempt"],
			task["status"], task["failures"], task["delay"]); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if _, err := tx.Exec(`DROP TABLE writeback_task`); err != nil {
		return err
	}
	if _, err := tx.Exec(`ALTER TABLE writeback_task_copy RENAME TO writeback_task`); err != nil {
		return err
	}
	return nil
}

func down00002(tx *sql.Tx) error {
	if _, err := tx.Exec(`
		CREATE TABLE writeback_task_copy (
			namespace    text      NOT NULL,
			digest       blob      NOT NULL,
			created_at   timestamp DEFAULT CURRENT_TIMESTAMP,
			last_attempt timestamp NOT NULL,
			status       text      NOT NULL,
			failures     integer   NOT NULL,
			delay        integer   NOT NULL,
			PRIMARY KEY(namespace, name)
		)
	`); err != nil {
		return err
	}

	rows, err := tx.Query(`SELECT * FROM writeback_task`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		task := make(map[string]interface{})
		if err := sqlx.MapScan(rows, task); err != nil {
			return err
		}
		name := task["name"].(string)
		d, err := core.NewSHA256DigestFromHex(name)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(`
			INSERT INTO writeback_task_copy (
				namespace, digest, created_at, last_attempt, status, failures, delay
			) VALUES (
				?, ?, ?, ?, ?, ?, ?
			)`,
			task["namespace"], d, task["created_at"], task["last_attempt"],
			task["status"], task["failures"], task["delay"]); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if _, err := tx.Exec(`DROP TABLE writeback_task`); err != nil {
		return err
	}
	if _, err := tx.Exec(`ALTER TABLE writeback_task_copy RENAME TO writeback_task`); err != nil {
		return err
	}
	return nil
}
