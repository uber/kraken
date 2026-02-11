// Copyright (c) 2016-2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package migrations

import (
	"database/sql"

	"github.com/pressly/goose"
)

func init() {
	goose.AddMigration(up00003, down00003)
}

func up00003(tx *sql.Tx) error {
	// Add trace_id, span_id, and trace_flags columns for linking async writeback to original request trace
	_, err := tx.Exec(`
		ALTER TABLE writeback_task ADD COLUMN trace_id text DEFAULT '';
	`)
	if err != nil {
		return err
	}
	_, err = tx.Exec(`
		ALTER TABLE writeback_task ADD COLUMN span_id text DEFAULT '';
	`)
	if err != nil {
		return err
	}
	_, err = tx.Exec(`
		ALTER TABLE writeback_task ADD COLUMN trace_flags text DEFAULT '';
	`)
	return err
}

func down00003(tx *sql.Tx) error {
	_, err := tx.Exec(`
		CREATE TABLE writeback_task_backup AS SELECT namespace, name, created_at, last_attempt, status, failures, delay FROM writeback_task;
		DROP TABLE writeback_task;
		CREATE TABLE writeback_task (
			namespace    text      NOT NULL,
			name         text      NOT NULL,
			created_at   timestamp DEFAULT CURRENT_TIMESTAMP,
			last_attempt timestamp NOT NULL,
			status       text      NOT NULL,
			failures     integer   NOT NULL,
			delay        integer   NOT NULL,
			PRIMARY KEY(namespace, name)
		);
		INSERT INTO writeback_task SELECT * FROM writeback_task_backup;
		DROP TABLE writeback_task_backup;
	`)
	return err
}
