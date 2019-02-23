// Copyright (c) 2016-2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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
