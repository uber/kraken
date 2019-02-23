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
	goose.AddMigration(up00001, down00001)
}

func up00001(tx *sql.Tx) error {
	_, err := tx.Exec(
		`CREATE TABLE IF NOT EXISTS replicate_tag_task (
		tag          text      NOT NULL,
		digest       blob      NOT NULL,
		dependencies blob      NOT NULL,
		destination  text      NOT NULL,
		created_at   timestamp DEFAULT CURRENT_TIMESTAMP,
		last_attempt timestamp NOT NULL,
		status       text      NOT NULL,
		failures     integer   NOT NULL,
		delay        integer   NOT NULL,
		PRIMARY KEY(tag, destination)
	);`)
	return err
}

func down00001(tx *sql.Tx) error {
	_, err := tx.Exec(`DROP TABLE replicate_tag_task;`)
	return err
}
