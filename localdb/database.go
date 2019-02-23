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
package localdb

import (
	"fmt"

	_ "github.com/uber/kraken/localdb/migrations" // Add migrations.
	"github.com/uber/kraken/utils/osutil"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3" // SQL driver.
	"github.com/pressly/goose"
)

// New creates a new locally embedded SQLite database.
func New(config Config) (*sqlx.DB, error) {
	if err := osutil.EnsureFilePresent(config.Source, 0775); err != nil {
		return nil, fmt.Errorf("ensure db source present: %s", err)
	}
	db, err := sqlx.Open("sqlite3", config.Source)
	if err != nil {
		return nil, fmt.Errorf("open sqlite3: %s", err)
	}
	// SQLite has concurrency issues where queries result in error if more than
	// one connection is accessing a table.
	db.SetMaxOpenConns(1)
	if err := goose.SetDialect("sqlite3"); err != nil {
		return nil, fmt.Errorf("set dialect as sqlite3: %s", err)
	}
	if err := goose.Up(db.DB, "."); err != nil {
		return nil, fmt.Errorf("perform db migration: %s", err)
	}
	return db, nil
}
