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
package localdb

import (
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/pressly/goose"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/kraken/utils/osutil"
)

func TestNew(t *testing.T) {
	tests := []struct {
		desc          string
		setupMocks    func(t *testing.T) (source string, cleanup func())
		wantErr       bool
		errContains   string
		verifySuccess func(t *testing.T, db *sqlx.DB)
	}{
		{
			desc: "success",
			setupMocks: func(t *testing.T) (string, func()) {
				resetMocks()
				source := filepath.Join(t.TempDir(), "test.db")
				return source, func() {}
			},
			wantErr: false,
			verifySuccess: func(t *testing.T, db *sqlx.DB) {
				// Verify DB is usable
				require.NoError(t, db.Ping())

				// Verify migrations ran - check tables exist
				var tables []string
				err := db.Select(&tables, `
					SELECT name FROM sqlite_master
					WHERE type='table' AND name NOT LIKE 'goose_%'
					ORDER BY name`)
				require.NoError(t, err)
				assert.Contains(t, tables, "replicate_tag_task")
				assert.Contains(t, tables, "writeback_task")
			},
		},
		{
			desc: "error_invalid_path",
			setupMocks: func(t *testing.T) (string, func()) {
				resetMocks()
				// Try to create DB under a file (not a directory)
				tmpfile := filepath.Join(t.TempDir(), "file")
				require.NoError(t, os.WriteFile(tmpfile, []byte("x"), 0644))
				invalidPath := filepath.Join(tmpfile, "db.sqlite")
				return invalidPath, func() {}
			},
			wantErr:     true,
			errContains: "ensure db source present",
		},
		{
			desc: "max_open_conns_is_one",
			setupMocks: func(t *testing.T) (string, func()) {
				resetMocks()
				source := filepath.Join(t.TempDir(), "test.db")
				return source, func() {}
			},
			wantErr: false,
			verifySuccess: func(t *testing.T, db *sqlx.DB) {
				stats := db.Stats()
				assert.Equal(t, 1, stats.MaxOpenConnections)
			},
		},
		{
			desc: "error_sqlx_open",
			setupMocks: func(t *testing.T) (string, func()) {
				origEnsure := ensureFilePresent
				origOpen := sqlxOpen

				ensureFilePresent = func(path string, perm os.FileMode) error {
					return nil
				}
				sqlxOpen = func(driverName, dataSourceName string) (*sqlx.DB, error) {
					return nil, errors.New("mock open error")
				}

				return "test.db", func() {
					ensureFilePresent = origEnsure
					sqlxOpen = origOpen
				}
			},
			wantErr:     true,
			errContains: "open sqlite3",
		},
		{
			desc: "error_goose_set_dialect",
			setupMocks: func(t *testing.T) (string, func()) {
				origEnsure := ensureFilePresent
				origOpen := sqlxOpen
				origDialect := gooseSetDialect

				source := filepath.Join(t.TempDir(), "test.db")

				ensureFilePresent = osutil.EnsureFilePresent
				sqlxOpen = sqlx.Open
				gooseSetDialect = func(dialect string) error {
					return errors.New("mock dialect error")
				}

				return source, func() {
					ensureFilePresent = origEnsure
					sqlxOpen = origOpen
					gooseSetDialect = origDialect
				}
			},
			wantErr:     true,
			errContains: "set dialect as sqlite3",
		},
		{
			desc: "error_goose_up",
			setupMocks: func(t *testing.T) (string, func()) {
				origEnsure := ensureFilePresent
				origOpen := sqlxOpen
				origDialect := gooseSetDialect
				origUp := gooseUp

				source := filepath.Join(t.TempDir(), "test.db")

				ensureFilePresent = osutil.EnsureFilePresent
				sqlxOpen = sqlx.Open
				gooseSetDialect = func(dialect string) error { return nil }
				gooseUp = func(db *sql.DB, dir string) error {
					return errors.New("mock migration error")
				}

				return source, func() {
					ensureFilePresent = origEnsure
					sqlxOpen = origOpen
					gooseSetDialect = origDialect
					gooseUp = origUp
				}
			},
			wantErr:     true,
			errContains: "perform db migration",
		},
	}

	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			source, cleanup := tt.setupMocks(t)
			defer cleanup()

			db, err := New(Config{Source: source})

			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, db)
				assert.Contains(t, err.Error(), tt.errContains)
			} else {
				require.NoError(t, err)
				defer db.Close()
				if tt.verifySuccess != nil {
					tt.verifySuccess(t, db)
				}
			}
		})
	}
}

// resetMocks resets all mocks to their original implementations.
func resetMocks() {
	ensureFilePresent = osutil.EnsureFilePresent
	sqlxOpen = sqlx.Open
	gooseSetDialect = goose.SetDialect
	gooseUp = func(db *sql.DB, dir string) error { return goose.Up(db, dir) }
}
