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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/kraken/utils/osutil"
)

func TestNew(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		tmpdir := t.TempDir()
		source := filepath.Join(tmpdir, "test.db")

		db, err := New(Config{Source: source})
		require.NoError(t, err)
		defer db.Close()

		// Verify DB is usable
		err = db.Ping()
		assert.NoError(t, err)

		// Verify migrations ran - check tables exist
		var tables []string
		err = db.Select(&tables, `
			SELECT name FROM sqlite_master
			WHERE type='table' AND name NOT LIKE 'goose_%'
			ORDER BY name`)
		require.NoError(t, err)
		assert.Contains(t, tables, "replicate_tag_task")
		assert.Contains(t, tables, "writeback_task")
	})

	t.Run("error_invalid_path", func(t *testing.T) {
		// Try to create DB under a file (not a directory)
		tmpfile := filepath.Join(t.TempDir(), "file")
		require.NoError(t, os.WriteFile(tmpfile, []byte("x"), 0644))

		invalidPath := filepath.Join(tmpfile, "db.sqlite") // file/db.sqlite is invalid

		db, err := New(Config{Source: invalidPath})
		assert.Error(t, err)
		assert.Nil(t, db)
		assert.Contains(t, err.Error(), "ensure db source present")
	})

	t.Run("max_open_conns_is_one", func(t *testing.T) {
		tmpdir := t.TempDir()
		source := filepath.Join(tmpdir, "test.db")

		db, err := New(Config{Source: source})
		require.NoError(t, err)
		defer db.Close()

		// Verify single connection setting
		stats := db.Stats()
		assert.Equal(t, 1, stats.MaxOpenConnections)
	})

	t.Run("error_sqlx_open", func(t *testing.T) {
		// Save and restore original
		origEnsure := ensureFilePresent
		origOpen := sqlxOpen
		defer func() {
			ensureFilePresent = origEnsure
			sqlxOpen = origOpen
		}()

		// Mock ensureFilePresent to succeed
		ensureFilePresent = func(path string, perm os.FileMode) error {
			return nil
		}

		// Mock sqlxOpen to fail
		sqlxOpen = func(driverName, dataSourceName string) (*sqlx.DB, error) {
			return nil, errors.New("mock open error")
		}

		db, err := New(Config{Source: "test.db"})
		assert.Error(t, err)
		assert.Nil(t, db)
		assert.Contains(t, err.Error(), "open sqlite3")
	})

	t.Run("error_goose_set_dialect", func(t *testing.T) {
		// Save and restore original
		origEnsure := ensureFilePresent
		origOpen := sqlxOpen
		origDialect := gooseSetDialect
		defer func() {
			ensureFilePresent = origEnsure
			sqlxOpen = origOpen
			gooseSetDialect = origDialect
		}()

		tmpdir := t.TempDir()
		source := filepath.Join(tmpdir, "test.db")

		// Use real implementations for file and open
		ensureFilePresent = osutil.EnsureFilePresent
		sqlxOpen = sqlx.Open

		// Mock gooseSetDialect to fail
		gooseSetDialect = func(dialect string) error {
			return errors.New("mock dialect error")
		}

		db, err := New(Config{Source: source})
		assert.Error(t, err)
		assert.Nil(t, db)
		assert.Contains(t, err.Error(), "set dialect as sqlite3")
	})

	t.Run("error_goose_up", func(t *testing.T) {
		// Save and restore original
		origEnsure := ensureFilePresent
		origOpen := sqlxOpen
		origDialect := gooseSetDialect
		origUp := gooseUp
		defer func() {
			ensureFilePresent = origEnsure
			sqlxOpen = origOpen
			gooseSetDialect = origDialect
			gooseUp = origUp
		}()

		tmpdir := t.TempDir()
		source := filepath.Join(tmpdir, "test.db")

		// Use real implementations
		ensureFilePresent = osutil.EnsureFilePresent
		sqlxOpen = sqlx.Open
		gooseSetDialect = func(dialect string) error { return nil }

		// Mock gooseUp to fail
		gooseUp = func(db *sql.DB, dir string) error {
			return errors.New("mock migration error")
		}

		db, err := New(Config{Source: source})
		assert.Error(t, err)
		assert.Nil(t, db)
		assert.Contains(t, err.Error(), "perform db migration")
	})
}
