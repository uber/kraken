package localdb

import (
	"fmt"

	_ "code.uber.internal/infra/kraken/localdb/migrations" // Add migrations.
	"code.uber.internal/infra/kraken/utils/osutil"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3" // SQL driver.
	"github.com/pressly/goose"
)

// New creates a new locally embedded SQLite database.
func New(config Config) (*sqlx.DB, error) {
	if err := osutil.EnsureFilePresent(config.Source); err != nil {
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
