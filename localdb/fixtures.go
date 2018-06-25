package localdb

import (
	"io/ioutil"
	"os"
	"path/filepath"

	"code.uber.internal/infra/kraken/utils/testutil"

	"github.com/jmoiron/sqlx"
)

// Fixture returns a temporary test database for testing.
func Fixture() (*sqlx.DB, func()) {
	var cleanup testutil.Cleanup
	defer cleanup.Recover()

	tmpdir, err := ioutil.TempDir(".", "test-db-")
	if err != nil {
		panic(err)
	}
	cleanup.Add(func() { os.RemoveAll(tmpdir) })

	source := filepath.Join(tmpdir, "test.db")

	db, err := New(Config{Source: source})
	if err != nil {
		panic(err)
	}

	return db, cleanup.Run
}
