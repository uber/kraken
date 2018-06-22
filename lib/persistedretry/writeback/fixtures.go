package writeback

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/utils/randutil"
	"code.uber.internal/infra/kraken/utils/testutil"
)

// StoreFixture creates a new Store for testing purposes.
func StoreFixture() (*Store, func()) {
	var cleanup testutil.Cleanup
	defer cleanup.Recover()

	tmpdir, err := ioutil.TempDir(".", "test-store-")
	if err != nil {
		panic(err)
	}
	cleanup.Add(func() { os.RemoveAll(tmpdir) })

	source := filepath.Join(tmpdir, "test.db")

	store, err := NewStore(source)
	if err != nil {
		panic(err)
	}
	cleanup.Add(func() { store.Close() })

	return store, cleanup.Run
}

// TaskFixture returns a randomly generated Task for testing purposes.
func TaskFixture() *Task {
	return NewTask(
		fmt.Sprintf("namespace-%s", randutil.Hex(8)),
		core.DigestFixture().Hex())
}
