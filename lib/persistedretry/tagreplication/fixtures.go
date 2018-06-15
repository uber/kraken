package tagreplication

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/utils/randutil"
	"code.uber.internal/infra/kraken/utils/testutil"
)

// StoreFixture creates a fixture of tagreplication.Store.
func StoreFixture(rv RemoteValidator) (*Store, string, func()) {
	var cleanup testutil.Cleanup
	defer cleanup.Recover()

	tmpDir, err := ioutil.TempDir(".", "test-store-")
	if err != nil {
		panic(err)
	}
	cleanup.Add(func() { os.RemoveAll(tmpDir) })

	source := filepath.Join(tmpDir, "test.db")

	store, err := NewStore(source, rv)
	if err != nil {
		panic(err)
	}
	cleanup.Add(func() { store.Close() })

	return store, source, cleanup.Run
}

// TaskFixture creates a fixture of tagreplication.Task.
func TaskFixture() *Task {
	tag := core.TagFixture()
	d := core.DigestFixture()
	dest := fmt.Sprintf("build-index-%s", randutil.Hex(8))
	return NewTask(tag, d, core.DigestListFixture(3), dest)
}
