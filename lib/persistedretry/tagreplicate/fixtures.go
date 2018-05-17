package tagreplicate

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/utils/randutil"
	"code.uber.internal/infra/kraken/utils/testutil"
)

// StoreFixture creates a fixture of tagreplicate.Store.
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

// TaskFixture creates a fixture of tagreplicate.Task.
func TaskFixture() *Task {
	id := randutil.Text(4)
	tag := fmt.Sprintf("prime/labrat-%s", id)
	d := core.DigestFixture()
	deps := []core.Digest{
		core.DigestFixture(), core.DigestFixture(), core.DigestFixture()}
	dest := fmt.Sprintf("build-index-%s", id)
	return NewTask(tag, d, deps, dest)
}
