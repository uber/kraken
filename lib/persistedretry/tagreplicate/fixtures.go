package tagreplicate

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/uber-go/tally"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/utils/randutil"
	"code.uber.internal/infra/kraken/utils/testutil"
)

// StoreFixture creates a fixture of tagreplicate.Store.
func StoreFixture(generator TaskGenerator) (*Store, string, func()) {
	var cleanup testutil.Cleanup
	defer cleanup.Recover()

	tmpDir, err := ioutil.TempDir(".", "test-store-")
	if err != nil {
		panic(err)
	}
	cleanup.Add(func() { os.RemoveAll(tmpDir) })

	source := filepath.Join(tmpDir, "test.db")
	store, err := NewStore(source, generator)
	if err != nil {
		panic(err)
	}

	return store, source, cleanup.Run
}

// TaskFixture creates a fixture of tagreplicate.Task.
func TaskFixture() *Task {
	id := randutil.Text(4)
	name := fmt.Sprintf("prime/labrat-%s", id)
	dest := fmt.Sprintf("build-index-%s", id)
	digest := core.DigestFixture()
	deps := []core.Digest{
		core.DigestFixture(), core.DigestFixture(), core.DigestFixture()}
	return NewTask(nil, nil, tally.NoopScope, name, dest, digest, deps...)
}
