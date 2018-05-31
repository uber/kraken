package writeback

import (
	"bytes"
	"testing"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/backend"
	"code.uber.internal/infra/kraken/lib/backend/backenderrors"
	"code.uber.internal/infra/kraken/lib/backend/blobinfo"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/mocks/lib/backend"
	"code.uber.internal/infra/kraken/utils/rwutil"
	"code.uber.internal/infra/kraken/utils/testutil"

	"github.com/andres-erbsen/clock"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

type executorMocks struct {
	ctrl     *gomock.Controller
	fs       store.OriginFileStore
	backends *backend.Manager
}

func newExecutorMocks(t *testing.T) (*executorMocks, func()) {
	var cleanup testutil.Cleanup
	defer cleanup.Recover()

	ctrl := gomock.NewController(t)
	cleanup.Add(ctrl.Finish)

	fs, c := store.OriginFileStoreFixture(clock.New())
	cleanup.Add(c)

	return &executorMocks{
		ctrl:     ctrl,
		fs:       fs,
		backends: backend.ManagerFixture(),
	}, cleanup.Run
}

func (m *executorMocks) new() *Executor {
	return NewExecutor(tally.NoopScope, m.fs, m.backends)
}

func (m *executorMocks) client(namespace string) *mockbackend.MockClient {
	client := mockbackend.NewMockClient(m.ctrl)
	if err := m.backends.Register(namespace, client); err != nil {
		panic(err)
	}
	return client
}

func TestExec(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newExecutorMocks(t)
	defer cleanup()

	blob := core.NewBlobFixture()

	require.NoError(mocks.fs.CreateCacheFile(blob.Digest.Hex(), bytes.NewReader(blob.Content)))

	task := NewTask("test-namespace", blob.Digest)

	client := mocks.client(task.Namespace)
	client.EXPECT().Stat(blob.Digest.Hex()).Return(nil, backenderrors.ErrBlobNotFound)
	client.EXPECT().Upload(blob.Digest.Hex(), rwutil.MatchReader(blob.Content)).Return(nil)

	executor := mocks.new()

	require.NoError(executor.Exec(task))
}

func TestExecNoopWhenFileAlreadyUploaded(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newExecutorMocks(t)
	defer cleanup()

	blob := core.NewBlobFixture()

	require.NoError(mocks.fs.CreateCacheFile(blob.Digest.Hex(), bytes.NewReader(blob.Content)))

	task := NewTask("test-namespace", blob.Digest)

	client := mocks.client(task.Namespace)
	client.EXPECT().Stat(blob.Digest.Hex()).Return(blobinfo.New(blob.Length()), nil)

	executor := mocks.new()

	require.NoError(executor.Exec(task))
}

func TestExecNoopWhenFileMissing(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newExecutorMocks(t)
	defer cleanup()

	blob := core.NewBlobFixture()

	task := NewTask("test-namespace", blob.Digest)

	client := mocks.client(task.Namespace)
	client.EXPECT().Stat(blob.Digest.Hex()).Return(nil, backenderrors.ErrBlobNotFound)

	executor := mocks.new()

	require.NoError(executor.Exec(task))
}

func TestExecNoopWhenNamespaceNotFound(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newExecutorMocks(t)
	defer cleanup()

	blob := core.NewBlobFixture()

	require.NoError(mocks.fs.CreateCacheFile(blob.Digest.Hex(), bytes.NewReader(blob.Content)))

	task := NewTask("test-namespace", blob.Digest)

	executor := mocks.new()

	require.NoError(executor.Exec(task))
}
