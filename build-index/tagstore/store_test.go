package tagstore_test

import (
	"sync"
	"testing"

	. "github.com/uber/kraken/build-index/tagstore"
	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/backend"
	"github.com/uber/kraken/lib/persistedretry/writeback"
	"github.com/uber/kraken/lib/store"
	"github.com/uber/kraken/mocks/lib/backend"
	"github.com/uber/kraken/mocks/lib/persistedretry"
	"github.com/uber/kraken/utils/mockutil"
	"github.com/uber/kraken/utils/testutil"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

const _testNamespace = ".*"

type storeMocks struct {
	ctrl             *gomock.Controller
	ss               *store.SimpleStore
	backends         *backend.Manager
	backendClient    *mockbackend.MockClient
	writeBackManager *mockpersistedretry.MockManager
}

func newStoreMocks(t *testing.T) (*storeMocks, func()) {
	var cleanup testutil.Cleanup
	defer cleanup.Recover()

	ctrl := gomock.NewController(t)
	cleanup.Add(ctrl.Finish)

	ss, c := store.SimpleStoreFixture()
	cleanup.Add(c)

	backends := backend.ManagerFixture()
	backendClient := mockbackend.NewMockClient(ctrl)
	require.NoError(t, backends.Register(_testNamespace, backendClient))

	writeBackManager := mockpersistedretry.NewMockManager(ctrl)

	return &storeMocks{ctrl, ss, backends, backendClient, writeBackManager}, cleanup.Run
}

func (m *storeMocks) new(config Config) Store {
	return New(config, tally.NoopScope, m.ss, m.backends, m.writeBackManager)
}

func checkConcurrentGets(t *testing.T, store Store, tag string, expected core.Digest) {
	t.Helper()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
		}()
	}
	wg.Wait()
}

func TestPutAndGetFromDisk(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newStoreMocks(t)
	defer cleanup()

	store := mocks.new(Config{})

	tag := core.TagFixture()
	digest := core.DigestFixture()

	mocks.writeBackManager.EXPECT().Add(
		writeback.MatchTask(writeback.NewTask(tag, tag, 0))).Return(nil)

	require.NoError(store.Put(tag, digest, 0))

	result, err := store.Get(tag)
	require.NoError(err)
	require.Equal(digest, result)
}

func TestPutAndGetFromDiskWriteThrough(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newStoreMocks(t)
	defer cleanup()

	store := mocks.new(Config{WriteThrough: true})

	tag := core.TagFixture()
	digest := core.DigestFixture()

	mocks.writeBackManager.EXPECT().SyncExec(
		writeback.MatchTask(writeback.NewTask(tag, tag, 0))).Return(nil)

	require.NoError(store.Put(tag, digest, 0))

	result, err := store.Get(tag)
	require.NoError(err)
	require.Equal(digest, result)
}

func TestGetCachesOnDisk(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newStoreMocks(t)
	defer cleanup()

	store := mocks.new(Config{})

	tag := core.TagFixture()
	digest := core.DigestFixture()

	mocks.backendClient.EXPECT().Download(
		tag, mockutil.MatchWriter([]byte(digest.String()))).Return(nil)

	// Getting multiple times should only cause one backend Download.
	for i := 0; i < 10; i++ {
		result, err := store.Get(tag)
		require.NoError(err)
		require.Equal(digest, result)
	}
}
