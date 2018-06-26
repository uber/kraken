package tagstore_test

import (
	"errors"
	"sync"
	"testing"

	"code.uber.internal/infra/kraken/build-index/tagclient"
	. "code.uber.internal/infra/kraken/build-index/tagstore"
	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/backend"
	"code.uber.internal/infra/kraken/lib/backend/backenderrors"
	"code.uber.internal/infra/kraken/lib/persistedretry/tagreplication"
	"code.uber.internal/infra/kraken/lib/persistedretry/writeback"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/mocks/build-index/tagclient"
	"code.uber.internal/infra/kraken/mocks/build-index/tagstore"
	"code.uber.internal/infra/kraken/mocks/lib/backend"
	"code.uber.internal/infra/kraken/mocks/lib/persistedretry"
	"code.uber.internal/infra/kraken/utils/rwutil"
	"code.uber.internal/infra/kraken/utils/testutil"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

const (
	_testNamespace = ".*"
	_testRemote    = "remote-build-index"
)

type storeMocks struct {
	ctrl              *gomock.Controller
	mfs               *mocktagstore.MockFileStore
	ss                *store.SimpleStore
	backends          *backend.Manager
	backendClient     *mockbackend.MockClient
	writeBackManager  *mockpersistedretry.MockManager
	remotes           tagreplication.Remotes
	tagClientProvider *mocktagclient.MockProvider
}

func newStoreMocks(t *testing.T) (*storeMocks, func()) {
	var cleanup testutil.Cleanup
	defer cleanup.Recover()

	ctrl := gomock.NewController(t)
	cleanup.Add(ctrl.Finish)

	mfs := mocktagstore.NewMockFileStore(ctrl)

	ss, c := store.SimpleStoreFixture()
	cleanup.Add(c)

	backends := backend.ManagerFixture()
	backendClient := mockbackend.NewMockClient(ctrl)
	require.NoError(t, backends.Register(_testNamespace, backendClient))

	writeBackManager := mockpersistedretry.NewMockManager(ctrl)

	remotes, err := tagreplication.RemotesConfig{
		_testNamespace: []string{_testRemote},
	}.Build()
	require.NoError(t, err)

	tagClientProvider := mocktagclient.NewMockProvider(ctrl)

	return &storeMocks{
		ctrl, mfs, ss, backends, backendClient, writeBackManager, remotes,
		tagClientProvider}, cleanup.Run
}

type storeType int

const (
	_realDisk storeType = iota + 1
	_mockDisk
)

func (m *storeMocks) new(st storeType) Store {
	var fs FileStore
	if st == _realDisk {
		fs = m.ss
	} else {
		fs = m.mfs
	}
	return New(
		Config{}, tally.NoopScope, fs, m.backends, m.writeBackManager, m.remotes,
		m.tagClientProvider)
}

func checkConcurrentGets(t *testing.T, store Store, tag string, expected core.Digest) {
	t.Helper()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, err := store.Get(tag, true)
			require.NoError(t, err)
			require.Equal(t, expected, result)
		}()
	}
	wg.Wait()
}

func TestPutAndGetFromDisk(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newStoreMocks(t)
	defer cleanup()

	store := mocks.new(_realDisk)

	tag := core.TagFixture()
	digest := core.DigestFixture()

	mocks.writeBackManager.EXPECT().Add(
		writeback.MatchTask(writeback.NewTask(tag, tag))).Return(nil)

	require.NoError(store.Put(tag, digest, 0))

	checkConcurrentGets(t, store, tag, digest)
}

func TestGetFromBackend(t *testing.T) {
	mocks, cleanup := newStoreMocks(t)
	defer cleanup()

	store := mocks.new(_mockDisk)

	tag := core.TagFixture()
	digest := core.DigestFixture()

	gomock.InOrder(
		mocks.mfs.EXPECT().GetCacheFileReader(tag).Return(nil, errors.New("some error")),
		mocks.backendClient.EXPECT().Download(
			tag, rwutil.MatchWriter([]byte(digest.String()))).Return(nil),
		mocks.mfs.EXPECT().CreateCacheFile(
			tag, rwutil.MatchReader([]byte(digest.String()))).Return(nil),
	)

	checkConcurrentGets(t, store, tag, digest)
}

func TestGetFromBackendNotFoundNoFallback(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newStoreMocks(t)
	defer cleanup()

	store := mocks.new(_mockDisk)

	tag := core.TagFixture()

	gomock.InOrder(
		mocks.mfs.EXPECT().GetCacheFileReader(tag).Return(nil, errors.New("some error")),
		mocks.backendClient.EXPECT().Download(tag, gomock.Any()).Return(backenderrors.ErrBlobNotFound),
	)

	_, err := store.Get(tag, false)
	require.Equal(ErrTagNotFound, err)
}

func TestGetFromRemote(t *testing.T) {
	mocks, cleanup := newStoreMocks(t)
	defer cleanup()

	store := mocks.new(_mockDisk)

	tag := core.TagFixture()
	digest := core.DigestFixture()

	remoteTagClient := mocktagclient.NewMockClient(mocks.ctrl)

	gomock.InOrder(
		mocks.mfs.EXPECT().GetCacheFileReader(tag).Return(nil, errors.New("some error")),
		mocks.backendClient.EXPECT().Download(tag, gomock.Any()).Return(errors.New("some error")),
		mocks.tagClientProvider.EXPECT().Provide(_testRemote).Return(remoteTagClient),
		remoteTagClient.EXPECT().GetLocal(tag).Return(digest, nil),
		mocks.mfs.EXPECT().CreateCacheFile(
			tag, rwutil.MatchReader([]byte(digest.String()))).Return(nil),
	)

	checkConcurrentGets(t, store, tag, digest)
}

func TestGetFromRemoteNotFound(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newStoreMocks(t)
	defer cleanup()

	store := mocks.new(_mockDisk)

	tag := core.TagFixture()

	remoteTagClient := mocktagclient.NewMockClient(mocks.ctrl)

	gomock.InOrder(
		mocks.mfs.EXPECT().GetCacheFileReader(tag).Return(nil, errors.New("some error")),
		mocks.backendClient.EXPECT().Download(tag, gomock.Any()).Return(errors.New("some error")),
		mocks.tagClientProvider.EXPECT().Provide(_testRemote).Return(remoteTagClient),
		remoteTagClient.EXPECT().GetLocal(tag).Return(core.Digest{}, tagclient.ErrTagNotFound),
	)

	_, err := store.Get(tag, true)
	require.Equal(ErrTagNotFound, err)
}
