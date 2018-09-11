package transfer

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"sync"
	"testing"
	"time"

	"code.uber.internal/infra/kraken/build-index/tagclient"
	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/mocks/build-index/tagclient"
	"code.uber.internal/infra/kraken/mocks/lib/torrent/scheduler"
	"code.uber.internal/infra/kraken/utils/testutil"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

type agentTransfererMocks struct {
	cads  *store.CADownloadStore
	tags  *mocktagclient.MockClient
	sched *mockscheduler.MockScheduler
}

func newReadOnlyTransfererMocks(t *testing.T) (*agentTransfererMocks, func()) {
	var cleanup testutil.Cleanup

	cads, c := store.CADownloadStoreFixture()
	cleanup.Add(c)

	ctrl := gomock.NewController(t)
	cleanup.Add(ctrl.Finish)

	tags := mocktagclient.NewMockClient(ctrl)

	sched := mockscheduler.NewMockScheduler(ctrl)

	return &agentTransfererMocks{cads, tags, sched}, cleanup.Run
}

func (m *agentTransfererMocks) new() *ReadOnlyTransferer {
	return NewReadOnlyTransferer(tally.NoopScope, m.cads, m.tags, m.sched)
}

func TestReadOnlyTransfererDownloadCachesBlob(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newReadOnlyTransfererMocks(t)
	defer cleanup()

	transferer := mocks.new()

	namespace := "docker/labrat:latest"
	blob := core.NewBlobFixture()

	mocks.sched.EXPECT().Download(
		namespace, blob.Digest.Hex()).DoAndReturn(func(namespace, name string) error {

		return store.RunDownload(mocks.cads, blob.Digest, blob.Content)
	})

	// Downloading multiple times should only call scheduler download once.
	for i := 0; i < 10; i++ {
		result, err := transferer.Download(namespace, blob.Digest)
		require.NoError(err)
		b, err := ioutil.ReadAll(result)
		require.NoError(err)
		require.Equal(blob.Content, b)
	}
}

func TestReadOnlyTransfererStat(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newReadOnlyTransfererMocks(t)
	defer cleanup()

	transferer := mocks.new()

	namespace := "docker/labrat:latest"
	blob := core.NewBlobFixture()

	mocks.sched.EXPECT().Download(
		namespace, blob.Digest.Hex()).DoAndReturn(func(namespace, name string) error {

		return store.RunDownload(mocks.cads, blob.Digest, blob.Content)
	})

	// Stat-ing multiple times should only call scheduler download once.
	for i := 0; i < 10; i++ {
		bi, err := transferer.Stat(namespace, blob.Digest)
		require.NoError(err)
		require.Equal(blob.Info(), bi)
	}
}

func TestReadOnlyTransfererGetTag(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newReadOnlyTransfererMocks(t)
	defer cleanup()

	transferer := mocks.new()

	tag := "docker/some-tag"
	manifest := core.DigestFixture()

	mocks.tags.EXPECT().Get(tag).Return(manifest, nil)

	d, err := transferer.GetTag(tag)
	require.NoError(err)
	require.Equal(manifest, d)
}

func TestReadOnlyTransfererGetTagNotFound(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newReadOnlyTransfererMocks(t)
	defer cleanup()

	transferer := mocks.new()

	tag := "docker/some-tag"

	mocks.tags.EXPECT().Get(tag).Return(core.Digest{}, tagclient.ErrTagNotFound)

	_, err := transferer.GetTag(tag)
	require.Error(err)
	require.Equal(ErrTagNotFound, err)
}

// TODO(codyg): This is a particularly ugly test that is a symptom of the lack
// of abstraction surrounding scheduler / file store operations.
func TestReadOnlyTransfererMultipleDownloadsOfSameBlob(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newReadOnlyTransfererMocks(t)
	defer cleanup()

	transferer := mocks.new()

	namespace := "docker/labrat:latest"
	blob := core.NewBlobFixture()

	require.NoError(mocks.cads.CreateDownloadFile(blob.Digest.Hex(), blob.Length()))
	w, err := mocks.cads.GetDownloadFileReadWriter(blob.Digest.Hex())
	require.NoError(err)
	_, err = io.Copy(w, bytes.NewReader(blob.Content))
	require.NoError(err)

	commit := make(chan struct{})

	mocks.sched.EXPECT().Download(
		namespace, blob.Digest.Hex()).DoAndReturn(func(namespace, name string) error {

		<-commit

		if err := mocks.cads.MoveDownloadFileToCache(name); !os.IsExist(err) {
			return err
		}
		return nil
	}).Times(10)

	// Multiple clients trying to download the same file which is already in
	// the download state should queue up until the file has been committed to
	// the cache.
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, err := transferer.Download(namespace, blob.Digest)
			require.NoError(err)
			b, err := ioutil.ReadAll(result)
			require.NoError(err)
			require.Equal(blob.Content, b)
		}()
	}

	time.Sleep(250 * time.Millisecond)

	close(commit)

	wg.Wait()
}
