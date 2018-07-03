package transfer

import (
	"io/ioutil"
	"testing"

	"code.uber.internal/infra/kraken/build-index/tagclient"
	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/mocks/build-index/tagclient"
	"code.uber.internal/infra/kraken/mocks/lib/torrent/scheduler"
	"code.uber.internal/infra/kraken/utils/testutil"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

type agentTransfererMocks struct {
	cads  *store.CADownloadStore
	tags  *mocktagclient.MockClient
	sched *mockscheduler.MockScheduler
}

func newAgentTransfererMocks(t *testing.T) (*agentTransfererMocks, func()) {
	var cleanup testutil.Cleanup

	cads, c := store.CADownloadStoreFixture()
	cleanup.Add(c)

	ctrl := gomock.NewController(t)
	cleanup.Add(ctrl.Finish)

	tags := mocktagclient.NewMockClient(ctrl)

	sched := mockscheduler.NewMockScheduler(ctrl)

	return &agentTransfererMocks{cads, tags, sched}, cleanup.Run
}

func (m *agentTransfererMocks) new() *AgentTransferer {
	return NewAgentTransferer(m.cads, m.tags, m.sched)
}

func TestAgentTransfererDownloadCachesBlob(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newAgentTransfererMocks(t)
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

func TestAgentTransfererStat(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newAgentTransfererMocks(t)
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

func TestAgentTransfererGetTag(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newAgentTransfererMocks(t)
	defer cleanup()

	transferer := mocks.new()

	tag := "docker/some-tag"
	manifest := core.DigestFixture()

	mocks.tags.EXPECT().Get(tag).Return(manifest, nil)

	d, err := transferer.GetTag(tag)
	require.NoError(err)
	require.Equal(manifest, d)
}

func TestAgentTransfererGetTagNotFound(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newAgentTransfererMocks(t)
	defer cleanup()

	transferer := mocks.new()

	tag := "docker/some-tag"

	mocks.tags.EXPECT().Get(tag).Return(core.Digest{}, tagclient.ErrTagNotFound)

	_, err := transferer.GetTag(tag)
	require.Error(err)
	require.Equal(ErrTagNotFound, err)
}
