package transfer

import (
	"bytes"
	"io/ioutil"
	"testing"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/mocks/lib/backend"
	"code.uber.internal/infra/kraken/mocks/lib/torrent/scheduler"
	"code.uber.internal/infra/kraken/utils/testutil"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

type agentTransfererMocks struct {
	fs        store.FileStore
	tagClient *mockbackend.MockClient
	sched     *mockscheduler.MockScheduler
}

func newAgentTransfererMocks(t *testing.T) (*agentTransfererMocks, func()) {
	var cleanup testutil.Cleanup

	fs, c := store.LocalFileStoreFixture()
	cleanup.Add(c)

	ctrl := gomock.NewController(t)
	cleanup.Add(ctrl.Finish)

	tagClient := mockbackend.NewMockClient(ctrl)

	sched := mockscheduler.NewMockScheduler(ctrl)

	return &agentTransfererMocks{fs, tagClient, sched}, cleanup.Run
}

func (m *agentTransfererMocks) new() *AgentTransferer {
	return NewAgentTransferer(m.fs, m.tagClient, _testNamespace, m.sched)
}

func TestAgentTransfererDownloadCachesBlob(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newAgentTransfererMocks(t)
	defer cleanup()

	transferer := mocks.new()

	blob := core.NewBlobFixture()
	name := blob.Digest.Hex()

	mocks.sched.EXPECT().Download(_testNamespace, name).DoAndReturn(func(namespace, name string) error {
		return mocks.fs.CreateCacheFile(name, bytes.NewReader(blob.Content))
	})

	// Downloading multiple times should only call scheduler download once.
	for i := 0; i < 10; i++ {
		result, err := transferer.Download(_testNamespace, name)
		require.NoError(err)
		b, err := ioutil.ReadAll(result)
		require.NoError(err)
		require.Equal(blob.Content, b)
	}
}
