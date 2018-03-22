package storage

import (
	"testing"

	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/mocks/tracker/metainfoclient"
	"code.uber.internal/infra/kraken/utils/testutil"
	"github.com/golang/mock/gomock"
	"github.com/uber-go/tally"
)

type agentMocks struct {
	fs             store.FileStore
	metaInfoClient *mockmetainfoclient.MockClient
}

func newAgentMocks(t *testing.T) (*agentMocks, func()) {
	var cleanup testutil.Cleanup

	ctrl := gomock.NewController(t)
	cleanup.Add(ctrl.Finish)

	fs, c := store.LocalFileStoreFixture()
	cleanup.Add(c)

	metaInfoClient := mockmetainfoclient.NewMockClient(ctrl)

	return &agentMocks{fs, metaInfoClient}, cleanup.Run
}

func (m *agentMocks) newTorrentArchive() *AgentTorrentArchive {
	return NewAgentTorrentArchive(tally.NoopScope, m.fs, m.metaInfoClient)
}
