package storage

import (
	"testing"

	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/mocks/tracker/metainfoclient"
	"code.uber.internal/infra/kraken/utils/testutil"
	"github.com/golang/mock/gomock"
)

type torrentArchiveMocks struct {
	fs             store.FileStore
	metaInfoClient *mockmetainfoclient.MockClient
}

func newTorrentArchiveMocks(t *testing.T) (*torrentArchiveMocks, func()) {
	var cleanup testutil.Cleanup

	ctrl := gomock.NewController(t)
	cleanup.Add(ctrl.Finish)

	fs, c := store.LocalFileStoreFixture()
	cleanup.Add(c)

	metaInfoClient := mockmetainfoclient.NewMockClient(ctrl)

	return &torrentArchiveMocks{fs, metaInfoClient}, cleanup.Run
}

func (m *torrentArchiveMocks) newAgentTorrentArchive() *AgentTorrentArchive {
	return NewAgentTorrentArchive(m.fs, m.metaInfoClient)
}

func (m *torrentArchiveMocks) newOriginTorrentArchive() *OriginTorrentArchive {
	return NewOriginTorrentArchive(m.fs, m.metaInfoClient)
}
