package agentserver

import (
	"testing"

	"github.com/golang/mock/gomock"

	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/mocks/lib/torrent"
	"code.uber.internal/infra/kraken/utils/testutil"
)

type serverMocks struct {
	fs            store.FileStore
	torrentClient *mocktorrent.MockClient
	cleanup       *testutil.Cleanup
}

func newServerMocks(t *testing.T) (*serverMocks, func()) {
	var cleanup testutil.Cleanup

	fs, c := store.LocalFileStoreFixture()
	cleanup.Add(c)

	ctrl := gomock.NewController(t)
	cleanup.Add(ctrl.Finish)

	torrentClient := mocktorrent.NewMockClient(ctrl)

	return &serverMocks{fs, torrentClient, &cleanup}, cleanup.Run
}

func (m *serverMocks) startServer() string {
	s := New(Config{}, m.fs, m.torrentClient)
	addr, stop := testutil.StartServer(s.Handler())
	m.cleanup.Add(stop)
	return addr
}
