package agentserver

import (
	"testing"

	"github.com/golang/mock/gomock"

	"code.uber.internal/infra/kraken/mocks/lib/torrent"
	"code.uber.internal/infra/kraken/utils/testutil"
)

type serverMocks struct {
	torrentClient *mocktorrent.MockClient
	cleanup       *testutil.Cleanup
}

func newServerMocks(t *testing.T) (*serverMocks, func()) {
	var cleanup testutil.Cleanup

	ctrl := gomock.NewController(t)
	cleanup.Add(ctrl.Finish)

	torrentClient := mocktorrent.NewMockClient(ctrl)

	return &serverMocks{torrentClient, &cleanup}, cleanup.Run
}

func (m *serverMocks) startServer() string {
	s := New(Config{}, m.torrentClient)
	addr, stop := testutil.StartServer(s.Handler())
	m.cleanup.Add(stop)
	return addr
}
