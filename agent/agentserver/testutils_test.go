package agentserver

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/uber-go/tally"

	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/mocks/lib/torrent/scheduler"
	"code.uber.internal/infra/kraken/utils/testutil"
)

type serverMocks struct {
	fs      store.FileStore
	sched   *mockscheduler.MockReloadableScheduler
	cleanup *testutil.Cleanup
}

func newServerMocks(t *testing.T) (*serverMocks, func()) {
	var cleanup testutil.Cleanup

	fs, c := store.LocalFileStoreFixture()
	cleanup.Add(c)

	ctrl := gomock.NewController(t)
	cleanup.Add(ctrl.Finish)

	sched := mockscheduler.NewMockReloadableScheduler(ctrl)

	return &serverMocks{fs, sched, &cleanup}, cleanup.Run
}

func (m *serverMocks) startServer() string {
	s := New(Config{}, tally.NoopScope, m.fs, m.sched)
	addr, stop := testutil.StartServer(s.Handler())
	m.cleanup.Add(stop)
	return addr
}
