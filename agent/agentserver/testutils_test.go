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
	cads    *store.CADownloadStore
	sched   *mockscheduler.MockReloadableScheduler
	cleanup *testutil.Cleanup
}

func newServerMocks(t *testing.T) (*serverMocks, func()) {
	var cleanup testutil.Cleanup

	cads, c := store.CADownloadStoreFixture()
	cleanup.Add(c)

	ctrl := gomock.NewController(t)
	cleanup.Add(ctrl.Finish)

	sched := mockscheduler.NewMockReloadableScheduler(ctrl)

	return &serverMocks{cads, sched, &cleanup}, cleanup.Run
}

func (m *serverMocks) startServer() string {
	s := New(Config{}, tally.NoopScope, m.cads, m.sched)
	addr, stop := testutil.StartServer(s.Handler())
	m.cleanup.Add(stop)
	return addr
}
