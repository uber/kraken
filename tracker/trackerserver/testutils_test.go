package trackerserver

import (
	"net/http"
	"testing"

	"code.uber.internal/infra/kraken/mocks/origin/blobclient"
	"code.uber.internal/infra/kraken/mocks/tracker/mockpeerstore"
	"code.uber.internal/infra/kraken/tracker/peerhandoutpolicy"

	"github.com/golang/mock/gomock"
	"github.com/uber-go/tally"
)

type serverMocks struct {
	config        Config
	policy        *peerhandoutpolicy.PriorityPolicy
	ctrl          *gomock.Controller
	peerStore     *mockpeerstore.MockStore
	originCluster *mockblobclient.MockClusterClient
	stats         tally.Scope
}

func newServerMocks(t *testing.T, config Config) (*serverMocks, func()) {
	ctrl := gomock.NewController(t)
	return &serverMocks{
		config:        config,
		policy:        peerhandoutpolicy.DefaultPriorityPolicyFixture(),
		peerStore:     mockpeerstore.NewMockStore(ctrl),
		originCluster: mockblobclient.NewMockClusterClient(ctrl),
		stats:         tally.NewTestScope("testing", nil),
	}, ctrl.Finish
}

func (m *serverMocks) handler() http.Handler {
	return New(
		m.config,
		m.stats,
		m.policy,
		m.peerStore,
		m.originCluster).Handler()
}
