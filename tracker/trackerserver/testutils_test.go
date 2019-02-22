package trackerserver

import (
	"net/http"
	"testing"

	"github.com/uber/kraken/mocks/origin/blobclient"
	"github.com/uber/kraken/mocks/tracker/originstore"
	"github.com/uber/kraken/mocks/tracker/peerstore"
	"github.com/uber/kraken/tracker/peerhandoutpolicy"

	"github.com/golang/mock/gomock"
	"github.com/uber-go/tally"
)

type serverMocks struct {
	config        Config
	policy        *peerhandoutpolicy.PriorityPolicy
	ctrl          *gomock.Controller
	peerStore     *mockpeerstore.MockStore
	originStore   *mockoriginstore.MockStore
	originCluster *mockblobclient.MockClusterClient
	stats         tally.Scope
}

func newServerMocks(t *testing.T, config Config) (*serverMocks, func()) {
	ctrl := gomock.NewController(t)
	return &serverMocks{
		config:        config,
		policy:        peerhandoutpolicy.DefaultPriorityPolicyFixture(),
		peerStore:     mockpeerstore.NewMockStore(ctrl),
		originStore:   mockoriginstore.NewMockStore(ctrl),
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
		m.originStore,
		m.originCluster).Handler()
}
