package trackerserver

import (
	"net/http"
	"testing"

	"code.uber.internal/infra/kraken/mocks/origin/blobclient"
	"code.uber.internal/infra/kraken/mocks/tracker/mockstorage"
	"code.uber.internal/infra/kraken/tracker/peerhandoutpolicy"
	"code.uber.internal/infra/kraken/tracker/storage"

	"github.com/golang/mock/gomock"
	"github.com/uber-go/tally"
)

type serverMocks struct {
	config        Config
	policy        peerhandoutpolicy.PeerHandoutPolicy
	ctrl          *gomock.Controller
	peerStore     *mockstorage.MockStorage
	metaInfoStore storage.MetaInfoStore
	originCluster *mockblobclient.MockClusterClient
	stats         tally.Scope
}

func newServerMocks(t *testing.T, config Config) (*serverMocks, func()) {
	ctrl := gomock.NewController(t)
	return &serverMocks{
		config:        config,
		policy:        peerhandoutpolicy.DefaultPeerHandoutPolicyFixture(),
		peerStore:     mockstorage.NewMockStorage(ctrl),
		metaInfoStore: storage.TestMetaInfoStore(),
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
		m.metaInfoStore,
		m.originCluster).Handler()
}
