package trackerserver

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strconv"
	"testing"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/mocks/lib/backend"
	"code.uber.internal/infra/kraken/mocks/origin/blobclient"
	"code.uber.internal/infra/kraken/mocks/tracker/mockstorage"
	"code.uber.internal/infra/kraken/tracker/peerhandoutpolicy"
	"code.uber.internal/infra/kraken/utils/testutil"

	"github.com/golang/mock/gomock"
	"github.com/uber-go/tally"
)

// jsonBytesEqual compares the JSON in two byte slices.
func jsonBytesEqual(a, b []byte) (bool, error) {
	var j1, j2 interface{}
	if err := json.Unmarshal(a, &j1); err != nil {
		return false, err
	}
	if err := json.Unmarshal(b, &j2); err != nil {
		return false, err
	}
	return reflect.DeepEqual(j2, j1), nil
}

type testMocks struct {
	config        Config
	policy        peerhandoutpolicy.PeerHandoutPolicy
	ctrl          *gomock.Controller
	datastore     *mockstorage.MockStorage
	originCluster *mockblobclient.MockClusterClient
	tags          *mockbackend.MockClient
	stats         tally.Scope
}

func newTestMocks(t *testing.T) (*testMocks, func()) {
	ctrl := gomock.NewController(t)
	return &testMocks{
		config:        configFixture(),
		policy:        peerhandoutpolicy.DefaultPeerHandoutPolicyFixture(),
		datastore:     mockstorage.NewMockStorage(ctrl),
		originCluster: mockblobclient.NewMockClusterClient(ctrl),
		tags:          mockbackend.NewMockClient(ctrl),
		stats:         tally.NewTestScope("testing", nil),
	}, ctrl.Finish
}

// mockController sets up all mocks and returns a teardown func that can be called with defer
func (m *testMocks) mockController(t gomock.TestReporter) func() {
	m.config = configFixture()
	m.policy = peerhandoutpolicy.DefaultPeerHandoutPolicyFixture()
	m.ctrl = gomock.NewController(t)
	m.datastore = mockstorage.NewMockStorage(m.ctrl)
	m.originCluster = mockblobclient.NewMockClusterClient(m.ctrl)
	m.stats = tally.NewTestScope("testing", nil)
	return m.ctrl.Finish
}

func (m *testMocks) startServer() (string, func()) {
	return testutil.StartServer(m.handler())
}

func (m *testMocks) handler() http.Handler {
	return Handler(
		m.config,
		m.stats,
		m.policy,
		m.datastore,
		m.datastore,
		m.originCluster,
		m.tags)
}

func (m *testMocks) CreateHandlerAndServeRequest(request *http.Request) *http.Response {
	w := httptest.NewRecorder()
	m.handler().ServeHTTP(w, request)
	return w.Result()
}

func performRequest(handler http.Handler, request *http.Request) *http.Response {
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, request)
	return w.Result()
}

func createAnnouncePath(mi *core.MetaInfo, p *core.PeerInfo) string {
	v := url.Values{}
	v.Set("name", mi.Info.Name)
	v.Set("info_hash", mi.InfoHash.HexString())
	v.Set("peer_id", p.PeerID)
	v.Set("ip", p.IP)
	v.Set("port", strconv.FormatInt(p.Port, 10))
	v.Set("dc", p.DC)
	v.Set("complete", strconv.FormatBool(p.Complete))

	return "/announce?" + v.Encode()
}

func configFixture() Config {
	return Config{}
}
