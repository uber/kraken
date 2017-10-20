package service

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strconv"
	"time"

	"code.uber.internal/infra/kraken/mocks/origin/blobclient"
	"code.uber.internal/infra/kraken/mocks/tracker/mockstorage"
	"code.uber.internal/infra/kraken/torlib"
	"code.uber.internal/infra/kraken/tracker/peerhandoutpolicy"

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
	config         Config
	policy         peerhandoutpolicy.PeerHandoutPolicy
	ctrl           *gomock.Controller
	datastore      *mockstorage.MockStorage
	originResolver *mockblobclient.MockClusterResolver
	stats          tally.Scope
}

// mockController sets up all mocks and returns a teardown func that can be called with defer
func (m *testMocks) mockController(t gomock.TestReporter) func() {
	m.config = configFixture()
	m.policy = peerhandoutpolicy.DefaultPeerHandoutPolicyFixture()
	m.ctrl = gomock.NewController(t)
	m.datastore = mockstorage.NewMockStorage(m.ctrl)
	m.originResolver = mockblobclient.NewMockClusterResolver(m.ctrl)
	m.stats = tally.NewTestScope("testing", nil)
	return m.ctrl.Finish
}

func (m *testMocks) Handler() http.Handler {
	return Handler(
		m.config,
		m.stats,
		m.policy,
		m.datastore,
		m.datastore,
		m.datastore,
		m.originResolver,
	)
}

func (m *testMocks) CreateHandlerAndServeRequest(request *http.Request) *http.Response {
	w := httptest.NewRecorder()
	m.Handler().ServeHTTP(w, request)
	return w.Result()
}

func performRequest(handler http.Handler, request *http.Request) *http.Response {
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, request)
	return w.Result()
}

func createAnnouncePath(mi *torlib.MetaInfo, p *torlib.PeerInfo) string {
	v := url.Values{}
	v.Set("name", mi.Info.Name)
	v.Set("info_hash", mi.InfoHash.HexString())
	v.Set("peer_id", p.PeerID)
	v.Set("ip", p.IP)
	v.Set("port", strconv.FormatInt(p.Port, 10))
	v.Set("dc", p.DC)
	v.Set("downloaded", strconv.FormatInt(p.BytesDownloaded, 10))
	v.Set("uploaded", strconv.FormatInt(p.BytesUploaded, 10))
	v.Set("left", strconv.FormatInt(p.BytesLeft, 10))
	v.Set("event", p.Event)

	return "/announce?" + v.Encode()
}

func configFixture() Config {
	return Config{
		AnnounceInterval: 5 * time.Second,
	}
}
