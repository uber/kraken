package service

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strconv"

	config "code.uber.internal/infra/kraken/config/tracker"
	"code.uber.internal/infra/kraken/torlib"
	"code.uber.internal/infra/kraken/tracker/storage"

	"github.com/golang/mock/gomock"
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
	appCfg    config.AppConfig
	ctrl      *gomock.Controller
	datastore *storage.MockStorage
}

// mockController sets up all mocks and returns a teardown func that can be called with defer
func (m *testMocks) mockController(t gomock.TestReporter) func() {
	m.appCfg = config.AppConfig{
		PeerHandoutPolicy: config.PeerHandoutConfig{
			Priority: "default",
			Sampling: "default",
		},
	}
	m.ctrl = gomock.NewController(t)
	m.datastore = storage.NewMockStorage(m.ctrl)
	return func() {
		m.ctrl.Finish()
	}
}

func (m *testMocks) CreateHandler() http.Handler {
	return InitializeAPI(
		m.appCfg,
		m.datastore,
		m.datastore,
		m.datastore,
	)
}

func (m *testMocks) CreateHandlerAndServeRequest(request *http.Request) *http.Response {
	w := httptest.NewRecorder()
	m.CreateHandler().ServeHTTP(w, request)
	return w.Result()
}

func performRequest(handler http.Handler, request *http.Request) *http.Response {
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, request)
	return w.Result()
}

func createAnnouncePath(mi *torlib.MetaInfo, p *torlib.PeerInfo) string {
	v := url.Values{}
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
