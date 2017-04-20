package service

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"testing"

	"code.uber.internal/infra/kraken/config/tracker"
	"code.uber.internal/infra/kraken/kraken/tracker/storage"
	"code.uber.internal/infra/kraken/test/mocks/mock_storage"
	"github.com/golang/mock/gomock"
	bencode "github.com/jackpal/bencode-go"
	"github.com/stretchr/testify/assert"
)

type testMocks struct {
	appCfg    config.AppConfig
	ctrl      *gomock.Controller
	datastore *mock_storage.MockStorage
}

// mockController sets up all mocks and returns a teardown func that can be called with defer
func (m *testMocks) mockController(t gomock.TestReporter) func() {
	m.appCfg = config.AppConfig{}
	m.ctrl = gomock.NewController(t)
	m.datastore = mock_storage.NewMockStorage(m.ctrl)
	return func() {
		m.ctrl.Finish()
	}
}

func (m *testMocks) CreateHandler() http.Handler {
	return InitializeAPI(
		m.appCfg,
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

func TestAnnounceEndPoint(t *testing.T) {
	infoHash := "12345678901234567890"
	rawinfoHash, _ := hex.DecodeString(infoHash)
	peerID := "09876543210987654321"
	rawpeerID, _ := hex.DecodeString(peerID)
	portStr := "6881"
	ip := "255.255.255.255"
	downloaded := "1234"
	uploaded := "5678"
	left := "910"
	event := "stopped"

	port, _ := strconv.ParseInt(portStr, 10, 64)
	bytesUploaded, _ := strconv.ParseInt(uploaded, 10, 64)
	bytesDownloaded, _ := strconv.ParseInt(downloaded, 10, 64)
	bytesLeft, _ := strconv.ParseInt(left, 10, 64)

	t.Run("Return 500 if missing parameters", func(t *testing.T) {
		announceRequest, _ := http.NewRequest("GET", "/announce", nil)
		announceRequest.Host = fmt.Sprintf("%s:%s", ip, portStr)

		mocks := &testMocks{}
		defer mocks.mockController(t)()

		response := mocks.CreateHandlerAndServeRequest(announceRequest)
		assert.Equal(t, 500, response.StatusCode)
	})
	t.Run("Return 200 and empty bencoded response", func(t *testing.T) {

		announceRequest, _ := http.NewRequest("GET",
			"/announce?info_hash="+string(rawinfoHash[:])+
				"&peer_id="+string(rawpeerID[:])+
				"&port="+portStr+
				"&downloaded="+downloaded+
				"&uploaded="+uploaded+
				"&left="+left+
				"&event="+event, nil)
		announceRequest.Host = fmt.Sprintf("%s:%s", ip, portStr)

		mocks := &testMocks{}
		defer mocks.mockController(t)()

		mocks.datastore.EXPECT().Read(infoHash).Return([]storage.PeerInfo{}, nil)
		mocks.datastore.EXPECT().Update(
			&storage.PeerInfo{
				InfoHash:        infoHash,
				PeerID:          peerID,
				IP:              ip,
				Port:            port,
				BytesUploaded:   bytesUploaded,
				BytesDownloaded: bytesDownloaded,
				BytesLeft:       bytesLeft,
				Event:           event,
				Flags:           0}).Return(nil)
		response := mocks.CreateHandlerAndServeRequest(announceRequest)
		announceResponse := AnnouncerResponse{}
		bencode.Unmarshal(response.Body, &announceResponse)
		assert.Equal(t, announceResponse.Interval, int64(0))
		assert.Equal(t, announceResponse.Peers, []storage.PeerInfo{})
		assert.Equal(t, 200, response.StatusCode)
	})
	t.Run("Return 200 and single peer bencoded response", func(t *testing.T) {

		announceRequest, _ := http.NewRequest("GET",
			"/announce?info_hash="+string(rawinfoHash[:])+
				"&peer_id="+string(rawpeerID[:])+
				"&port="+portStr+
				"&downloaded="+downloaded+
				"&uploaded="+uploaded+
				"&left="+left+
				"&event="+event, nil)
		announceRequest.Host = fmt.Sprintf("%s:%s", ip, portStr)

		mocks := &testMocks{}
		defer mocks.mockController(t)()

		peerFrom := storage.PeerInfo{
			InfoHash:        infoHash,
			PeerID:          peerID,
			IP:              ip,
			Port:            port,
			BytesUploaded:   bytesUploaded,
			BytesDownloaded: bytesDownloaded,
			BytesLeft:       bytesLeft,
			Event:           event,
			Flags:           0}

		peerTo := storage.PeerInfo{
			PeerID: peerID,
			IP:     ip,
			Port:   port}

		mocks.datastore.EXPECT().Read(infoHash).Return([]storage.PeerInfo{peerFrom}, nil)
		mocks.datastore.EXPECT().Update(&peerFrom).Return(nil)
		response := mocks.CreateHandlerAndServeRequest(announceRequest)
		announceResponse := AnnouncerResponse{}
		bencode.Unmarshal(response.Body, &announceResponse)
		assert.Equal(t, announceResponse.Interval, int64(0))
		assert.Equal(t, announceResponse.Peers, []storage.PeerInfo{peerTo})
		assert.Equal(t, 200, response.StatusCode)
	})

}

func TestGetInfoHashHandler(t *testing.T) {
	infoHash := "12345678901234567890"
	name := "asdfhjkl"

	t.Run("Return 400 on empty name", func(t *testing.T) {
		getRequest, _ := http.NewRequest("GET",
			"/infohash?name=", nil)

		mocks := &testMocks{}
		defer mocks.mockController(t)()
		getResponse := mocks.CreateHandlerAndServeRequest(getRequest)
		assert.Equal(t, 400, getResponse.StatusCode)
	})

	t.Run("Return 404 on name not found", func(t *testing.T) {
		getRequest, _ := http.NewRequest("GET",
			"/infohash?name="+name, nil)

		mocks := &testMocks{}
		defer mocks.mockController(t)()

		mocks.datastore.EXPECT().ReadTorrent(name).Return(nil, nil)
		response := mocks.CreateHandlerAndServeRequest(getRequest)
		assert.Equal(t, 404, response.StatusCode)
	})

	t.Run("Return 200 and info hash", func(t *testing.T) {
		getRequest, _ := http.NewRequest("GET",
			"/infohash?name="+name, nil)

		mocks := &testMocks{}
		defer mocks.mockController(t)()

		mocks.datastore.EXPECT().ReadTorrent(name).Return(&storage.TorrentInfo{InfoHash: infoHash}, nil)
		response := mocks.CreateHandlerAndServeRequest(getRequest)
		assert.Equal(t, 200, response.StatusCode)
		data := make([]byte, len(infoHash))
		response.Body.Read(data)
		assert.Equal(t, infoHash, string(data[:]))
	})
}

func TestPostInfoHashHandler(t *testing.T) {
	infoHash := "12345678901234567890"
	name := "asdfhjkl"

	t.Run("Return 400 on empty name or infohash", func(t *testing.T) {
		getRequest, _ := http.NewRequest("POST",
			"/infohash?name=", nil)

		mocks := &testMocks{}
		defer mocks.mockController(t)()
		getResponse := mocks.CreateHandlerAndServeRequest(getRequest)
		assert.Equal(t, 400, getResponse.StatusCode)

		getRequest, _ = http.NewRequest("POST",
			"/infohash?name="+name, nil)
		getResponse = mocks.CreateHandlerAndServeRequest(getRequest)
		assert.Equal(t, 400, getResponse.StatusCode)
	})

	t.Run("Return 200", func(t *testing.T) {
		getRequest, _ := http.NewRequest("POST",
			"/infohash?name="+name+"&info_hash="+infoHash, nil)

		mocks := &testMocks{}
		defer mocks.mockController(t)()

		mocks.datastore.EXPECT().CreateTorrent(&storage.TorrentInfo{
			TorrentName: name,
			InfoHash:    infoHash,
		}).Return(nil)
		response := mocks.CreateHandlerAndServeRequest(getRequest)
		assert.Equal(t, 200, response.StatusCode)
	})
}

func TestPostManifestHandler(t *testing.T) {
	manifest := `{
                 "schemaVersion": 2,
                 "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
                 "config": {
                    "mediaType": "application/octet-stream",
                    "size": 11936,
                    "digest": "sha256:d2176faa6180566e5e6727e101ba26b13c19ef35f171c9b4419c4d50626aad9d"
                 },
                 "layers": [{
                    "mediaType": "application/vnd.docker.image.rootfs.diff.tar.gzip",
                    "size": 52998821,
                    "digest": "sha256:1508613826413590a9fdb496cbedb0c2ebf564cfbcd2c85c2a07bb3a40813233"
                 },
                 {
                    "mediaType": "application/vnd.docker.image.rootfs.diff.tar.gzip",
                    "size": 115242848,
                    "digest": "sha256:f1f1d5da237f1b069eae23cdc9b291e217a4c1fda8f29262c4275a786a4dd322"
                  }]}`
	name := "tag1"

	t.Run("Return 400 on invalid manifest", func(t *testing.T) {
		getRequest, _ := http.NewRequest("POST",
			"/manifest/"+name, bytes.NewBuffer([]byte("")))

		mocks := &testMocks{}
		defer mocks.mockController(t)()
		getResponse := mocks.CreateHandlerAndServeRequest(getRequest)
		assert.Equal(t, 400, getResponse.StatusCode)
	})

	t.Run("Return 200", func(t *testing.T) {
		getRequest, _ := http.NewRequest("POST",
			"/manifest/"+name, bytes.NewBuffer([]byte(manifest)))

		mocks := &testMocks{}
		defer mocks.mockController(t)()

		mocks.datastore.EXPECT().UpdateManifest(&storage.Manifest{
			TagName:  name,
			Manifest: manifest,
			Flags:    0,
		}).Return(nil)
		response := mocks.CreateHandlerAndServeRequest(getRequest)
		assert.Equal(t, 200, response.StatusCode)
	})
}

// JSONBytesEqual compares the JSON in two byte slices.
func JSONBytesEqual(a, b []byte) (bool, error) {
	var j1, j2 interface{}
	if err := json.Unmarshal(a, &j1); err != nil {
		return false, err
	}
	if err := json.Unmarshal(b, &j2); err != nil {
		return false, err
	}
	return reflect.DeepEqual(j2, j1), nil
}

func TestGetManifestHandler(t *testing.T) {
	manifest := `{
                 "schemaVersion": 2,
                 "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
                 "config": {
                    "mediaType": "application/octet-stream",
                    "size": 11936,
                    "digest": "sha256:d2176faa6180566e5e6727e101ba26b13c19ef35f171c9b4419c4d50626aad9d"
                 },
                 "layers": [{
                    "mediaType": "application/vnd.docker.image.rootfs.diff.tar.gzip",
                    "size": 52998821,
                    "digest": "sha256:1508613826413590a9fdb496cbedb0c2ebf564cfbcd2c85c2a07bb3a40813233"
                 },
                 {
                    "mediaType": "application/vnd.docker.image.rootfs.diff.tar.gzip",
                    "size": 115242848,
                    "digest": "sha256:f1f1d5da237f1b069eae23cdc9b291e217a4c1fda8f29262c4275a786a4dd322"
                  }]}`
	name := "tag1"

	t.Run("Return 400 on empty tag name", func(t *testing.T) {
		getRequest, _ := http.NewRequest("GET",
			"/manifest/", nil)

		mocks := &testMocks{}
		defer mocks.mockController(t)()
		getResponse := mocks.CreateHandlerAndServeRequest(getRequest)
		assert.Equal(t, 400, getResponse.StatusCode)
	})

	t.Run("Return 404 on manifest not found", func(t *testing.T) {
		getRequest, _ := http.NewRequest("GET",
			"/manifest/"+name, nil)

		mocks := &testMocks{}
		defer mocks.mockController(t)()

		mocks.datastore.EXPECT().ReadManifest(name).Return(nil, nil)
		response := mocks.CreateHandlerAndServeRequest(getRequest)
		assert.Equal(t, 404, response.StatusCode)
	})

	t.Run("Return 200 and manifest", func(t *testing.T) {
		getRequest, _ := http.NewRequest("GET",
			"/manifest/"+name, nil)

		mocks := &testMocks{}
		defer mocks.mockController(t)()

		mocks.datastore.EXPECT().ReadManifest(name).Return(&storage.Manifest{TagName: name, Manifest: manifest}, nil)
		response := mocks.CreateHandlerAndServeRequest(getRequest)
		assert.Equal(t, 200, response.StatusCode)
		data, _ := ioutil.ReadAll(response.Body)
		var j1, j2 interface{}
		j1, err := json.Marshal(data)
		assert.Equal(t, err, nil)
		err = json.Unmarshal([]byte(manifest), &j2)
		assert.Equal(t, err, nil)

		result := reflect.DeepEqual(j1, j1)
		assert.Equal(t, result, true)
	})
}
