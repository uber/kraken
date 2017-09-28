package client

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"

	"github.com/pressly/chi"

	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/testutils"
)

const (
	_uploadLocation = "mockuploadlocation"
	_digestHex      = "09b4be55821450cbf046f7ed71c7a1d9512b442c7967004651f7bff084a285c1"
)

// BlobAPIClientFixture returns a BlobAPIClient object and a cleanup function
func BlobAPIClientFixture() (*BlobAPIClient, func()) {
	cleanup := &testutils.Cleanup{}
	defer cleanup.Recover()

	s, storeCleanup := store.LocalStoreFixture()
	cleanup.Add(storeCleanup)
	trackerAddr, trackerClose := mockTracker()
	cleanup.Add(trackerClose)
	originAddr, originClose := mockOrigin()
	cleanup.Add(originClose)

	config := &Config{
		RequestTimeout: defaultTimeout,
		ChunkSize:      int64(defaultChunkSize),
		TrackerAddr:    trackerAddr,
		OriginAddr:     originAddr,
	}

	cli := NewBlobAPIClient(config, s)

	return cli, cleanup.Run
}

// TODO: create fixtures for origin and tracker
func mockTracker() (string, func()) {
	r := chi.NewRouter()
	r.Get("/manifest/:name", getManifestHandler)
	r.Post("/manifest/:name", postManifestHandler)

	s := httptest.NewServer(r)

	stop := s.Close

	return strings.TrimLeft(s.URL, "http://"), stop
}

func mockOrigin() (string, func()) {
	r := chi.NewRouter()
	r.Get("/blobs/:extra", getBlobHandler)
	r.Head("/blobs/:extra", headBlobHandler)
	r.Post("/blobs/:extra", postBlobHandler)
	r.Patch(fmt.Sprintf("/%s:extra", _uploadLocation), patchBlobHandler)
	r.Put(fmt.Sprintf("/%s:extra", _uploadLocation), putBlobHandler)

	s := httptest.NewServer(r)

	stop := s.Close

	return strings.TrimLeft(s.URL, "http://"), stop
}

func getManifestHandler(w http.ResponseWriter, r *http.Request) {
	data, err := ioutil.ReadFile("../test/testmanifest.json")
	if err != nil {
		log.Fatal(err)
	}

	w.WriteHeader(http.StatusOK)
	io.WriteString(w, string(data))
}

func postManifestHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func getBlobHandler(w http.ResponseWriter, r *http.Request) {
	data, err := ioutil.ReadFile("../test/testmanifest.json")
	if err != nil {
		log.Fatal(err)
	}

	w.WriteHeader(http.StatusOK)
	io.WriteString(w, string(data))
}

func headBlobHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNotFound)
}

func postBlobHandler(w http.ResponseWriter, r *http.Request) {
	uploadBlobHelper(w, r, http.StatusAccepted)
}

func patchBlobHandler(w http.ResponseWriter, r *http.Request) {
	uploadBlobHelper(w, r, http.StatusAccepted)
}

func putBlobHandler(w http.ResponseWriter, r *http.Request) {
	uploadBlobHelper(w, r, http.StatusCreated)
}

func uploadBlobHelper(w http.ResponseWriter, r *http.Request, code int) {
	w.Header().Set("Location", _uploadLocation)
	w.WriteHeader(code)
}
