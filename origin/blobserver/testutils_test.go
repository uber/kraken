package blobserver

import (
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/golang/mock/gomock"

	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/mocks/lib/store"
	"code.uber.internal/infra/kraken/mocks/origin/client"
	"code.uber.internal/infra/kraken/origin/client"
)

const (
	emptyDigestHex = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	randomUUID     = "b9cb2c15-3cb5-46bf-a63c-26b0c5b9bc24"
	testMaster     = "dummy-origin-master01-dca1"
)

func configFixture() Config {
	return Config{
		NumReplica: 2,
		HashNodes: map[string]HashNodeConfig{
			"dummy-origin-master01-dca1": {Label: "origin1", Weight: 100},
			"dummy-origin-master02-dca1": {Label: "origin2", Weight: 100},
			"dummy-origin-master03-dca1": {Label: "origin3", Weight: 100},
		},
		Repair: RepairConfig{
			NumWorkers: 10,
			NumRetries: 3,
			RetryDelay: 200 * time.Millisecond,
		},
	}
}

func configNoRedirectFixture() Config {
	c := configFixture()
	c.NumReplica = 3
	return c
}

type serverMocks struct {
	ctrl           *gomock.Controller
	fileStore      *mockstore.MockFileStore
	blobTransferer *mockclient.MockBlobTransferer
}

func newServerMocks(t *testing.T) *serverMocks {
	ctrl := gomock.NewController(t)
	return &serverMocks{
		ctrl:           ctrl,
		fileStore:      mockstore.NewMockFileStore(ctrl),
		blobTransferer: mockclient.NewMockBlobTransferer(ctrl),
	}
}

func testServer(config Config, mocks *serverMocks) (addr string, stop func()) {
	s, err := New(
		config,
		testMaster,
		mocks.fileStore,
		func(string, store.FileStore) client.BlobTransferer { return mocks.blobTransferer })
	if err != nil {
		panic(err)
	}
	l, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		panic(err)
	}
	hs := &http.Server{Handler: s.Handler()}
	go hs.Serve(l)
	return l.Addr().String(), func() { hs.Close() }
}
