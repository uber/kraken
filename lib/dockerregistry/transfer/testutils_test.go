package transfer

import (
	"io/ioutil"
	"log"
	"testing"

	"github.com/golang/mock/gomock"

	"code.uber.internal/infra/kraken/lib/dockerregistry/image"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/mocks/lib/dockerregistry/transfer"
	"code.uber.internal/infra/kraken/mocks/lib/store"
	"code.uber.internal/infra/kraken/mocks/origin/blobserver"
)

const _mockOriginDNS string = "mockOriginDns"

type originClusterTransfererMocks struct {
	originDNS          string
	ctrl               *gomock.Controller
	fileStore          *mockstore.MockFileStore
	blobClientProvider *mockblobserver.MockClientProvider
	blobClients        map[string]*mockblobserver.MockClient
	manifestClient     *mocktransferer.MockManifestClient
}

func newOrginClusterTransfererMocks(t *testing.T, originAddrs ...string) *originClusterTransfererMocks {
	ctrl := gomock.NewController(t)
	m := make(map[string]*mockblobserver.MockClient)
	for _, addr := range originAddrs {
		m[addr] = mockblobserver.NewMockClient(ctrl)
	}
	m[_mockOriginDNS] = mockblobserver.NewMockClient(ctrl)

	return &originClusterTransfererMocks{
		originDNS:          _mockOriginDNS,
		ctrl:               ctrl,
		fileStore:          mockstore.NewMockFileStore(ctrl),
		blobClientProvider: mockblobserver.NewMockClientProvider(ctrl),
		blobClients:        m,
		manifestClient:     mocktransferer.NewMockManifestClient(ctrl),
	}
}

func testOriginClusterTransferer(mocks *originClusterTransfererMocks) *OriginClusterTransferer {
	return &OriginClusterTransferer{
		originAddr:         mocks.originDNS,
		store:              mocks.fileStore,
		blobClientProvider: mocks.blobClientProvider,
		manifestClient:     mocks.manifestClient,
		concurrency:        1,
		numWorkers:         make(chan struct{}, 1),
	}
}

func mockManifestReadWriter() (rw *store.MockFileReadWriter, digest image.Digest, cleanup func()) {
	data, err := ioutil.ReadFile("../test/testmanifest.json")
	if err != nil {
		log.Panic(err)
	}
	digest, err = image.NewDigester().FromBytes(data)
	if err != nil {
		log.Panic(err)
	}

	f, cleanup := store.NewMockFileReadWriter(data)
	return f, digest, cleanup
}
