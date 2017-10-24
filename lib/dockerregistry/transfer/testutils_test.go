package transfer

import (
	"io/ioutil"
	"log"
	"testing"

	"github.com/golang/mock/gomock"

	"code.uber.internal/infra/kraken/lib/dockerregistry/image"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/mocks/lib/dockerregistry/transfer/manifestclient"
	"code.uber.internal/infra/kraken/mocks/origin/blobclient"
	"code.uber.internal/infra/kraken/origin/blobclient"
)

type originClusterTransfererMocks struct {
	ctrl           *gomock.Controller
	originResolver *mockblobclient.MockClusterResolver
	manifestClient *mockmanifestclient.MockClient
}

func newOrginClusterTransfererMocks(t *testing.T) *originClusterTransfererMocks {
	ctrl := gomock.NewController(t)
	return &originClusterTransfererMocks{
		ctrl:           ctrl,
		originResolver: mockblobclient.NewMockClusterResolver(ctrl),
		manifestClient: mockmanifestclient.NewMockClient(ctrl),
	}
}

func (m *originClusterTransfererMocks) newTransferer() *OriginClusterTransferer {
	return NewOriginClusterTransferer(1, m.originResolver, m.manifestClient)
}

func (m *originClusterTransfererMocks) expectClients(d image.Digest, locs ...string) []*mockblobclient.MockClient {
	var mockClients []*mockblobclient.MockClient
	var clients []blobclient.Client
	for _, loc := range locs {
		c := mockblobclient.NewMockClient(m.ctrl)
		c.EXPECT().Addr().Return(loc).AnyTimes()
		mockClients = append(mockClients, c)
		clients = append(clients, c)
	}
	m.originResolver.EXPECT().Resolve(d).Return(clients, nil).MinTimes(1)
	return mockClients
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
