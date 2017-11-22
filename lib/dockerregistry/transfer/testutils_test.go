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
	"code.uber.internal/infra/kraken/mocks/tracker/metainfoclient"
	"code.uber.internal/infra/kraken/origin/blobclient"
	"code.uber.internal/infra/kraken/utils/testutil"
)

func originClusterTransfererConfigFixture() OriginClusterTransfererConfig {
	return OriginClusterTransfererConfig{}.applyDefaults()
}

type originClusterTransfererMocks struct {
	ctrl           *gomock.Controller
	originResolver *mockblobclient.MockClusterResolver
	manifestClient *mockmanifestclient.MockClient
	metaInfoClient *mockmetainfoclient.MockClient
	fs             store.FileStore
}

func newOrginClusterTransfererMocks(t *testing.T) (*originClusterTransfererMocks, func()) {
	var cleanup testutil.Cleanup

	ctrl := gomock.NewController(t)
	cleanup.Add(ctrl.Finish)

	fs, c := store.LocalFileStoreFixture()
	cleanup.Add(c)

	return &originClusterTransfererMocks{
		ctrl:           ctrl,
		originResolver: mockblobclient.NewMockClusterResolver(ctrl),
		manifestClient: mockmanifestclient.NewMockClient(ctrl),
		metaInfoClient: mockmetainfoclient.NewMockClient(ctrl),
		fs:             fs,
	}, cleanup.Run
}

func (m *originClusterTransfererMocks) newTransferer() *OriginClusterTransferer {
	return NewOriginClusterTransferer(
		originClusterTransfererConfigFixture(),
		m.originResolver,
		m.manifestClient,
		m.metaInfoClient,
		m.fs)
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
