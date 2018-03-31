package storage

import (
	"testing"

	"code.uber.internal/infra/kraken/lib/backend"
	"code.uber.internal/infra/kraken/lib/blobrefresh"
	"code.uber.internal/infra/kraken/lib/metainfogen"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/mocks/lib/backend"
	"code.uber.internal/infra/kraken/mocks/tracker/metainfoclient"
	"code.uber.internal/infra/kraken/utils/testutil"

	"github.com/andres-erbsen/clock"
	"github.com/golang/mock/gomock"
	"github.com/uber-go/tally"
)

const namespace = "test-namespace"

const pieceLength = 4

type agentMocks struct {
	fs             store.FileStore
	metaInfoClient *mockmetainfoclient.MockClient
}

func newAgentMocks(t *testing.T) (*agentMocks, func()) {
	var cleanup testutil.Cleanup

	ctrl := gomock.NewController(t)
	cleanup.Add(ctrl.Finish)

	fs, c := store.LocalFileStoreFixture()
	cleanup.Add(c)

	metaInfoClient := mockmetainfoclient.NewMockClient(ctrl)

	return &agentMocks{fs, metaInfoClient}, cleanup.Run
}

func (m *agentMocks) newTorrentArchive() *AgentTorrentArchive {
	return NewAgentTorrentArchive(tally.NoopScope, m.fs, m.metaInfoClient)
}

type originMocks struct {
	fs            store.OriginFileStore
	backendClient *mockbackend.MockClient
	blobRefresher *blobrefresh.Refresher
}

func newOriginMocks(t *testing.T) (*originMocks, func()) {
	var cleanup testutil.Cleanup
	defer cleanup.Recover()

	fs, c := store.OriginFileStoreFixture(clock.New())
	cleanup.Add(c)

	ctrl := gomock.NewController(t)
	cleanup.Add(ctrl.Finish)

	backendClient := mockbackend.NewMockClient(ctrl)

	backends, err := backend.NewManager(nil, nil)
	if err != nil {
		panic(err)
	}
	backends.Register(namespace, backendClient)

	blobRefresher := blobrefresh.New(
		tally.NoopScope, fs, backends, metainfogen.Fixture(fs, pieceLength))

	return &originMocks{fs, backendClient, blobRefresher}, cleanup.Run
}

func (m *originMocks) newTorrentArchive() *OriginTorrentArchive {
	return NewOriginTorrentArchive(m.fs, m.blobRefresher)
}
