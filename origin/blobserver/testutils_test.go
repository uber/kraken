package blobserver

import (
	"bytes"
	"testing"

	"go.uber.org/zap"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/backend"
	"code.uber.internal/infra/kraken/lib/blobrefresh"
	"code.uber.internal/infra/kraken/lib/hashring"
	"code.uber.internal/infra/kraken/lib/healthcheck"
	"code.uber.internal/infra/kraken/lib/hostlist"
	"code.uber.internal/infra/kraken/lib/metainfogen"
	"code.uber.internal/infra/kraken/lib/persistedretry"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/mocks/lib/backend"
	"code.uber.internal/infra/kraken/mocks/lib/persistedretry"
	"code.uber.internal/infra/kraken/mocks/origin/blobclient"
	"code.uber.internal/infra/kraken/origin/blobclient"
	"code.uber.internal/infra/kraken/utils/log"
	"code.uber.internal/infra/kraken/utils/stringset"
	"code.uber.internal/infra/kraken/utils/testutil"
)

const (
	master1 = "dummy-origin-master01-dca1:80"
	master2 = "dummy-origin-master02-dca1:80"
	master3 = "dummy-origin-master03-dca1:80"
)

func init() {
	zapConfig := zap.NewProductionConfig()
	zapConfig.OutputPaths = []string{}
	log.ConfigureLogger(zapConfig)
}

func newHashRing(maxReplica int) hashring.Ring {
	return hashring.New(
		hashring.Config{MaxReplica: maxReplica},
		hostlist.Fixture(master1, master2, master3),
		healthcheck.IdentityFilter{})
}

func hashRingNoReplica() hashring.Ring   { return newHashRing(1) }
func hashRingSomeReplica() hashring.Ring { return newHashRing(2) }
func hashRingMaxReplica() hashring.Ring  { return newHashRing(3) }

// testClientProvider implements blobclient.ClientProvider. It maps origin hostnames to
// the local addresses they are running on, such that Provide("dummy-origin")
// can resolve a real address.
type testClientProvider struct {
	clients map[string]blobclient.Client
}

func newTestClientProvider() *testClientProvider {
	return &testClientProvider{make(map[string]blobclient.Client)}
}

func (p *testClientProvider) register(host string, client blobclient.Client) {
	p.clients[host] = client
}

func (p *testClientProvider) Provide(host string) blobclient.Client {
	c, ok := p.clients[host]
	if !ok {
		log.Panicf("host %q not found", host)
	}
	return c
}

func startServer(
	host string,
	ring hashring.Ring,
	cas *store.CAStore,
	cp blobclient.Provider,
	pctx core.PeerContext,
	bm *backend.Manager,
	writeBackManager persistedretry.Manager) (addr string, stop func()) {

	mg := metainfogen.Fixture(cas, 4)

	br := blobrefresh.New(blobrefresh.Config{}, tally.NoopScope, cas, bm, mg)

	s, err := New(Config{}, tally.NoopScope, host, ring, cas, cp, pctx, bm, br, mg, writeBackManager)
	if err != nil {
		panic(err)
	}
	return testutil.StartServer(s.Handler())
}

// testServer is a convenience wrapper around the underlying components of a
// Server and faciliates restarting Servers with new configuration.
type testServer struct {
	ctrl             *gomock.Controller
	host             string
	addr             string
	cas              *store.CAStore
	cp               *testClientProvider
	pctx             core.PeerContext
	backendManager   *backend.Manager
	writeBackManager *mockpersistedretry.MockManager
	cleanup          func()
}

func newTestServer(
	t *testing.T, host string, ring hashring.Ring, cp *testClientProvider) *testServer {

	var cleanup testutil.Cleanup
	defer cleanup.Recover()

	ctrl := gomock.NewController(t)
	cleanup.Add(ctrl.Finish)

	pctx := core.PeerContextFixture()

	cas, c := store.CAStoreFixture()
	cleanup.Add(c)

	bm := backend.ManagerFixture()

	writeBackManager := mockpersistedretry.NewMockManager(ctrl)

	mg := metainfogen.Fixture(cas, 4)

	br := blobrefresh.New(blobrefresh.Config{}, tally.NoopScope, cas, bm, mg)

	s, err := New(Config{}, tally.NoopScope, host, ring, cas, cp, pctx, bm, br, mg, writeBackManager)
	if err != nil {
		panic(err)
	}

	addr, stop := testutil.StartServer(s.Handler())
	cleanup.Add(stop)

	cp.register(host, blobclient.NewWithConfig(addr, blobclient.Config{ChunkSize: 16}))

	return &testServer{
		ctrl:             ctrl,
		host:             host,
		addr:             addr,
		cas:              cas,
		cp:               cp,
		pctx:             pctx,
		backendManager:   bm,
		writeBackManager: writeBackManager,
		cleanup:          cleanup.Run,
	}
}

func (s *testServer) backendClient(namespace string) *mockbackend.MockClient {
	client := mockbackend.NewMockClient(s.ctrl)
	if err := s.backendManager.Register(namespace, client); err != nil {
		panic(err)
	}
	return client
}

func (s *testServer) remoteClient(name string) *mockblobclient.MockClient {
	client := mockblobclient.NewMockClient(s.ctrl)
	s.cp.register(name, client)
	return client
}

// computeBlobForHosts generates a random digest / content which shards to hosts.
func computeBlobForHosts(ring hashring.Ring, hosts ...string) *core.BlobFixture {
	want := stringset.New(hosts...)
	for {
		blob := core.SizedBlobFixture(32, 4)
		got := stringset.New(ring.Locations(blob.Digest)...)
		if stringset.Equal(want, got) {
			return blob
		}
	}
}

func ensureHasBlob(t *testing.T, c blobclient.Client, namespace string, blob *core.BlobFixture) {
	var buf bytes.Buffer
	require.NoError(t, c.DownloadBlob(namespace, blob.Digest, &buf))
	require.Equal(t, string(blob.Content), buf.String())
}
