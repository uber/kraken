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
	master1 = "dummy-origin-master01-dca1"
	master2 = "dummy-origin-master02-dca1"
	master3 = "dummy-origin-master03-dca1"
)

func init() {
	zapConfig := zap.NewProductionConfig()
	zapConfig.OutputPaths = []string{}
	log.ConfigureLogger(zapConfig)
}

func configFixture() Config {
	return Config{
		NumReplica: 2,
		HashNodes: map[string]HashNodeConfig{
			master1: {Label: "origin1", Weight: 100},
			master2: {Label: "origin2", Weight: 100},
			master3: {Label: "origin3", Weight: 100},
		},
	}
}

// configMaxReplicaFixture returns a config that ensures all blobs are replicated
// to every master.
func configMaxReplicaFixture() Config {
	c := configFixture()
	c.NumReplica = 3
	return c
}

func configNoReplicaFixture() Config {
	c := configFixture()
	c.NumReplica = 1
	return c
}

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
	config Config,
	cas *store.CAStore,
	cp blobclient.Provider,
	pctx core.PeerContext,
	bm *backend.Manager,
	writeBackManager persistedretry.Manager) (addr string, stop func()) {

	mg := metainfogen.Fixture(cas, 4)

	br := blobrefresh.New(blobrefresh.Config{}, tally.NoopScope, cas, bm, mg)

	s, err := New(config, tally.NoopScope, host, cas, cp, pctx, bm, br, mg, writeBackManager)
	if err != nil {
		panic(err)
	}
	return testutil.StartServer(s.Handler())
}

// testServer is a convenience wrapper around the underlying components of a
// Server and faciliates restarting Servers with new configuration.
type testServer struct {
	ctrl             *gomock.Controller
	config           Config
	host             string
	addr             string
	cas              *store.CAStore
	cp               *testClientProvider
	pctx             core.PeerContext
	backendManager   *backend.Manager
	writeBackManager *mockpersistedretry.MockManager
	cleanup          func()
}

func newTestServer(t *testing.T, host string, config Config, cp *testClientProvider) *testServer {
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

	s, err := New(config, tally.NoopScope, host, cas, cp, pctx, bm, br, mg, writeBackManager)
	if err != nil {
		panic(err)
	}

	addr, stop := testutil.StartServer(s.Handler())
	cleanup.Add(stop)

	cp.register(host, blobclient.NewWithConfig(addr, blobclient.Config{ChunkSize: 16}))

	return &testServer{
		ctrl:             ctrl,
		config:           config,
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

// labelSet converts hosts into their corresponding labels as specified by config.
func labelSet(config Config, hosts []string) stringset.Set {
	s := make(stringset.Set)
	for _, host := range hosts {
		s.Add(config.HashNodes[host].Label)
	}
	return s
}

// hostsOwnShard returns true if shardID is owned by hosts.
func hostsOwnShard(config Config, shardID string, hosts ...string) bool {
	hashState := config.HashState()
	nodes, err := hashState.GetOrderedNodes(shardID, config.NumReplica)
	if err != nil {
		log.Panicf("failed to get nodes for shard %q: %s", shardID, err)
	}
	labels := make(stringset.Set)
	for _, node := range nodes {
		labels.Add(node.Label)
	}
	return stringset.Equal(labelSet(config, hosts), labels)
}

// computeBlobForHosts generates a random digest / content which shards to hosts.
func computeBlobForHosts(config Config, hosts ...string) *core.BlobFixture {
	for {
		blob := core.SizedBlobFixture(32, 4)
		if hostsOwnShard(config, blob.Digest.ShardID(), hosts...) {
			return blob
		}
	}
}

func ensureHasBlob(t *testing.T, c blobclient.Client, namespace string, blob *core.BlobFixture) {
	var buf bytes.Buffer
	require.NoError(t, c.DownloadBlob(namespace, blob.Digest, &buf))
	require.Equal(t, string(blob.Content), buf.String())
}
