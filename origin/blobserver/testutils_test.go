package blobserver

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/andres-erbsen/clock"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/backend"
	"code.uber.internal/infra/kraken/lib/blobrefresh"
	"code.uber.internal/infra/kraken/lib/metainfogen"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/mocks/lib/store"
	"code.uber.internal/infra/kraken/origin/blobclient"
	"code.uber.internal/infra/kraken/utils/log"
	"code.uber.internal/infra/kraken/utils/randutil"
	"code.uber.internal/infra/kraken/utils/stringset"
	"code.uber.internal/infra/kraken/utils/testutil"
)

const (
	master1 = "dummy-origin-master01-dca1"
	master2 = "dummy-origin-master02-dca1"
	master3 = "dummy-origin-master03-dca1"
)

const namespace = "test-namespace"

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
		Repair: RepairConfig{
			NumWorkers: 10,
			MaxRetries: 3,
			RetryDelay: 200 * time.Millisecond,
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
	fs store.OriginFileStore,
	cp blobclient.Provider,
	pctx core.PeerContext,
	bm *backend.Manager) (addr string, stop func()) {

	mg := metainfogen.Fixture(fs, 4)

	br := blobrefresh.New(tally.NoopScope, fs, bm, mg)

	s, err := New(config, tally.NoopScope, host, fs, cp, pctx, bm, br, mg)
	if err != nil {
		panic(err)
	}
	return testutil.StartServer(s.Handler())
}

// testServer is a convenience wrapper around the underlying components of a
// Server and faciliates restarting Servers with new configuration.
type testServer struct {
	host           string
	addr           string
	fs             store.OriginFileStore
	cp             *testClientProvider
	pctx           core.PeerContext
	backendManager *backend.Manager
	stop           func()
	cleanFS        func()
}

func newTestServer(host string, config Config, cp *testClientProvider) *testServer {
	pctx := core.PeerContextFixture()
	fs, cleanFS := store.OriginFileStoreFixture(clock.New())
	bm := backend.ManagerFixture()
	addr, stop := startServer(host, config, fs, cp, pctx, bm)
	cp.register(host, blobclient.NewWithConfig(addr, blobclient.Config{ChunkSize: 16}))
	return &testServer{
		host:           host,
		addr:           addr,
		fs:             fs,
		cp:             cp,
		pctx:           pctx,
		backendManager: bm,
		stop:           stop,
		cleanFS:        cleanFS,
	}
}

func (s *testServer) restart(config Config) {
	s.stop()

	s.addr, s.stop = startServer(s.host, config, s.fs, s.cp, s.pctx, s.backendManager)
	s.cp.register(s.host, blobclient.NewWithConfig(s.addr, blobclient.Config{ChunkSize: 16}))
}

func (s *testServer) cleanup() {
	s.stop()
	s.cleanFS()
}

// serverMocks is a convenience wrapper around a completely mocked Server.
type serverMocks struct {
	ctrl           *gomock.Controller
	fileStore      *mockstore.MockOriginFileStore
	clientProvider blobclient.Provider
}

func newServerMocks(t *testing.T) *serverMocks {
	ctrl := gomock.NewController(t)
	return &serverMocks{
		ctrl:      ctrl,
		fileStore: mockstore.NewMockOriginFileStore(ctrl),
		// TODO(codyg): Support mock client providers.
		clientProvider: nil,
	}
}

func (mocks *serverMocks) server(config Config) (addr string, stop func()) {
	return startServer(master1, config, mocks.fileStore, mocks.clientProvider, core.PeerContextFixture(), nil)
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

// pickShard generates a shard that is owned by hosts, as specified by config.
func pickShard(config Config, hosts ...string) string {
	for tries := 0; tries < 1000; tries++ {
		shardID := randutil.Hex(4)
		if hostsOwnShard(config, shardID, hosts...) {
			return shardID
		}
	}
	panic(fmt.Sprintf("cannot find shard for hosts %v", hosts))
}

// computeBlobForShard generates a random digest / content which matches shardID.
// XXX This function is not cheap! Each call takes around 0.1 seconds.
func computeBlobForShard(shardID string) *core.BlobFixture {
	buf := make([]byte, 32)
	for {
		if _, err := rand.Read(buf); err != nil {
			panic(err)
		}
		d, err := core.NewDigester().FromBytes(buf)
		if err != nil {
			panic(err)
		}
		if d.ShardID() != shardID {
			continue
		}
		mi, err := core.NewMetaInfoFromBlob(d.Hex(), bytes.NewReader(buf), 1)
		if err != nil {
			panic(err)
		}
		return core.CustomBlobFixture(buf, d, mi)
	}
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

func ensureHasBlob(t *testing.T, c blobclient.Client, blob *core.BlobFixture) {
	var buf bytes.Buffer
	require.NoError(t, c.DownloadBlob(namespace, blob.Digest, &buf))
	require.Equal(t, string(blob.Content), buf.String())
}
