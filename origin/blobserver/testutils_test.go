package blobserver

import (
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"

	"code.uber.internal/infra/kraken/lib/dockerregistry/image"
	"code.uber.internal/infra/kraken/lib/peercontext"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/lib/torrent"
	"code.uber.internal/infra/kraken/mocks/lib/store"
	"code.uber.internal/infra/kraken/origin/blobclient"
	"code.uber.internal/infra/kraken/utils/randutil"
	"code.uber.internal/infra/kraken/utils/stringset"
)

const (
	master1 = "dummy-origin-master01-dca1"
	master2 = "dummy-origin-master02-dca1"
	master3 = "dummy-origin-master03-dca1"
)

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

func configNoRedirectFixture() Config {
	c := configFixture()
	c.NumReplica = 3
	return c
}

func clientConfigFixture() blobclient.Config {
	return blobclient.Config{
		UploadChunkSize: 16,
	}
}

// testClientProvider implements blobclient.ClientProvider. It maps origin hostnames to
// the local addresses they are running on, such that Provide("dummy-origin")
// can resolve a real address.
type testClientProvider struct {
	config         blobclient.Config
	addrByHostname map[string]string
}

func newTestClientProvider(config blobclient.Config) *testClientProvider {
	return &testClientProvider{config, make(map[string]string)}
}

func (p *testClientProvider) register(host string, addr string) {
	p.addrByHostname[host] = addr
}

func (p *testClientProvider) Provide(host string) blobclient.Client {
	addr, ok := p.addrByHostname[host]
	if !ok {
		log.Panicf("host %q not found", host)
	}
	return blobclient.New(p.config, addr)
}

func startServer(
	host string,
	config Config,
	fs store.FileStore,
	cp blobclient.Provider,
	pctx peercontext.PeerContext) (addr string, stop func()) {

	stats := tally.NewTestScope("", nil)

	var torrentConfig torrent.Config
	torrentConfig.Disabled = true
	s, err := New(config, torrentConfig, stats, host, fs, cp, nil)
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

// testServer is a convenience wrapper around the underlying components of a
// Server and faciliates restarting Servers with new configuration.
type testServer struct {
	host    string
	addr    string
	fs      *store.LocalFileStore
	cp      *testClientProvider
	pctx    peercontext.PeerContext
	stop    func()
	cleanFS func()
}

func newTestServer(host string, config Config, cp *testClientProvider) *testServer {
	pctx := peercontext.Fixture()
	fs, cleanFS := store.LocalFileStoreFixture()
	addr, stop := startServer(host, config, fs, cp, pctx)
	cp.register(host, addr)
	return &testServer{
		host:    host,
		addr:    addr,
		fs:      fs,
		cp:      cp,
		pctx:    pctx,
		stop:    stop,
		cleanFS: cleanFS,
	}
}

func (s *testServer) restart(config Config) {
	s.stop()

	s.addr, s.stop = startServer(s.host, config, s.fs, s.cp, s.pctx)
	s.cp.register(s.host, s.addr)
}

func (s *testServer) cleanup() {
	s.stop()
	s.cleanFS()
}

// serverMocks is a convenience wrapper around a completely mocked Server.
type serverMocks struct {
	ctrl           *gomock.Controller
	fileStore      *mockstore.MockFileStore
	clientProvider blobclient.Provider
}

func newServerMocks(t *testing.T) *serverMocks {
	ctrl := gomock.NewController(t)
	return &serverMocks{
		ctrl:      ctrl,
		fileStore: mockstore.NewMockFileStore(ctrl),
		// TODO(codyg): Support mock client providers.
		clientProvider: nil,
	}
}

func (mocks *serverMocks) server(config Config) (addr string, stop func()) {
	return startServer(master1, config, mocks.fileStore, mocks.clientProvider, peercontext.Fixture())
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

func blobFixture(size int64) (image.Digest, []byte) {
	b := make([]byte, size)
	_, err := rand.Read(b)
	if err != nil {
		panic(err)
	}
	d, err := image.NewDigester().FromBytes(b)
	if err != nil {
		panic(err)
	}
	return d, b
}

// computeBlobForShard generates a random digest / content which matches shardID.
// XXX This function is not cheap! Each call takes around 0.1 seconds.
func computeBlobForShard(shardID string) (image.Digest, []byte) {
	b := make([]byte, 64)
	for {
		_, err := rand.Read(b)
		if err != nil {
			panic(err)
		}
		d, err := image.NewDigester().FromBytes(b)
		if err != nil {
			panic(err)
		}
		if d.ShardID() == shardID {
			return d, b
		}
	}
}

// computeBlobForHosts generates a random digest / content which shards to hosts.
func computeBlobForHosts(config Config, hosts ...string) (image.Digest, []byte) {
	b := make([]byte, 64)
	for {
		_, err := rand.Read(b)
		if err != nil {
			panic(err)
		}
		d, err := image.NewDigester().FromBytes(b)
		if err != nil {
			panic(err)
		}
		if hostsOwnShard(config, d.ShardID(), hosts...) {
			return d, b
		}
	}
}

func ensureHasBlob(t *testing.T, fs store.FileStore, d image.Digest, expected []byte) {
	require := require.New(t)

	f, err := fs.GetCacheFileReader(d.Hex())
	require.NoError(err)
	content, err := ioutil.ReadAll(f)
	require.NoError(err)
	require.Equal(expected, content)
}
