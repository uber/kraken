// Copyright (c) 2016-2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package blobserver

import (
	"bytes"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/andres-erbsen/clock"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/backend"
	"github.com/uber/kraken/lib/blobrefresh"
	"github.com/uber/kraken/lib/hashring"
	"github.com/uber/kraken/lib/healthcheck"
	"github.com/uber/kraken/lib/hostlist"
	"github.com/uber/kraken/lib/metainfogen"
	"github.com/uber/kraken/lib/store"
	"github.com/uber/kraken/mocks/lib/backend"
	"github.com/uber/kraken/mocks/lib/persistedretry"
	"github.com/uber/kraken/mocks/origin/blobclient"
	"github.com/uber/kraken/origin/blobclient"
	"github.com/uber/kraken/utils/log"
	"github.com/uber/kraken/utils/stringset"
	"github.com/uber/kraken/utils/testutil"
)

const (
	master1 = "dummy-origin-master01-zone2:80"
	master2 = "dummy-origin-master02-zone2:80"
	master3 = "dummy-origin-master03-zone2:80"
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

// testServer is a convenience wrapper around the underlying components of a
// Server and faciliates restarting Servers with new configuration.
type testServer struct {
	ctrl             *gomock.Controller
	host             string
	addr             string
	cas              *store.CAStore
	cp               *testClientProvider
	clusterProvider  *mockblobclient.MockClusterProvider
	pctx             core.PeerContext
	backendManager   *backend.Manager
	writeBackManager *mockpersistedretry.MockManager
	clk              *clock.Mock
	cleanup          func()
}

func newTestServer(
	t *testing.T, host string, ring hashring.Ring, cp *testClientProvider) *testServer {

	var cleanup testutil.Cleanup
	defer cleanup.Recover()

	ctrl := gomock.NewController(t)
	cleanup.Add(ctrl.Finish)

	clusterProvider := mockblobclient.NewMockClusterProvider(ctrl)

	pctx := core.PeerContextFixture()

	cas, c := store.CAStoreFixture()
	cleanup.Add(c)

	bm := backend.ManagerFixture()

	writeBackManager := mockpersistedretry.NewMockManager(ctrl)

	mg := metainfogen.Fixture(cas, 4)

	br := blobrefresh.New(blobrefresh.Config{}, tally.NoopScope, cas, bm, mg)

	clk := clock.NewMock()
	clk.Set(time.Now())

	s, err := New(
		Config{}, tally.NoopScope, clk, host, ring, cas, cp, clusterProvider, pctx,
		bm, br, mg, writeBackManager)
	if err != nil {
		panic(err)
	}

	addr, stop := testutil.StartServer(s.Handler())
	cleanup.Add(stop)

	cp.register(host, blobclient.New(addr, blobclient.WithChunkSize(16)))

	return &testServer{
		ctrl:             ctrl,
		host:             host,
		addr:             addr,
		cas:              cas,
		cp:               cp,
		clusterProvider:  clusterProvider,
		pctx:             pctx,
		backendManager:   bm,
		writeBackManager: writeBackManager,
		clk:              clk,
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

func (s *testServer) expectRemoteCluster(dns string) *mockblobclient.MockClusterClient {
	cc := mockblobclient.NewMockClusterClient(s.ctrl)
	s.clusterProvider.EXPECT().Provide(dns).Return(cc, nil).MinTimes(1)
	return cc
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
