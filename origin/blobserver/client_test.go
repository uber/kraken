package blobserver

import (
	"bytes"
	"io/ioutil"
	"sort"
	"testing"

	"code.uber.internal/infra/kraken/lib/dockerregistry/image"
	"code.uber.internal/infra/kraken/lib/serverset"
	"code.uber.internal/infra/kraken/origin/blobclient"

	"github.com/stretchr/testify/require"
)

func toAddrs(clients []blobclient.Client) []string {
	var addrs []string
	for _, c := range clients {
		addrs = append(addrs, c.Addr())
	}
	sort.Strings(addrs)
	return addrs
}

func TestClusterClientResilientToUnavailableMasters(t *testing.T) {
	require := require.New(t)

	cp := newTestClientProvider()

	s := newTestServer(master1, configNoRedirectFixture(), cp)
	defer s.cleanup()

	// Register a dummy master addresses so Provide can still create a Client for
	// unavailable masters.
	cp.register(master2, "http://localhost:0")
	cp.register(master3, "http://localhost:0")

	masters, err := serverset.NewRoundRobin(serverset.RoundRobinConfig{
		Addrs:   []string{master1, master2, master3},
		Retries: 3,
	})
	require.NoError(err)

	cc := blobclient.NewClusterClient(cp, masters)

	// Run many times to make sure we eventually hit unavailable masters.
	for i := 0; i < 100; i++ {
		d, blob := image.DigestWithBlobFixture()

		require.NoError(cc.UploadBlob("noexist", d, bytes.NewReader(blob)))

		mi, err := cc.GetMetaInfo("noexist", d)
		require.NoError(err)
		require.NotNil(mi)

		r, err := cc.DownloadBlob(d)
		require.NoError(err)
		result, err := ioutil.ReadAll(r)
		require.NoError(err)
		require.Equal(string(blob), string(result))

		peers, err := cc.Owners(d)
		require.NoError(err)
		require.Len(peers, 1)
		require.Equal(s.pctx, peers[0])
	}
}

func TestClusterClientReturnsErrorOnNoAvailability(t *testing.T) {
	require := require.New(t)

	cp := newTestClientProvider()
	cp.register(master1, "http://localhost:0")
	cp.register(master2, "http://localhost:0")
	cp.register(master3, "http://localhost:0")

	masters, err := serverset.NewRoundRobin(serverset.RoundRobinConfig{
		Addrs:   []string{master1, master2, master3},
		Retries: 3,
	})
	require.NoError(err)

	cc := blobclient.NewClusterClient(cp, masters)

	d, blob := image.DigestWithBlobFixture()

	require.Error(cc.UploadBlob("noexist", d, bytes.NewReader(blob)))

	_, err = cc.GetMetaInfo("noexist", d)
	require.Error(err)

	_, err = cc.DownloadBlob(d)
	require.Error(err)

	_, err = cc.Owners(d)
	require.Error(err)
}
