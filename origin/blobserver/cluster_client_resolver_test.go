package blobserver

import (
	"sort"
	"testing"

	"code.uber.internal/infra/kraken/lib/dockerregistry/image"

	"github.com/stretchr/testify/require"
)

func toAddrs(clients []Client) []string {
	var addrs []string
	for _, c := range clients {
		addrs = append(addrs, c.Addr())
	}
	sort.Strings(addrs)
	return addrs
}

func TestRoundRobinClientResolverProvidesCorrectClients(t *testing.T) {
	require := require.New(t)

	cp := newTestClientProvider(clientConfigFixture())

	config := configFixture()

	s1 := newTestServer(master1, config, cp)
	defer s1.cleanup()
	s2 := newTestServer(master2, config, cp)
	defer s2.cleanup()
	s3 := newTestServer(master3, config, cp)
	defer s3.cleanup()

	d, _ := computeBlobForHosts(config, master1, master2)

	resolver, err := NewRoundRobinClientResolver(cp, 3, master1, master2, master3)
	require.NoError(err)

	clients, err := resolver.Resolve(d)
	require.NoError(err)
	expected := []string{s1.addr, s2.addr}
	sort.Strings(expected)
	require.Equal(expected, toAddrs(clients))
}

func TestRoundRobinClientResolverResilientToUnavailableMasters(t *testing.T) {
	require := require.New(t)

	cp := newTestClientProvider(clientConfigFixture())

	config := configFixture()

	s := newTestServer(master1, config, cp)
	defer s.cleanup()

	// Register a dummy master addresses so Provide can still create a Client for
	// unavailable masters.
	cp.register(master2, "master2-dummy-addr")
	cp.register(master3, "master3-dummy-addr")

	d, _ := computeBlobForHosts(config, master1, master2)

	// master2 and master3 are unavailable, however we should still be able to query
	// locations from master1.
	resolver, err := NewRoundRobinClientResolver(cp, 3, master1, master2, master3)
	require.NoError(err)

	// Run Resolve multiple times to ensure we eventually hit an unavailable server.
	for i := 0; i < 3; i++ {
		clients, err := resolver.Resolve(d)
		require.NoError(err)
		expected := []string{s.addr, "master2-dummy-addr"}
		sort.Strings(expected)
		require.Equal(expected, toAddrs(clients))
	}
}

func TestRoundRobinClientResolverReturnsErrorOnNoAvailability(t *testing.T) {
	require := require.New(t)

	cp := NewHTTPClientProvider(clientConfigFixture())

	resolver, err := NewRoundRobinClientResolver(cp, 3, master1, master2, master3)
	require.NoError(err)

	_, err = resolver.Resolve(image.DigestFixture())
	require.Error(err)
}
