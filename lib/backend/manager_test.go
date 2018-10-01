package backend

import (
	"testing"

	"code.uber.internal/infra/kraken/lib/backend/namepath"
	"code.uber.internal/infra/kraken/lib/backend/testfs"
	"code.uber.internal/infra/kraken/utils/bandwidth"
	"github.com/stretchr/testify/require"
)

func TestManagerNamespaceMatching(t *testing.T) {
	c1 := ClientFixture()
	c2 := ClientFixture()
	c3 := ClientFixture()

	tests := []struct {
		namespace string
		expected  Client
	}{
		{"static", c1},
		{"uber-usi/labrat", c2},
		{"terrablob/maps_data", c3},
	}
	for _, test := range tests {
		t.Run(test.namespace, func(t *testing.T) {
			require := require.New(t)

			m := ManagerFixture()

			require.NoError(m.Register("static", c1))
			require.NoError(m.Register("uber-usi/.*", c2))
			require.NoError(m.Register("terrablob/.*", c3))

			result, err := m.GetClient(test.namespace)
			require.NoError(err)
			require.True(test.expected.(*testClient) == result.(*testClient))
		})
	}
}

func TestManagerErrNamespaceNotFound(t *testing.T) {
	m := ManagerFixture()
	_, err := m.GetClient("no-match")
	require.Equal(t, ErrNamespaceNotFound, err)
}

func TestManagerNamespaceOrdering(t *testing.T) {
	require := require.New(t)

	fooAddr := "testfs-foo"
	fooBarAddr := "testfs-foo-bar"
	defaultAddr := "testfs-default"

	configs := []Config{
		{
			Namespace: "foo/bar/.*",
			Backend:   "testfs",
			TestFS:    testfs.Config{Addr: fooBarAddr, NamePath: namepath.Identity},
		}, {
			Namespace: "foo/.*",
			Backend:   "testfs",
			TestFS:    testfs.Config{Addr: fooAddr, NamePath: namepath.Identity},
		}, {
			Namespace: ".*",
			Backend:   "testfs",
			TestFS:    testfs.Config{Addr: defaultAddr, NamePath: namepath.Identity},
		},
	}

	m, err := NewManager(configs, AuthConfig{})
	require.NoError(err)

	for ns, expected := range map[string]string{
		"foo/bar/baz": fooBarAddr,
		"foo/bar/123": fooBarAddr,
		"foo/123":     fooAddr,
		"abc":         defaultAddr,
		"xyz":         defaultAddr,
		"bar/baz":     defaultAddr,
		"":            defaultAddr,
	} {
		c, err := m.GetClient(ns)
		require.NoError(err)
		require.Equal(expected, c.(*testfs.Client).Addr(), "Namespace: %s", ns)
	}
}

func TestManagerThrottleClient(t *testing.T) {
	require := require.New(t)

	m, err := NewManager([]Config{{
		Namespace: ".*",
		Bandwidth: bandwidth.Config{EgressBitsPerSec: 1, IngressBitsPerSec: 1, Enable: true},
		Backend:   "testfs",
		TestFS:    testfs.Config{Addr: "test-addr", NamePath: namepath.Identity},
	}}, AuthConfig{})
	require.NoError(err)

	c, err := m.GetClient("foo")
	require.NoError(err)
	_, ok := c.(*throttledClient)
	require.True(ok)
}
