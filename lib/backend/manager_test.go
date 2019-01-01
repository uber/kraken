package backend_test

import (
	"testing"

	. "github.com/uber/kraken/lib/backend"
	"github.com/uber/kraken/lib/backend/namepath"
	"github.com/uber/kraken/lib/backend/testfs"
	"github.com/uber/kraken/utils/bandwidth"
	"github.com/uber/kraken/utils/stringset"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

func TestManagerNamespaceMatching(t *testing.T) {
	c1 := ClientFixture()
	c2 := ClientFixture()

	tests := []struct {
		namespace string
		expected  Client
	}{
		{"static", c1},
		{"namespace-foo/repo-bar", c2},
	}
	for _, test := range tests {
		t.Run(test.namespace, func(t *testing.T) {
			require := require.New(t)

			m := ManagerFixture()

			require.NoError(m.Register("static", c1))
			require.NoError(m.Register("namespace-foo/.*", c2))

			result, err := m.GetClient(test.namespace)
			require.NoError(err)
			require.True(test.expected.(*TestClient) == result.(*TestClient))
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

	fooBarAddr := "testfs-foo-bar"
	fooAddr := "testfs-foo"
	defaultAddr := "testfs-default"

	configStr := `
- namespace: foo/bar/.*
  backend:
      testfs:
          addr: testfs-foo-bar
          name_path: identity
- namespace: foo/.*
  backend:
      testfs:
          addr: testfs-foo
          name_path: identity
- namespace: .*
  backend:
      testfs:
          addr: testfs-default
          name_path: identity
`
	var configs []Config
	require.NoError(yaml.Unmarshal([]byte(configStr), &configs))

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

func TestManagerBandwidth(t *testing.T) {
	require := require.New(t)

	m, err := NewManager([]Config{{
		Namespace: ".*",
		Bandwidth: bandwidth.Config{
			EgressBitsPerSec:  10,
			IngressBitsPerSec: 50,
			TokenSize:         1,
			Enable:            true,
		},
		Backend: map[string]interface{}{
			"testfs": testfs.Config{Addr: "test-addr", NamePath: namepath.Identity},
		},
	}}, AuthConfig{})
	require.NoError(err)

	checkBandwidth := func(egress, ingress int64) {
		c, err := m.GetClient("foo")
		require.NoError(err)
		tc, ok := c.(*ThrottledClient)
		require.True(ok)
		require.Equal(egress, tc.EgressLimit())
		require.Equal(ingress, tc.IngressLimit())
	}

	checkBandwidth(10, 50)

	watcher := NewBandwidthWatcher(m)
	watcher.Notify(stringset.New("a", "b"))

	checkBandwidth(5, 25)
}
