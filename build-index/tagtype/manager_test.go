package tagtype

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func testConfigs() []Config {
	return []Config{
		{Namespace: "uber-usi/.*", Type: "docker"},
		{Namespace: "replicator/.*", Type: "default"},
	}
}

func TestManagerGetResolver(t *testing.T) {
	require := require.New(t)

	m, err := NewManager(testConfigs(), nil)
	require.NoError(err)

	_, err = m.GetDependencyResolver("uber-usi/tag")
	require.NoError(err)
}

func TestManagerGetResolverNamespaceError(t *testing.T) {
	require := require.New(t)

	m, err := NewManager(testConfigs(), nil)
	require.NoError(err)

	_, err = m.GetDependencyResolver("invalid/tag")
	require.Error(ErrNamespaceNotFound, err)
}
