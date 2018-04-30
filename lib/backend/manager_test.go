package backend

import (
	"testing"

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

			m, err := NewManager(nil, nil)
			require.NoError(err)

			require.NoError(m.Register("static", c1))
			require.NoError(m.Register("uber-usi/.*", c2))
			require.NoError(m.Register("terrablob/.*", c3))

			result, err := m.GetClient(test.namespace)
			require.NoError(err)
			require.True(test.expected.(*testClient) == result.(*testClient))
		})
	}
}

func TestManagerNamespaceNoMatch(t *testing.T) {
	require := require.New(t)

	m, err := NewManager(nil, nil)
	require.NoError(err)

	_, err = m.GetClient("")
	require.Error(err)

	_, err = m.GetClient("unknown")
	require.Error(err)
}
