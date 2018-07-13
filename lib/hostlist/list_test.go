package hostlist

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestListResolve(t *testing.T) {
	require := require.New(t)

	l, err := New(Config{Static: []string{"a", "b", "c"}}, 80)
	require.NoError(err)

	s, err := l.Resolve()
	require.NoError(err)
	require.ElementsMatch([]string{"a:80", "b:80", "c:80"}, s.ToSlice())
}
