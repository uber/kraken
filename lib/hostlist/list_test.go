package hostlist

import (
	"testing"

	"code.uber.internal/infra/kraken/utils/stringset"

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

func TestListResolveNonLocal(t *testing.T) {
	require := require.New(t)

	localNames, err := getLocalNames()
	require.NoError(err)
	require.NotEmpty(localNames)

	config := Config{
		Static: append([]string{"a", "b", "c"}, localNames.ToSlice()...),
	}

	l, err := New(config, 80)
	require.NoError(err)

	s, err := l.ResolveNonLocal()
	require.NoError(err)
	require.ElementsMatch([]string{"a:80", "b:80", "c:80"}, s.ToSlice())
}

func TestAttachPortIfMissing(t *testing.T) {
	addrs, err := attachPortIfMissing(stringset.New("x", "y:5", "z"), 7)
	require.NoError(t, err)
	require.Equal(t, stringset.New("x:7", "y:5", "z:7"), addrs)
}

func TestAttachPortIfMissingError(t *testing.T) {
	_, err := attachPortIfMissing(stringset.New("a:b:c"), 7)
	require.Error(t, err)
}
