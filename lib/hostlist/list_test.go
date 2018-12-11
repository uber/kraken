package hostlist

import (
	"testing"

	"github.com/uber/kraken/utils/stringset"

	"github.com/stretchr/testify/require"
)

func TestListResolve(t *testing.T) {
	require := require.New(t)

	addrs := []string{"a:80", "b:80", "c:80"}

	l, err := New(Config{Static: addrs})
	require.NoError(err)

	require.ElementsMatch(addrs, l.Resolve().ToSlice())
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

func TestInvalidConfig(t *testing.T) {
	tests := []struct {
		desc   string
		config Config
	}{
		{"dns missing port", Config{DNS: "some-dns"}},
		{"static missing port", Config{Static: []string{"a:80", "b"}}},
	}
	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			_, err := New(test.config)
			require.Error(t, err)
		})
	}
}
