package testfs

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber/kraken/lib/backend/namepath"
)

func TestClientFactory(t *testing.T) {
	require := require.New(t)

	config := Config{
		Addr:     "test",
		NamePath: namepath.Identity,
	}
	f := factory{}
	_, err := f.Create(config, nil)
	require.NoError(err)
}
