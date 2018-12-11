package core

import (
	"fmt"
	"testing"

	"github.com/uber/kraken/utils/randutil"
	"github.com/uber/kraken/utils/stringset"
	"github.com/stretchr/testify/require"
)

func TestHashedPeerID(t *testing.T) {
	require := require.New(t)

	n := 50

	ids := make(stringset.Set)
	for i := 0; i < n; i++ {
		addr := fmt.Sprintf("%s:%d", randutil.IP(), randutil.Port())
		peerID, err := HashedPeerID(addr)
		require.NoError(err)
		ids.Add(peerID.String())
	}

	// None of the hashes should conflict.
	require.Len(ids, n)
}

func TestHashedPeerIDReturnsErrOnEmpty(t *testing.T) {
	_, err := HashedPeerID("")
	require.Error(t, err)
}
