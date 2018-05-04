package testfs

import (
	"bytes"
	"testing"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/backend/backenderrors"
	"code.uber.internal/infra/kraken/utils/testutil"
	"github.com/stretchr/testify/require"
)

func TestServerUploadDownloadStat(t *testing.T) {
	require := require.New(t)

	s := NewServer()
	defer s.Cleanup()

	addr, stop := testutil.StartServer(s.Handler())
	defer stop()

	c, err := NewClient(Config{Addr: addr})
	require.NoError(err)

	blob := core.NewBlobFixture()

	_, err = c.Stat(blob.Digest.Hex())
	require.Equal(backenderrors.ErrBlobNotFound, err)

	require.NoError(c.Upload(blob.Digest.Hex(), bytes.NewReader(blob.Content)))

	var b bytes.Buffer
	require.NoError(c.Download(blob.Digest.Hex(), &b))
	require.Equal(blob.Content, b.Bytes())

	_, err = c.Stat(blob.Digest.Hex())
	require.NoError(err)
}
