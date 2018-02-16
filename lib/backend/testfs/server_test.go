package testfs

import (
	"bytes"
	"testing"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/utils/testutil"
	"github.com/stretchr/testify/require"
)

func TestServerUploadDownload(t *testing.T) {
	require := require.New(t)

	s := NewServer()
	defer s.Cleanup()

	addr, stop := testutil.StartServer(s.Handler())
	defer stop()

	c, err := NewClient(Config{Addr: addr})
	require.NoError(err)

	d, blob := core.DigestWithBlobFixture()

	require.NoError(c.Upload(d.Hex(), bytes.NewReader(blob)))

	var b bytes.Buffer
	require.NoError(c.Download(d.Hex(), &b))
	require.Equal(blob, b.Bytes())
}
