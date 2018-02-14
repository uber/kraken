package testfs

import (
	"bytes"
	"io/ioutil"
	"os"
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

	f, err := ioutil.TempFile("", "")
	require.NoError(err)
	defer os.Remove(f.Name())

	require.NoError(c.Download(d.Hex(), f))

	f, err = os.Open(f.Name())
	require.NoError(err)
	result, err := ioutil.ReadAll(f)
	require.NoError(err)
	require.Equal(string(blob), string(result))
}
