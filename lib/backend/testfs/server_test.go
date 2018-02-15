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

func TestServerUploadFileDownloadFile(t *testing.T) {
	require := require.New(t)

	s := NewServer()
	defer s.Cleanup()

	addr, stop := testutil.StartServer(s.Handler())
	defer stop()

	c, err := NewClient(Config{Addr: addr})
	require.NoError(err)

	d, blob := core.DigestWithBlobFixture()

	require.NoError(c.UploadFile(d.Hex(), bytes.NewReader(blob)))

	f, err := ioutil.TempFile("", "")
	require.NoError(err)
	defer os.Remove(f.Name())

	require.NoError(c.DownloadFile(d.Hex(), f))

	f, err = os.Open(f.Name())
	require.NoError(err)
	result, err := ioutil.ReadAll(f)
	require.NoError(err)
	require.Equal(string(blob), string(result))
}

func TestServerUploadBytesDownloadBytes(t *testing.T) {
	require := require.New(t)

	s := NewServer()
	defer s.Cleanup()

	addr, stop := testutil.StartServer(s.Handler())
	defer stop()

	c, err := NewClient(Config{Addr: addr})
	require.NoError(err)

	d, blob := core.DigestWithBlobFixture()

	require.NoError(c.UploadBytes(d.Hex(), blob))

	result, err := c.DownloadBytes(d.Hex())
	require.NoError(err)
	require.Equal(blob, result)
}
