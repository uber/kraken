package testfs

import (
	"bytes"
	"testing"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/backend/backenderrors"
	"code.uber.internal/infra/kraken/utils/testutil"
	"github.com/stretchr/testify/require"
)

func TestServerBlob(t *testing.T) {
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

	info, err := c.Stat(blob.Digest.Hex())
	require.NoError(err)
	require.Equal(int64(len(blob.Content)), info.Size)
}

func TestServerTag(t *testing.T) {
	require := require.New(t)

	s := NewServer()
	defer s.Cleanup()

	addr, stop := testutil.StartServer(s.Handler())
	defer stop()

	c, err := NewClient(Config{Addr: addr})
	require.NoError(err)

	tag := "labrat:latest"
	d := core.DigestFixture().String()

	require.NoError(c.Upload(tag, bytes.NewBufferString(d)))

	var b bytes.Buffer
	require.NoError(c.Download(tag, &b))
	require.Equal(d, b.String())
}

func TestServerList(t *testing.T) {
	require := require.New(t)

	s := NewServer()
	defer s.Cleanup()

	addr, stop := testutil.StartServer(s.Handler())
	defer stop()

	c, err := NewClient(Config{Addr: addr})
	require.NoError(err)

	require.NoError(c.Upload("a/b/c", bytes.NewBufferString("foo")))
	require.NoError(c.Upload("a/b/d", bytes.NewBufferString("bar")))
	require.NoError(c.Upload("x/y/z", bytes.NewBufferString("baz")))

	names, err := c.List("a/b")
	require.NoError(err)
	require.ElementsMatch([]string{"c", "d"}, names)
}
