package testfs

import (
	"bytes"
	"testing"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/backend/backenderrors"
	"github.com/uber/kraken/lib/backend/namepath"
	"github.com/uber/kraken/utils/testutil"

	"github.com/stretchr/testify/require"
)

func TestServerBlob(t *testing.T) {
	require := require.New(t)

	s := NewServer()
	defer s.Cleanup()

	addr, stop := testutil.StartServer(s.Handler())
	defer stop()

	c, err := NewClient(Config{Addr: addr, NamePath: namepath.Identity})
	require.NoError(err)

	blob := core.NewBlobFixture()
	ns := core.NamespaceFixture()

	_, err = c.Stat(ns, blob.Digest.Hex())
	require.Equal(backenderrors.ErrBlobNotFound, err)

	require.NoError(c.Upload(ns, blob.Digest.Hex(), bytes.NewReader(blob.Content)))

	var b bytes.Buffer
	require.NoError(c.Download(ns, blob.Digest.Hex(), &b))
	require.Equal(blob.Content, b.Bytes())

	info, err := c.Stat(ns, blob.Digest.Hex())
	require.NoError(err)
	require.Equal(int64(len(blob.Content)), info.Size)
}

func TestServerTag(t *testing.T) {
	require := require.New(t)

	s := NewServer()
	defer s.Cleanup()

	addr, stop := testutil.StartServer(s.Handler())
	defer stop()

	c, err := NewClient(Config{Addr: addr, NamePath: namepath.Identity})
	require.NoError(err)

	ns := core.NamespaceFixture()
	tag := "labrat:latest"
	d := core.DigestFixture().String()

	require.NoError(c.Upload(ns, tag, bytes.NewBufferString(d)))

	var b bytes.Buffer
	require.NoError(c.Download(ns, tag, &b))
	require.Equal(d, b.String())
}

func TestServerList(t *testing.T) {
	tests := []struct {
		desc     string
		prefix   string
		expected []string
	}{
		{"root", "", []string{"a/b/c.txt", "a/b/d.txt", "x/y/z.txt"}},
		{"dir", "a", []string{"a/b/c.txt", "a/b/d.txt"}},
		{"file", "a/b/c.txt", nil},
	}
	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			require := require.New(t)

			s := NewServer()
			defer s.Cleanup()

			addr, stop := testutil.StartServer(s.Handler())
			defer stop()

			ns := core.NamespaceFixture()
			c, err := NewClient(Config{Addr: addr, Root: "root", NamePath: namepath.Identity})
			require.NoError(err)

			require.NoError(c.Upload(ns, "a/b/c.txt", bytes.NewBufferString("foo")))
			require.NoError(c.Upload(ns, "a/b/d.txt", bytes.NewBufferString("bar")))
			require.NoError(c.Upload(ns, "x/y/z.txt", bytes.NewBufferString("baz")))

			names, err := c.List(test.prefix)
			require.NoError(err)
			require.ElementsMatch(test.expected, names)
		})
	}
}

func TestDockerTagList(t *testing.T) {
	require := require.New(t)

	s := NewServer()
	defer s.Cleanup()

	addr, stop := testutil.StartServer(s.Handler())
	defer stop()

	c, err := NewClient(Config{Addr: addr, Root: "tags", NamePath: namepath.DockerTag})
	require.NoError(err)

	ns := core.NamespaceFixture()
	tags := []string{"foo:v0", "foo:latest", "bar:v0", "bar/baz:v0"}
	for _, tag := range tags {
		require.NoError(c.Upload(ns, tag, bytes.NewBufferString(core.DigestFixture().String())))
	}

	names, err := c.List("")
	require.NoError(err)
	require.ElementsMatch(tags, names)
}
