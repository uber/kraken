package client

import (
	"bytes"
	"io/ioutil"
	"os"
	"testing"

	"code.uber.internal/infra/kraken/lib/dockerregistry/image"

	"github.com/stretchr/testify/require"
)

func TestGetAndPostManifest(t *testing.T) {
	require := require.New(t)
	cli, cleanup := BlobAPIClientFixture()
	defer cleanup()

	// Pull and save manifest
	digest, err := cli.GetManifest("repo", "tag")
	require.NoError(err)
	require.Equal(_digestHex, digest)

	_, err = cli.blobStore.GetCacheFileStat(digest)
	require.NoError(err)

	// Post manifest by digest
	require.NoError(cli.PostManifest("repo", "tag", digest))
	// Manifest not found
	require.Error(os.ErrNotExist, cli.PostManifest("repo", "tag", "1234"))
}

func TestPullAndPushBlob(t *testing.T) {
	require := require.New(t)
	cli, cleanup := BlobAPIClientFixture()
	defer cleanup()
	// Pull and verify blob
	imageDigest, err := image.NewDigestFromString("sha256:" + _digestHex)
	require.NoError(err)
	require.NoError(cli.PullBlob(*imageDigest))

	// Push blob by digest
	require.NoError(cli.PushBlob(*imageDigest))
	// Blob not found
	imageDigest, err = image.NewDigestFromString("sha256:1234")
	require.NoError(err)
	require.Error(os.ErrNotExist, cli.PushBlob(*imageDigest))
}

func TestVerifyBlob(t *testing.T) {
	require := require.New(t)
	cli, cleanup := BlobAPIClientFixture()
	defer cleanup()

	data, err := ioutil.ReadFile("../test/testmanifest.json")
	require.NoError(err)
	reader := bytes.NewReader(data)

	imageDigest, err := image.NewDigestFromString("sha256:1234")
	require.NoError(err)
	ok, err := cli.verifyBlob(*imageDigest, reader)
	require.NoError(err)
	require.False(ok)

	_, err = reader.Seek(0, 0)
	require.NoError(err)

	imageDigest, err = image.NewDigestFromString("sha256:" + _digestHex)
	require.NoError(err)
	ok, err = cli.verifyBlob(*imageDigest, reader)
	require.NoError(err)
	require.True(ok)
}
