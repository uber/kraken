package dockerregistry

import "testing"
import "github.com/stretchr/testify/require"

var (
	layer    = "/docker/registry/v2/repositories/external/ubuntu/_layers/sha256/35bc48a1ca97c3971611dc4662d08d131869daa692acb281c7e9e052924e38b1/link"
	manifest = "/docker/registry/v2/repositories/external/ubuntu/_manifests/tags/sjc1-produ-0000001/current/link"
)

func TestIsTag(t *testing.T) {
	assert := require.New(t)
	d := &P2PStorageDriver{}
	isTag, tagOrDigest, _ := d.isTag(layer)
	assert.False(isTag)
	assert.Equal(tagOrDigest, "35bc48a1ca97c3971611dc4662d08d131869daa692acb281c7e9e052924e38b1")
	isTag, tagOrDigest, _ = d.isTag(manifest)
	assert.True(isTag)
	assert.Equal(tagOrDigest, "sjc1-produ-0000001")
	_, _, err := d.isTag("/docker/registry/v2/repositories/external/ubuntu/asdf")
	assert.NotNil(err)
	assert.Equal(err.Error(), "Invalid path format /docker/registry/v2/repositories/external/ubuntu/asdf")
}

func TestGetRepoName(t *testing.T) {
	assert := require.New(t)
	d := &P2PStorageDriver{}
	name, _ := d.getRepoName(layer)
	assert.Equal(name, "external/ubuntu")
	name, _ = d.getRepoName(manifest)
	assert.Equal(name, "external/ubuntu")
}
