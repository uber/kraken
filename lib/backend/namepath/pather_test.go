package namepath

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBlobPathConversion(t *testing.T) {
	tests := []struct {
		pather   string
		name     string
		expected string
	}{
		{
			DockerTag,
			"repo-bar:latest",
			"/root/docker/registry/v2/repositories/repo-bar/_manifests/tags/latest/current/link",
		}, {
			ShardedDockerBlob,
			"ff85ceb9734a3c2fbb886e0f7cfc66b046eeeae953d8cb430dc5a7ace544b0e9",
			"/root/docker/registry/v2/blobs/sha256/ff/ff85ceb9734a3c2fbb886e0f7cfc66b046eeeae953d8cb430dc5a7ace544b0e9/data",
		}, {
			Identity,
			"foo/bar",
			"/root/foo/bar",
		},
	}
	for _, test := range tests {
		t.Run(test.pather, func(t *testing.T) {
			require := require.New(t)

			p, err := New("/root", test.pather)
			require.NoError(err)

			path, err := p.BlobPath(test.name)
			require.NoError(err)
			require.Equal(test.expected, path)

			original, err := p.NameFromBlobPath(path)
			require.NoError(err)
			require.Equal(test.name, original)
		})
	}
}

func TestDockerTagErrors(t *testing.T) {
	for _, name := range []string{
		"4dfa0d38b99b774aabfde9a62421ac787ab168369e92421df968c7348893b60c",
		":",
		"repo:",
		":tag",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := DockerTagPather{"/"}.BlobPath(name)
			require.Error(t, err)
		})
	}
}

func TestShardedDockerBlobErrors(t *testing.T) {
	for _, name := range []string{
		"4d",
		":",
		"",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := ShardedDockerBlobPather{"/"}.BlobPath(name)
			require.Error(t, err)
		})
	}
}

func TestNameFromBlobPathErrors(t *testing.T) {
	// TODO(codyg): Write me!
}
