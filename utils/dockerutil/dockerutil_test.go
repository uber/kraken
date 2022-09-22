package dockerutil_test

import (
	"testing"

	"github.com/docker/distribution/manifest/manifestlist"
	"github.com/stretchr/testify/require"
	"github.com/uber/kraken/utils/dockerutil"
)

var testManifestListBytes = []byte(`{
	"schemaVersion": 2,
	"mediaType": "application/vnd.docker.distribution.manifest.list.v2+json",
	"manifests": [
	   {
		  "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
		  "size": 985,
		  "digest": "sha256:1a9ec845ee94c202b2d5da74a24f0ed2058318bfa9879fa541efaecba272e86b",
		  "platform": {
			 "architecture": "amd64",
			 "os": "linux",
			 "features": [
				"sse4"
			 ]
		  }
	   },
	   {
		  "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
		  "size": 2392,
		  "digest": "sha256:6346340964309634683409684360934680934608934608934608934068934608",
		  "platform": {
			 "architecture": "sun4m",
			 "os": "sunos"
		  }
	   }
	]
 }`)

var testManifestBytes = []byte(`{
	"schemaVersion": 2,
	"mediaType": "application/vnd.docker.distribution.manifest.v2+json",
	"config": {
	   "mediaType": "application/vnd.docker.container.image.v1+json",
	   "size": 985,
	   "digest": "sha256:1a9ec845ee94c202b2d5da74a24f0ed2058318bfa9879fa541efaecba272e86b"
	},
	"layers": [
	   {
		  "mediaType": "application/vnd.docker.image.rootfs.diff.tar.gzip",
		  "size": 153263,
		  "digest": "sha256:62d8908bee94c202b2d35224a221aaa2058318bfa9879fa541efaecba272331b"
	   }
	]
 }`)

func TestParseManifestV2List(t *testing.T) {
	require := require.New(t)

	tests := []struct {
		name          string
		hasError      bool
		manifestBytes []byte
	}{
		{
			name:          "success",
			hasError:      false,
			manifestBytes: testManifestListBytes,
		},
		{
			name:          "wrong manifest type",
			hasError:      true,
			manifestBytes: testManifestBytes,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manifest, d, err := dockerutil.ParseManifestV2List(tt.manifestBytes)
			if tt.hasError {
				require.Error(err)
				return
			}

			require.NoError(err)
			mediaType, _, err := manifest.Payload()
			require.NoError(err)
			require.EqualValues(manifestlist.MediaTypeManifestList, mediaType)
			require.Equal("sha256", d.Algo())
		})
	}
}
