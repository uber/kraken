// Copyright (c) 2016-2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package dockerutil

import (
	"fmt"

	"github.com/uber/kraken/core"
)

// ManifestFixture creates a manifest blob for testing purposes.
func ManifestFixture(config core.Digest, layer1 core.Digest, layer2 core.Digest) (core.Digest, []byte) {
	raw := []byte(fmt.Sprintf(`{
	   "schemaVersion": 2,
	   "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
	   "config": {
		  "mediaType": "application/vnd.docker.container.image.v1+json",
		  "size": 2940,
		  "digest": "%s"
	   },
	   "layers": [
		  {
			 "mediaType": "application/vnd.docker.image.rootfs.diff.tar.gzip",
			 "size": 1902063,
			 "digest": "%s"
		  },
		  {
			 "mediaType": "application/vnd.docker.image.rootfs.diff.tar.gzip",
			 "size": 2345077,
			 "digest": "%s"
		  }
	   ]
	}`, config, layer1, layer2))

	d, err := core.NewDigester().FromBytes(raw)
	if err != nil {
		panic(err)
	}

	return d, raw
}

// ManifestListFixture creates a manifest list blob for testing purposes.
func ManifestListFixture(manifest1 core.Digest, manifest2 core.Digest) (core.Digest, []byte) {
	raw := []byte(fmt.Sprintf(`{
		"schemaVersion": 2,
		"mediaType": "application/vnd.docker.distribution.manifest.list.v2+json",
		"manifests": [
			{
				"mediaType": "application/vnd.docker.distribution.manifest.v2+json",
				"size": 1024,
				"digest": "%s",
				"platform": {
					"architecture": "amd64",
					"os": "linux"
				}
			},
			{
				"mediaType": "application/vnd.docker.distribution.manifest.v2+json",
				"size": 1024,
				"digest": "%s",
				"platform": {
					"architecture": "arm64",
					"os": "linux"
				}
			}
		]
	}`, manifest1, manifest2))

	d, err := core.NewDigester().FromBytes(raw)
	if err != nil {
		panic(err)
	}
	return d, raw
}

// OCIManifestFixture creates an OCI image manifest blob for testing purposes.
func OCIManifestFixture(config core.Digest, layer1 core.Digest, layer2 core.Digest) (core.Digest, []byte) {
	raw := []byte(fmt.Sprintf(`{
		"schemaVersion": 2,
		"mediaType": "application/vnd.oci.image.manifest.v1+json",
		"config": {
			"mediaType": "application/vnd.oci.image.config.v1+json",
			"size": 2940,
			"digest": "%s"
		},
		"layers": [
			{
				"mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
				"size": 1902063,
				"digest": "%s"
			},
			{
				"mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
				"size": 2345077,
				"digest": "%s"
			}
		]
	}`, config, layer1, layer2))

	d, err := core.NewDigester().FromBytes(raw)
	if err != nil {
		panic(err)
	}
	return d, raw
}

// OCIIndexFixture creates an OCI image index blob for testing purposes.
func OCIIndexFixture(manifest1 core.Digest, manifest2 core.Digest) (core.Digest, []byte) {
	raw := []byte(fmt.Sprintf(`{
		"schemaVersion": 2,
		"mediaType": "application/vnd.oci.image.index.v1+json",
		"manifests": [
			{
				"mediaType": "application/vnd.oci.image.manifest.v1+json",
				"size": 1024,
				"digest": "%s",
				"platform": {
					"architecture": "amd64",
					"os": "linux"
				}
			},
			{
				"mediaType": "application/vnd.oci.image.manifest.v1+json",
				"size": 1024,
				"digest": "%s",
				"platform": {
					"architecture": "arm64",
					"os": "linux"
				}
			}
		]
	}`, manifest1, manifest2))

	d, err := core.NewDigester().FromBytes(raw)
	if err != nil {
		panic(err)
	}
	return d, raw
}
