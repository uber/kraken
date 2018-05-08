package dockerutil

import (
	"fmt"

	"code.uber.internal/infra/kraken/core"
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
