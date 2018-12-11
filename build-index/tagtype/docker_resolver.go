package tagtype

import (
	"bytes"
	"fmt"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/origin/blobclient"
	"github.com/uber/kraken/utils/dockerutil"
	"github.com/docker/distribution"
)

type dockerResolver struct {
	originClient blobclient.ClusterClient
}

// Resolve returns all layers + manifest of given tag as its dependencies.
func (r *dockerResolver) Resolve(tag string, d core.Digest) (core.DigestList, error) {
	m, err := r.downloadManifest(tag, d)
	if err != nil {
		return nil, err
	}
	deps, err := dockerutil.GetManifestReferences(m)
	if err != nil {
		return nil, fmt.Errorf("get manifest references: %s", err)
	}
	return append(deps, d), nil
}

func (r *dockerResolver) downloadManifest(tag string, d core.Digest) (distribution.Manifest, error) {
	buf := &bytes.Buffer{}
	if err := r.originClient.DownloadBlob(tag, d, buf); err != nil {
		return nil, fmt.Errorf("download blob: %s", err)
	}
	manifest, _, err := dockerutil.ParseManifestV2(buf)
	if err != nil {
		return nil, fmt.Errorf("parse manifest: %s", err)
	}
	return manifest, nil
}
