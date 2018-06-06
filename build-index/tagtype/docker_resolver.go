package tagtype

import (
	"bytes"
	"fmt"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/origin/blobclient"
	"code.uber.internal/infra/kraken/utils/dockerutil"
	"github.com/docker/distribution"
)

// DockerResolver resolve docker tag dependencies.
type DockerResolver struct {
	originClient blobclient.ClusterClient
}

// NewDockerResolver creates a new resolver for docker tags.
func NewDockerResolver(originClient blobclient.ClusterClient) DependencyResolver {
	return &DockerResolver{originClient}
}

// Resolve returns all layers + manifest of given tag as its dependencies.
func (r *DockerResolver) Resolve(tag string, d core.Digest) (core.DigestList, error) {
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

func (r *DockerResolver) downloadManifest(tag string, d core.Digest) (distribution.Manifest, error) {
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
