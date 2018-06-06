package tagtype

import (
	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/origin/blobclient"
)

// DefaultResolver resolve default tag rependencies.
type DefaultResolver struct {
	originClient blobclient.ClusterClient
}

// NewDefaultResolver creates a new resolver for default tags.
func NewDefaultResolver(originClient blobclient.ClusterClient) DependencyResolver {
	return &DefaultResolver{originClient}
}

// Resolve always returns d as the sole dependency of tag.
func (r *DefaultResolver) Resolve(tag string, d core.Digest) (core.DigestList, error) {
	return core.DigestList{d}, nil
}
