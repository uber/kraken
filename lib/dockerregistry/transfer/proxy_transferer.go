package transfer

import (
	"fmt"
	"os"

	"code.uber.internal/infra/kraken/build-index/tagclient"
	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/origin/blobclient"
)

// ProxyTransferer is a Transferer for proxy. Uploads/downloads blobs via the
// local origin cluster, and posts/gets tags via the local build-index.
type ProxyTransferer struct {
	tags          tagclient.Client
	originCluster blobclient.ClusterClient
	cas           *store.CAStore
}

// NewProxyTransferer creates a new ProxyTransferer.
func NewProxyTransferer(
	tags tagclient.Client,
	originCluster blobclient.ClusterClient,
	cas *store.CAStore) *ProxyTransferer {

	return &ProxyTransferer{tags, originCluster, cas}
}

// Download only checks local cache from previous uploads and never downloads
// from origin, to avoid downloading blobs when handling HEAD requests during
// upload.
func (t *ProxyTransferer) Download(namespace string, d core.Digest) (store.FileReader, error) {
	blob, err := t.cas.GetCacheFileReader(d.Hex())
	if os.IsNotExist(err) {
		return nil, ErrBlobNotFound
	} else if err != nil {
		return nil, fmt.Errorf("get cache reader %s: %s", d.Hex(), err)
	}
	return blob, nil
}

// Upload uploads blob to the origin cluster.
func (t *ProxyTransferer) Upload(
	namespace string, d core.Digest, blob store.FileReader) error {

	return t.originCluster.UploadBlob(namespace, d, blob)
}

// GetTag returns the manifest digest for tag.
func (t *ProxyTransferer) GetTag(tag string) (core.Digest, error) {
	return t.tags.Get(tag)
}

// PostTag uploads d as the manifest digest for tag.
func (t *ProxyTransferer) PostTag(tag string, d core.Digest) error {
	f, err := t.cas.GetCacheFileReader(d.Hex())
	if err != nil {
		return fmt.Errorf("cache: %s", err)
	}
	defer f.Close()
	if err := t.tags.Put(tag, d); err != nil {
		return fmt.Errorf("put tag: %s", err)
	}
	if err := t.tags.Replicate(tag); err != nil {
		return fmt.Errorf("replicate tag: %s", err)
	}
	return nil
}

// ListRepository lists all tags of repo.
func (t *ProxyTransferer) ListRepository(repo string) ([]string, error) {
	return t.tags.ListRepository(repo)
}
