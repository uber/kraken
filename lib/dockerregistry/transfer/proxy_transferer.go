package transfer

import (
	"fmt"
	"os"

	"code.uber.internal/infra/kraken/build-index/tagclient"
	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/origin/blobclient"

	"github.com/docker/distribution/uuid"
)

// ProxyTransferer is a Transferer for proxy. Uploads/downloads blobs via the
// local origin cluster, and posts/gets tags via the local build-index.
type ProxyTransferer struct {
	tags          tagclient.Client
	originCluster blobclient.ClusterClient
	fs            store.FileStore
}

// NewProxyTransferer creates a new ProxyTransferer.
func NewProxyTransferer(
	tags tagclient.Client,
	originCluster blobclient.ClusterClient,
	fs store.FileStore) *ProxyTransferer {

	return &ProxyTransferer{tags, originCluster, fs}
}

// Download downloads the blob of name into the file store and returns a reader
// to the newly downloaded file.
func (t *ProxyTransferer) Download(namespace string, d core.Digest) (store.FileReader, error) {
	blob, err := t.fs.GetCacheFileReader(d.Hex())
	if err != nil {
		if os.IsNotExist(err) {
			tmp := fmt.Sprintf("%s.%s", d.Hex(), uuid.Generate().String())
			if err := t.fs.CreateUploadFile(tmp, 0); err != nil {
				return nil, err
			}
			w, err := t.fs.GetUploadFileReadWriter(tmp)
			if err != nil {
				return nil, err
			}
			defer w.Close()

			if err := t.originCluster.DownloadBlob(namespace, d, w); err != nil {
				return nil, fmt.Errorf("remote backend download: %s", err)
			}

			if err := t.fs.MoveUploadFileToCache(tmp, d.Hex()); err != nil {
				if !os.IsExist(err) {
					return nil, err
				}
				// If file exists another thread else is pulling the same blob.
			}
			blob, err = t.fs.GetCacheFileReader(d.Hex())
			if err != nil {
				return nil, fmt.Errorf("get cache file: %s", err)
			}
		} else {
			return nil, fmt.Errorf("get cache file: %s", err)
		}
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
	f, err := t.fs.GetCacheFileReader(d.Hex())
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
