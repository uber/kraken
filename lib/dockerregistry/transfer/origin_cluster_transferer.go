package transfer

import (
	"fmt"
	"io"
	"os"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/dockerregistry/transfer/manifestclient"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/origin/blobclient"
)

// OriginClusterTransferer wraps transferring blobs to the origin cluster.
type OriginClusterTransferer struct {
	originCluster  blobclient.ClusterClient
	manifestClient manifestclient.Client
	fs             store.FileStore
}

// NewOriginClusterTransferer creates a new OriginClusterTransferer.
func NewOriginClusterTransferer(
	originCluster blobclient.ClusterClient,
	manifestClient manifestclient.Client,
	fs store.FileStore) *OriginClusterTransferer {

	return &OriginClusterTransferer{
		originCluster:  originCluster,
		manifestClient: manifestClient,
		fs:             fs,
	}
}

// Download downloads the blob of name into the file store and returns a reader
// to the newly downloaded file.
func (t *OriginClusterTransferer) Download(name string) (store.FileReader, error) {
	blob, err := t.fs.GetCacheFileReader(name)
	if err != nil {
		if os.IsNotExist(err) {
			r, err := t.originCluster.DownloadBlob(core.NewSHA256DigestFromHex(name))
			if err != nil {
				return nil, fmt.Errorf("origin cluster: %s", err)
			}
			if err := t.fs.CreateCacheFile(name, r); err != nil {
				return nil, fmt.Errorf("create cache file: %s", err)
			}
			blob, err = t.fs.GetCacheFileReader(name)
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
func (t *OriginClusterTransferer) Upload(name string, blob store.FileReader, size int64) error {
	d := core.NewSHA256DigestFromHex(name)
	return t.originCluster.UploadBlob("TODO", d, blob, size)
}

// GetManifest gets and saves manifest given addr, repo and tag
func (t *OriginClusterTransferer) GetManifest(repo, tag string) (io.ReadCloser, error) {
	return t.manifestClient.GetManifest(repo, tag)
}

// PostManifest posts manifest to addr given repo and tag
func (t *OriginClusterTransferer) PostManifest(repo, tag string, manifest io.Reader) error {
	return t.manifestClient.PostManifest(repo, tag, manifest)
}
