package transfer

import (
	"bytes"
	"fmt"
	"os"
	"strings"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/backend"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/origin/blobclient"
)

// OriginClusterTransferer wraps transferring blobs to the origin cluster.
type OriginClusterTransferer struct {
	originCluster blobclient.ClusterClient
	tagClient     backend.Client
	fs            store.FileStore
}

// NewOriginClusterTransferer creates a new OriginClusterTransferer.
func NewOriginClusterTransferer(
	originCluster blobclient.ClusterClient,
	tagClient backend.Client,
	fs store.FileStore) *OriginClusterTransferer {

	return &OriginClusterTransferer{
		originCluster: originCluster,
		tagClient:     tagClient,
		fs:            fs,
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

// GetTag gets manifest digest, given repo and tag.
func (t *OriginClusterTransferer) GetTag(repo, tag string) (core.Digest, error) {
	var b bytes.Buffer
	if err := t.tagClient.Download(fmt.Sprintf("%s:%s", repo, tag), &b); err != nil {
		return core.Digest{}, fmt.Errorf("download tag through client: %s", err)
	}

	d, err := core.NewDigestFromString(b.String())
	if err != nil {
		return core.Digest{}, fmt.Errorf("construct manifest digest: %s", err)
	}
	return d, nil
}

// PostTag posts tag:manifest_digest mapping to addr given repo and tag.
func (t *OriginClusterTransferer) PostTag(repo, tag string, manifestDigest core.Digest) error {
	r := strings.NewReader(manifestDigest.String())
	if err := t.tagClient.Upload(fmt.Sprintf("%s:%s", repo, tag), r); err != nil {
		return fmt.Errorf("upload tag through client: %s", err)
	}
	return nil
}
