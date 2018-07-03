package transfer

import (
	"fmt"
	"os"

	"code.uber.internal/infra/kraken/build-index/tagclient"
	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/origin/blobclient"
	"code.uber.internal/infra/kraken/utils/log"

	"github.com/docker/distribution/uuid"
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

// Stat returns blob info from origin cluster or local cache.
func (t *ProxyTransferer) Stat(namespace string, d core.Digest) (*core.BlobInfo, error) {
	fi, err := t.cas.GetCacheFileStat(d.Hex())
	if err != nil {
		if os.IsNotExist(err) {
			return t.originStat(namespace, d)
		}
		return nil, fmt.Errorf("stat cache file: %s", err)
	}
	return core.NewBlobInfo(fi.Size()), nil
}

func (t *ProxyTransferer) originStat(namespace string, d core.Digest) (*core.BlobInfo, error) {
	bi, err := t.originCluster.Stat(namespace, d)
	if err != nil {
		// `docker push` stats blobs before uploading them. If the blob is not
		// found, it will upload it. However if remote blob storage is unavailable,
		// this will be a 5XX error, and will short-circuit push. We must consider
		// this class of error to be a 404 to allow pushes to succeed while remote
		// storage is down (write-back will eventually persist the blobs).
		if err != blobclient.ErrBlobNotFound {
			log.With("digest", d).Info("Error stat-ing origin blob: %s", err)
		}
		return nil, ErrBlobNotFound
	}
	return bi, nil
}

// Download downloads the blob of name into the file store and returns a reader
// to the newly downloaded file.
func (t *ProxyTransferer) Download(namespace string, d core.Digest) (store.FileReader, error) {
	blob, err := t.cas.GetCacheFileReader(d.Hex())
	if err != nil {
		if os.IsNotExist(err) {
			return t.downloadFromOrigin(namespace, d)
		}
		return nil, fmt.Errorf("get cache file: %s", err)
	}
	return blob, nil
}

func (t *ProxyTransferer) downloadFromOrigin(namespace string, d core.Digest) (store.FileReader, error) {
	tmp := fmt.Sprintf("%s.%s", d.Hex(), uuid.Generate().String())
	if err := t.cas.CreateUploadFile(tmp, 0); err != nil {
		return nil, fmt.Errorf("create upload file: %s", err)
	}
	w, err := t.cas.GetUploadFileReadWriter(tmp)
	if err != nil {
		return nil, fmt.Errorf("get upload writer: %s", err)
	}
	defer w.Close()
	if err := t.originCluster.DownloadBlob(namespace, d, w); err != nil {
		if err == blobclient.ErrBlobNotFound {
			return nil, ErrBlobNotFound
		}
		return nil, fmt.Errorf("origin: %s", err)
	}
	if err := t.cas.MoveUploadFileToCache(tmp, d.Hex()); err != nil && !os.IsExist(err) {
		return nil, fmt.Errorf("move upload file to cache: %s", err)
	}
	blob, err := t.cas.GetCacheFileReader(d.Hex())
	if err != nil {
		return nil, fmt.Errorf("get cache file: %s", err)
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
	d, err := t.tags.Get(tag)
	if err != nil {
		if err == tagclient.ErrTagNotFound {
			return core.Digest{}, ErrTagNotFound
		}
		return core.Digest{}, fmt.Errorf("client get tag: %s", err)
	}
	return d, nil
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
