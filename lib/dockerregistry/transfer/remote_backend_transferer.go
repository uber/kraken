package transfer

import (
	"bytes"
	"fmt"
	"os"
	"strings"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/backend"
	"code.uber.internal/infra/kraken/lib/store"

	"github.com/docker/distribution/uuid"
	lru "github.com/hashicorp/golang-lru"
)

const _defaultTagLRUSize = 256

// RemoteBackendTransferer wraps transferring blobs to/from remote storage backend.
type RemoteBackendTransferer struct {
	tagClient  backend.Client
	blobClient backend.Client

	cache *lru.Cache      // In-memory LRU cache for tags
	fs    store.FileStore // On-disk cache for blobs
}

// NewRemoteBackendTransferer creates a new RemoteBackendTransferer.
func NewRemoteBackendTransferer(
	tagClient backend.Client,
	blobClient backend.Client,
	fs store.FileStore) (*RemoteBackendTransferer, error) {

	cache, err := lru.New(_defaultTagLRUSize)
	if err != nil {
		return nil, fmt.Errorf("failed to init tag lru: %s", err)
	}

	return &RemoteBackendTransferer{
		tagClient:  tagClient,
		blobClient: blobClient,
		cache:      cache,
		fs:         fs,
	}, nil
}

// Download downloads the blob of name into the file store and returns a reader
// to the newly downloaded file.
func (t *RemoteBackendTransferer) Download(namespace, name string) (store.FileReader, error) {
	blob, err := t.fs.GetCacheFileReader(name)
	if err != nil {
		if os.IsNotExist(err) {
			tmp := fmt.Sprintf("%s.%s", name, uuid.Generate().String())
			if err := t.fs.CreateUploadFile(tmp, 0); err != nil {
				return nil, err
			}
			w, err := t.fs.GetUploadFileReadWriter(tmp)
			if err != nil {
				return nil, err
			}
			defer w.Close()

			if err := t.blobClient.Download(name, w); err != nil {
				return nil, fmt.Errorf("remote backend download: %s", err)
			}

			if err := t.fs.MoveUploadFileToCache(tmp, name); err != nil {
				if !os.IsExist(err) {
					return nil, err
				}
				// if file exists somebosy else is pulling the same blob
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

// Upload uploads blob to a default remote storage backend
// TODO(igor): remove blob and size parameters from the interface. Transferer should just read
// directly from a filestore
func (t *RemoteBackendTransferer) Upload(name string, blob store.FileReader, size int64) error {
	return t.blobClient.Upload(name, blob)
}

// GetTag gets manifest digest, given repo and tag.
func (t *RemoteBackendTransferer) GetTag(repo, tag string) (core.Digest, error) {
	name := fmt.Sprintf("%s:%s", repo, tag)
	if v, ok := t.cache.Get(name); ok {
		if b, ok := v.([]byte); ok {
			return core.NewDigestFromString(string(b))
		}
	}

	var b bytes.Buffer
	if err := t.tagClient.Download(name, &b); err != nil {
		return core.Digest{}, err
	}
	t.cache.ContainsOrAdd(name, b.Bytes())

	return core.NewDigestFromString(b.String())
}

// PostTag posts tag:manifest_digest mapping to addr given repo and tag.
func (t *RemoteBackendTransferer) PostTag(repo, tag string, manifestDigest core.Digest) error {
	r := strings.NewReader(manifestDigest.String())
	return t.tagClient.Upload(fmt.Sprintf("%s:%s", repo, tag), r)
}
