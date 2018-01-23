package transfer

import (
	"fmt"
	"io"
	"os"

	"code.uber.internal/infra/kraken/lib/backend"
	"code.uber.internal/infra/kraken/lib/dockerregistry/transfer/manifestclient"
	"code.uber.internal/infra/kraken/lib/store"
	"github.com/docker/distribution/uuid"
)

// RemoteBackendTransferer wraps transferring blobs to/from remote storage backend.
type RemoteBackendTransferer struct {
	fs     store.FileStore
	mc     manifestclient.Client
	client backend.Client
}

// NewRemoteBackendTransferer creates a new RemoteBackendTransferer.
func NewRemoteBackendTransferer(
	manifestClient manifestclient.Client,
	client backend.Client,
	fs store.FileStore) (*RemoteBackendTransferer, error) {

	return &RemoteBackendTransferer{
		fs:     fs,
		mc:     manifestClient,
		client: client,
	}, nil
}

// Download downloads the blob of name into the file store and returns a reader
// to the newly downloaded file.
func (t *RemoteBackendTransferer) Download(name string) (store.FileReader, error) {
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

			err = t.client.Download(name, w)
			if err != nil {
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
	return t.client.Upload(name, blob)
}

// GetManifest gets and saves manifest given addr, repo and tag
func (t *RemoteBackendTransferer) GetManifest(repo, tag string) (io.ReadCloser, error) {
	return t.mc.GetManifest(repo, tag)
}

// PostManifest posts manifest to addr given repo and tag
func (t *RemoteBackendTransferer) PostManifest(repo, tag string, manifest io.Reader) error {
	return t.mc.PostManifest(repo, tag, manifest)
}
