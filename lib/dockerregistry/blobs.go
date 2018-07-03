package dockerregistry

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"time"

	"code.uber.internal/infra/kraken/lib/dockerregistry/transfer"
	"code.uber.internal/infra/kraken/lib/store"

	"github.com/docker/distribution/context"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
)

// BlobStore defines cache file accessors.
type BlobStore interface {
	GetCacheFileStat(name string) (os.FileInfo, error)
	GetCacheFileReader(name string) (store.FileReader, error)
}

type blobs struct {
	bs         BlobStore
	transferer transfer.ImageTransferer
}

func newBlobs(bs BlobStore, transferer transfer.ImageTransferer) *blobs {
	return &blobs{bs, transferer}
}

// getDigest returns blob digest given a blob path.
func (b *blobs) getDigest(path string) ([]byte, error) {
	digest, err := GetLayerDigest(path)
	if err != nil {
		return nil, err
	}

	return []byte(digest.String()), nil
}

func (b *blobs) stat(ctx context.Context, path string) (storagedriver.FileInfo, error) {
	repo, err := parseRepo(ctx)
	if err != nil {
		return nil, fmt.Errorf("parse repo %s: %s", path, err)
	}
	digest, err := GetBlobDigest(path)
	if err != nil {
		return nil, err
	}
	bi, err := b.transferer.Stat(repo, digest)
	if err != nil {
		if err == transfer.ErrBlobNotFound {
			return nil, storagedriver.PathNotFoundError{
				DriverName: "kraken",
				Path:       digest.Hex(),
			}
		}
		return nil, fmt.Errorf("transferer stat: %s", err)
	}
	// Hacking the path, since kraken storage driver is also the consumer of this info.
	// Instead of the relative path from root that docker registry expected, just use content hash.
	return storagedriver.FileInfoInternal{
		FileInfoFields: storagedriver.FileInfoFields{
			Path:    digest.Hex(),
			Size:    bi.Size,
			ModTime: time.Now(),
			IsDir:   false,
		},
	}, nil
}

func (b *blobs) reader(ctx context.Context, path string, offset int64) (io.ReadCloser, error) {
	return b.getCacheReaderHelper(ctx, path, offset)
}

func (b *blobs) getContent(ctx context.Context, path string) ([]byte, error) {
	r, err := b.getCacheReaderHelper(ctx, path, 0)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return ioutil.ReadAll(r)
}

func (b *blobs) getCacheReaderHelper(
	ctx context.Context, path string, offset int64) (io.ReadCloser, error) {

	repo, err := parseRepo(ctx)
	if err != nil {
		return nil, fmt.Errorf("parse repo %s: %s", path, err)
	}

	digest, err := GetBlobDigest(path)
	if err != nil {
		return nil, fmt.Errorf("get layer digest %s: %s", path, err)
	}

	r, err := b.transferer.Download(repo, digest)
	if err != nil {
		if err == transfer.ErrBlobNotFound {
			return nil, storagedriver.PathNotFoundError{
				DriverName: "kraken",
				Path:       digest.Hex(),
			}
		}
		return nil, fmt.Errorf("transferer download: %s", err)
	}

	if _, err := r.Seek(offset, 0); err != nil {
		return nil, fmt.Errorf("seek: %s", err)
	}
	return r, nil
}

func parseRepo(ctx context.Context) (string, error) {
	repo := context.GetStringValue(ctx, "vars.name")
	if repo == "" {
		return "", fmt.Errorf("Failed to parse context for repo name")
	}
	return repo, nil
}
