package dockerregistry

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"

	"code.uber.internal/infra/kraken/lib/dockerregistry/transfer"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/utils/log"
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

func (b *blobs) stat(path string) (storagedriver.FileInfo, error) {
	digest, err := GetBlobDigest(path)
	if err != nil {
		return nil, err
	}
	info, err := b.bs.GetCacheFileStat(digest.Hex())
	if err != nil {
		log.Errorf("Error stat-ing %s: %s", digest.Hex(), err)
		return nil, storagedriver.PathNotFoundError{
			DriverName: "kraken",
			Path:       digest.Hex(),
		}
	}
	// Hacking the path, since kraken storage driver is also the consumer of this info.
	// Instead of the relative path from root that docker registry expected, just use content hash.
	return storagedriver.FileInfoInternal{
		FileInfoFields: storagedriver.FileInfoFields{
			Path:    digest.Hex(),
			Size:    info.Size(),
			ModTime: info.ModTime(),
			IsDir:   info.IsDir(),
		},
	}, nil
}

func (b *blobs) reader(path string, offset int64) (io.ReadCloser, error) {
	digest, err := GetBlobDigest(path)
	if err != nil {
		return nil, err
	}
	return b.getCacheReader(digest.Hex(), offset)
}

func (b *blobs) getContent(path string) ([]byte, error) {
	digest, err := GetBlobDigest(path)
	if err != nil {
		return nil, err
	}
	r, err := b.getCacheReader(digest.Hex(), 0)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return ioutil.ReadAll(r)
}

// getDigest the only place storage driver would download a layer blob via
// torrent scheduler or origin because it has namespace information.
// The caller of storage driver would first call this function to resolve
// the layer link (and downloads layer blob),
// then call Stat or Reader which would assume the blob is on disk already.
func (b *blobs) getDigest(path string) ([]byte, error) {
	repo, err := GetRepo(path)
	if err != nil {
		return nil, fmt.Errorf("get repo: %s", err)
	}
	digest, err := GetLayerDigest(path)
	if err != nil {
		return nil, err
	}

	blob, err := b.transferer.Download(repo, digest)
	if err != nil {
		log.Errorf("Failed to download %s: %s", digest, err)
		return nil, storagedriver.PathNotFoundError{
			DriverName: "kraken",
			Path:       digest.String(),
		}
	}
	defer blob.Close()

	return []byte(digest.String()), nil
}

func (b *blobs) getCacheReader(name string, offset int64) (io.ReadCloser, error) {
	r, err := b.bs.GetCacheFileReader(name)
	if err != nil {
		log.Errorf("Error getting reader for %s: %s", name, err)
		return nil, storagedriver.PathNotFoundError{
			DriverName: "kraken",
			Path:       name,
		}
	}
	if _, err := r.Seek(offset, 0); err != nil {
		r.Close()
		return nil, fmt.Errorf("seek: %s", err)
	}
	return r, nil
}
