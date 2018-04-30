package dockerregistry

import (
	"fmt"
	"io"
	"io/ioutil"

	"code.uber.internal/infra/kraken/lib/dockerregistry/transfer"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/utils/log"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
)

// Blobs b
type Blobs struct {
	transferer transfer.ImageTransferer
	store      store.FileStore
}

// NewBlobs creates Blobs
func NewBlobs(transferer transfer.ImageTransferer, s store.FileStore) *Blobs {
	return &Blobs{
		transferer: transferer,
		store:      s,
	}
}

// GetStat returns fileinfo for the blobs
func (b *Blobs) GetStat(path string) (storagedriver.FileInfo, error) {
	digest, err := GetBlobDigest(path)
	if err != nil {
		return nil, err
	}
	return b.getBlobStat(digest.Hex())
}

// GetReader returns a reader to the blob
func (b *Blobs) GetReader(path string, offset int64) (io.ReadCloser, error) {
	digest, err := GetBlobDigest(path)
	if err != nil {
		return nil, err
	}
	return b.getBlobReader(digest.Hex(), offset)
}

// GetContent returns blob content in bytes
func (b *Blobs) GetContent(path string) ([]byte, error) {
	digest, err := GetBlobDigest(path)
	if err != nil {
		return nil, err
	}
	blob, err := b.getBlobReader(digest.Hex(), 0)
	if err != nil {
		return nil, err
	}
	defer blob.Close()
	return ioutil.ReadAll(blob)
}

// GetDigest downloads layer and returns layer digest.
// This is the only place storage driver would download a layer blob via
// torrent scheduler or origin because it has namespace information.
// The caller of storage driver would first call this function to resolve
// the layer link (and downloads layer blob),
// then call Stat or Reader which would assume the blob is on disk already.
func (b *Blobs) GetDigest(path string) ([]byte, error) {
	repo, err := GetRepo(path)
	if err != nil {
		return nil, fmt.Errorf("get repo: %s", err)
	}

	digest, err := GetLayerDigest(path)
	if err != nil {
		return nil, err
	}

	blob, err := b.transferer.Download(repo, digest.Hex())
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

func (b *Blobs) getBlobStat(fileName string) (storagedriver.FileInfo, error) {
	info, err := b.store.GetCacheFileStat(fileName)
	if err != nil {
		log.Errorf("Failed to state %s: %s", fileName, err)
		return nil, storagedriver.PathNotFoundError{
			DriverName: "kraken",
			Path:       fileName,
		}
	}

	// Hacking the path, since kraken storage driver is also the consumer of this info.
	// Instead of the relative path from root that docker registry expected, just use content hash.
	fi := storagedriver.FileInfoInternal{
		FileInfoFields: storagedriver.FileInfoFields{
			Path:    fileName,
			Size:    info.Size(),
			ModTime: info.ModTime(),
			IsDir:   info.IsDir(),
		},
	}
	return fi, nil
}

func (b *Blobs) getBlobReader(fileName string, offset int64) (io.ReadCloser, error) {
	blob, err := b.store.GetCacheFileReader(fileName)
	if err != nil {
		log.Errorf("Failed to state %s: %s", fileName, err)
		return nil, storagedriver.PathNotFoundError{
			DriverName: "kraken",
			Path:       fileName,
		}
	}

	if _, err := blob.Seek(offset, 0); err != nil {
		blob.Close()
		return nil, err
	}
	return blob, nil
}
