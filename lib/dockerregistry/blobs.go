package dockerregistry

import (
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
	return b.getBlobStat(digest)
}

// GetReader returns a reader to the blob
func (b *Blobs) GetReader(path string, offset int64) (io.ReadCloser, error) {
	name, err := GetBlobDigest(path)
	if err != nil {
		return nil, err
	}
	return b.getBlobReader(name, offset)
}

// GetContent returns blob content in bytes
func (b *Blobs) GetContent(path string) ([]byte, error) {
	name, err := GetBlobDigest(path)
	if err != nil {
		return nil, err
	}
	blob, err := b.getBlobReader(name, 0)
	if err != nil {
		return nil, err
	}
	defer blob.Close()
	return ioutil.ReadAll(blob)
}

// GetDigest returns layer sha
func (b *Blobs) GetDigest(path string) ([]byte, error) {
	layerDigest, err := GetLayerDigest(path)
	if err != nil {
		return nil, err
	}
	return []byte("sha256:" + layerDigest), nil
}

func (b *Blobs) getBlobStat(fileName string) (storagedriver.FileInfo, error) {
	info, err := b.store.GetCacheFileStat(fileName)
	if err != nil {
		readCloser, err := b.transferer.Download(fileName)
		if err != nil {
			log.Errorf("failed to download %s: %s", fileName, err)
			return nil, storagedriver.PathNotFoundError{
				DriverName: "kraken",
				Path:       fileName,
			}
		}
		defer readCloser.Close()

		err = b.store.CreateCacheFile(fileName, readCloser)
		if err != nil {
			return nil, err
		}

		info, err = b.store.GetCacheFileStat(fileName)
		if err != nil {
			return nil, err
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
		blob, err = b.transferer.Download(fileName)
		if err != nil {
			log.Errorf("failed to download %s: %s", fileName, err)
			return nil, storagedriver.PathNotFoundError{
				DriverName: "kraken",
				Path:       fileName,
			}
		}
	}
	if _, err := blob.Seek(offset, 0); err != nil {
		blob.Close()
		return nil, err
	}
	return blob, nil
}
