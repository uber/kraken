package dockerregistry

import (
	"io"
	"io/ioutil"

	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/infra/kraken/client/store"
	"code.uber.internal/infra/kraken/client/torrent"
	sd "github.com/docker/distribution/registry/storage/driver"
)

// Blobs b
type Blobs struct {
	client torrent.Client
	store  *store.LocalStore
}

// NewBlobs creates Blobs
func NewBlobs(cl torrent.Client, s *store.LocalStore) *Blobs {
	return &Blobs{
		client: cl,
		store:  s,
	}
}

func (b *Blobs) getBlobStat(fileName string) (sd.FileInfo, error) {
	info, err := b.store.GetCacheFileStat(fileName)
	if err != nil {
		err = b.client.DownloadTorrent(fileName)
		if err != nil {
			return nil, sd.PathNotFoundError{
				DriverName: "kraken",
				Path:       fileName,
			}
		}

		info, err = b.store.GetCacheFileStat(fileName)
		if err != nil {
			return nil, err
		}
	}

	fi := sd.FileInfoInternal{
		FileInfoFields: sd.FileInfoFields{
			Path:    info.Name(),
			Size:    info.Size(),
			ModTime: info.ModTime(),
			IsDir:   info.IsDir(),
		},
	}
	return fi, nil
}

func (b *Blobs) getOrDownloadBlobData(fileName string) (data []byte, err error) {
	// check cache
	reader, err := b.getOrDownloadBlobReader(fileName, 0)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	return ioutil.ReadAll(reader)
}

func (b *Blobs) getOrDownloadBlobReader(fileName string, offset int64) (reader io.ReadCloser, err error) {
	reader, err = b.getBlobReader(fileName, offset)
	if err != nil {
		err = b.client.DownloadTorrent(fileName)
		if err != nil {
			log.Errorf("Failed to download %s", err.Error())
			return nil, sd.PathNotFoundError{
				DriverName: "kraken",
				Path:       fileName,
			}
		}
		return b.getBlobReader(fileName, offset)
	}
	return reader, nil
}

func (b *Blobs) getBlobReader(fileName string, offset int64) (io.ReadCloser, error) {
	reader, err := b.store.GetCacheFileReader(fileName)
	if err != nil {
		return nil, err
	}

	// Set offset
	_, err = reader.Seek(offset, 0)
	if err != nil {
		reader.Close()
		return nil, err
	}

	return reader, nil
}
