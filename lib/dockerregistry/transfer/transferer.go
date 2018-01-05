package transfer

import (
	"io"

	"code.uber.internal/infra/kraken/lib/store"
)

// Downloader defines an interface to download blobs
type Downloader interface {
	Download(name string) (store.FileReader, error)
}

// Uploader defines an interface to upload blobs
type Uploader interface {
	Upload(name string, blob store.FileReader) error
}

// ManifestManager defines an interface to get and post manifest
type ManifestManager interface {
	GetManifest(repo, tag string) (io.ReadCloser, error)
	PostManifest(repo, tag string, reader io.Reader) error
}

// ImageTransferer defines an interface that transfers images
type ImageTransferer interface {
	Downloader
	Uploader
	ManifestManager
}
