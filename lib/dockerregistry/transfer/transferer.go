package transfer

import "io"

// Downloader defines an interface to download blobs
type Downloader interface {
	Download(digest string) (io.ReadCloser, error)
}

// Uploader defines an interface to upload blobs
type Uploader interface {
	Upload(digest string, reader io.Reader, size int64) error
}

// ManifestManager defines an interface to get and post manifest
type ManifestManager interface {
	GetManifest(repo, tag string) (readCloser io.ReadCloser, err error)
	PostManifest(repo, tag, digest string, reader io.Reader) error
}

// ImageTransferer defines an interface that transfers images
type ImageTransferer interface {
	Downloader
	Uploader
	ManifestManager
}
