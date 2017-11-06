package transfer

import "io"

// IOCloner provides access to distinct ReadClosers for a single ReadCloser.
// If clients need to read a blob file multiple times, a single reader is not
// sufficient because it can only be read once. Loading the blob file into a
// reusable buffer is not an option because the size of the blob may be in the
// magnitude of gigabytes. Thus, IOCloner allows clients to retrieve as many
// readers as they need.
type IOCloner interface {
	Clone() (io.ReadCloser, error)
}

// Downloader defines an interface to download blobs
type Downloader interface {
	Download(digest string) (io.ReadCloser, error)
}

// Uploader defines an interface to upload blobs
type Uploader interface {
	Upload(digest string, blobIO IOCloner, size int64) error
}

// ManifestManager defines an interface to get and post manifest
type ManifestManager interface {
	GetManifest(repo, tag string) (io.ReadCloser, error)
	PostManifest(repo, tag, digest string, reader io.Reader) error
}

// ImageTransferer defines an interface that transfers images
type ImageTransferer interface {
	Downloader
	Uploader
	ManifestManager
}
