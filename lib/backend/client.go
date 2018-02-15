package backend

import "code.uber.internal/infra/kraken/lib/fileio"

// Uploader uploads blobs.
type Uploader interface {
	// UploadFile reads a blob from src and uploads it to name.
	UploadFile(name string, src fileio.Reader) error

	// UploadBytes uploads b to name. Should only be used for very small blobs.
	UploadBytes(name string, b []byte) error
}

// Downloader downloads blobs. All implementations should return
// backenderrors.ErrBlobNotFound when the blob was not found.
type Downloader interface {
	// DownloadFile downloads blob for name and writes it to dst.
	DownloadFile(name string, dst fileio.Writer) error

	// DownloadBytes downloads blob for name. Should only be used for very
	// small blobs.
	DownloadBytes(name string) ([]byte, error)
}

// Client defines an interface for uploading and downloading blobs to a remote
// storage backend.
//
// Implementations of Client must be thread-safe, since they are cached and
// used concurrently by Manager.
type Client interface {
	Uploader
	Downloader
}
