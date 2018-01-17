package backend

import "code.uber.internal/infra/kraken/lib/fileio"

// Uploader reads blobs from src and uploads them to name. Name should be
// the digest of the blob.
type Uploader interface {
	Upload(name string, src fileio.Reader) error
}

// Downloader downloads blobs under name and writes them to dst. Name should
// be the digest of the blob. Implementations of Download should return
// ErrBlobNotFound when the blob was not found.
type Downloader interface {
	Download(name string, dst fileio.Writer) error
}

// Client defines an interface for uploading and downloading blobs to a remote
// storage backend. The name parameter in these methods should be the digest
// of the blob being uploaded.
//
// Implementations of Client must be thread-safe, since they are cached and
// used concurrently by Manager.
type Client interface {
	Uploader
	Downloader
}
