package backend

import "code.uber.internal/infra/kraken/lib/fileio"

// Uploader reads blobs from src and uploads them to name. Name should be
// the digest of the blob.
type Uploader interface {
	Upload(r fileio.Reader, dst string) error
}

// Downloader downloads blobs under name and writes them to dst. Name should
// be the digest of the blob.
type Downloader interface {
	Download(w fileio.Writer, src string) error
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
