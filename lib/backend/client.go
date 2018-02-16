package backend

import "io"

// Client defines an interface for uploading and downloading blobs to a remote
// storage backend.
//
// Implementations of Client must be thread-safe, since they are cached and
// used concurrently by Manager.
type Client interface {
	// Upload uploads src into name.
	Upload(name string, src io.Reader) error

	// Download downloads name into dst. All implementations should return
	// backenderrors.ErrBlobNotFound when the blob was not found.
	Download(name string, dst io.Writer) error
}
