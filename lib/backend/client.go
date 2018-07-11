package backend

import (
	"io"

	"code.uber.internal/infra/kraken/core"
)

// Client defines an interface for accessing blobs on a remote storage backend.
//
// Implementations of Client must be thread-safe, since they are cached and
// used concurrently by Manager.
type Client interface {
	// Stat returns blob info for name. All implementations should return
	// backenderrors.ErrBlobNotFound when the blob was not found.
	//
	// Stat is useful when we need to quickly know if a blob exists (and maybe
	// some basic information about it), without downloading the entire blob,
	// which may be very large.
	Stat(name string) (*core.BlobInfo, error)

	// Upload uploads src into name.
	Upload(name string, src io.Reader) error

	// Download downloads name into dst. All implementations should return
	// backenderrors.ErrBlobNotFound when the blob was not found.
	Download(name string, dst io.Writer) error

	// List lists entries whose names start with prefix.
	List(prefix string) ([]string, error)
}
