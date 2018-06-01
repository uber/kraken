package backend

import (
	"io"

	"code.uber.internal/infra/kraken/lib/backend/backenderrors"
	"code.uber.internal/infra/kraken/lib/backend/blobinfo"
)

// NoopNamespace is a special namespace which always returns a NoopClient.
const NoopNamespace = "__noop__"

// NoopClient is a special Client whose uploads always succeeds and whose blob
// lookups always 404. It is useful for users who want to operate on blobs that
// will be temporarily stored in the origin cluster and not backed up in remote
// storage.
type NoopClient struct{}

// Stat always returns ErrBlobNotFound.
func (c NoopClient) Stat(name string) (*blobinfo.Info, error) {
	return nil, backenderrors.ErrBlobNotFound
}

// Upload always returns nil.
func (c NoopClient) Upload(name string, src io.Reader) error {
	return nil
}

// Download always returns ErrBlobNotFound.
func (c NoopClient) Download(name string, dst io.Writer) error {
	return backenderrors.ErrBlobNotFound
}
