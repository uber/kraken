package backend

import (
	"fmt"
	"io"

	"code.uber.internal/infra/kraken/core"
)

var _factories = make(map[string]ClientFactory)

// ClientFactory creates backend client given name.
type ClientFactory interface {
	Create(config interface{}, authConfig interface{}) (Client, error)
}

// Register registers new Factory with corresponding backend client name.
func Register(name string, factory ClientFactory) {
	_factories[name] = factory
}

// getFactory returns backend client factory given client name.
func getFactory(name string) (ClientFactory, error) {
	factory, ok := _factories[name]
	if !ok {
		return nil, fmt.Errorf("no backend client defined with name %s", name)
	}
	return factory, nil
}

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
