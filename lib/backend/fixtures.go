package backend

import (
	"errors"
	"io"
	"io/ioutil"
	"sync"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/backend/backenderrors"
)

// ManagerFixture returns a Manager with no clients for testing purposes.
func ManagerFixture() *Manager {
	m, err := NewManager(nil, AuthConfig{})
	if err != nil {
		panic(err)
	}
	return m
}

// TestClient is a backend client for testing.
type TestClient struct {
	sync.Mutex
	blobs map[string][]byte
}

// ClientFixture returns an in-memory Client for testing purposes.
func ClientFixture() Client {
	return &TestClient{blobs: make(map[string][]byte)}
}

// Stat returns blob info for name.
func (c *TestClient) Stat(namespace, name string) (*core.BlobInfo, error) {
	c.Lock()
	defer c.Unlock()

	b, ok := c.blobs[name]
	if !ok {
		return nil, backenderrors.ErrBlobNotFound
	}
	return core.NewBlobInfo(int64(len(b))), nil
}

// Upload uploads src to name.
func (c *TestClient) Upload(namespace, name string, src io.Reader) error {
	c.Lock()
	defer c.Unlock()

	b, err := ioutil.ReadAll(src)
	if err != nil {
		return err
	}
	c.blobs[name] = b
	return nil
}

// Download downloads name into dst.
func (c *TestClient) Download(namespace, name string, dst io.Writer) error {
	c.Lock()
	defer c.Unlock()

	b, ok := c.blobs[name]
	if !ok {
		return backenderrors.ErrBlobNotFound
	}
	_, err := dst.Write(b)
	return err
}

// List lists names with start with prefix.
func (c *TestClient) List(dir string) ([]string, error) {
	return nil, errors.New("not supported")
}
