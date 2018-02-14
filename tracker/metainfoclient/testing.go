package metainfoclient

import (
	"errors"
	"sync"

	"code.uber.internal/infra/kraken/core"
)

// TestClient is a thread-safe, in-memory client for simulating downloads.
type TestClient struct {
	sync.Mutex
	m map[string]*core.MetaInfo
}

// NewTestClient returns a new TestClient.
func NewTestClient() *TestClient {
	return &TestClient{m: make(map[string]*core.MetaInfo)}
}

// Upload "uploads" metainfo that can then be subsequently downloaded. Upload
// is not supported in the Client interface and exists soley for testing purposes.
func (c *TestClient) Upload(mi *core.MetaInfo) error {
	c.Lock()
	defer c.Unlock()
	if _, ok := c.m[mi.Name()]; ok {
		return errors.New("metainfo already exists")
	}
	c.m[mi.Name()] = mi
	return nil
}

// Download returns the metainfo for digest. Ignores namespace.
func (c *TestClient) Download(namespace string, name string) (*core.MetaInfo, error) {
	c.Lock()
	defer c.Unlock()
	mi, ok := c.m[name]
	if !ok {
		return nil, ErrNotFound
	}
	return mi, nil
}
