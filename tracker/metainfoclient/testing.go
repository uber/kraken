package metainfoclient

import (
	"sync"

	"code.uber.internal/infra/kraken/torlib"
)

// TestClient is a thread-safe, in-memory client for simulating downloads.
type TestClient struct {
	sync.Mutex
	m map[string]*torlib.MetaInfo
}

// NewTestClient returns a new TestClient.
func NewTestClient() *TestClient {
	return &TestClient{m: make(map[string]*torlib.MetaInfo)}
}

// Upload "uploads" metainfo that can then be subsequently downloaded. Upload
// is not supported in the Client interface and exists soley for testing purposes.
func (c *TestClient) Upload(mi *torlib.MetaInfo) error {
	c.Lock()
	defer c.Unlock()
	if _, ok := c.m[mi.Name()]; ok {
		return ErrExists
	}
	c.m[mi.Name()] = mi
	return nil
}

// Download returns the metainfo for digest. Ignores namespace.
func (c *TestClient) Download(namespace string, name string) (*torlib.MetaInfo, error) {
	c.Lock()
	defer c.Unlock()
	mi, ok := c.m[name]
	if !ok {
		return nil, ErrNotFound
	}
	return mi, nil
}
