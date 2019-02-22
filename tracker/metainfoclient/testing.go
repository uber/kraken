package metainfoclient

import (
	"errors"
	"sync"

	"github.com/uber/kraken/core"
)

// TestClient is a thread-safe, in-memory client for simulating downloads.
type TestClient struct {
	sync.Mutex
	m map[core.Digest]*core.MetaInfo
}

// NewTestClient returns a new TestClient.
func NewTestClient() *TestClient {
	return &TestClient{m: make(map[core.Digest]*core.MetaInfo)}
}

// Upload "uploads" metainfo that can then be subsequently downloaded. Upload
// is not supported in the Client interface and exists soley for testing purposes.
func (c *TestClient) Upload(mi *core.MetaInfo) error {
	c.Lock()
	defer c.Unlock()
	if _, ok := c.m[mi.Digest()]; ok {
		return errors.New("metainfo already exists")
	}
	c.m[mi.Digest()] = mi
	return nil
}

// Download returns the metainfo for digest. Ignores namespace.
func (c *TestClient) Download(namespace string, d core.Digest) (*core.MetaInfo, error) {
	c.Lock()
	defer c.Unlock()
	mi, ok := c.m[d]
	if !ok {
		return nil, ErrNotFound
	}
	return mi, nil
}
