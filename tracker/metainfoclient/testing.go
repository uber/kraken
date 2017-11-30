package metainfoclient

import (
	"sync"

	"code.uber.internal/infra/kraken/torlib"
)

type testClient struct {
	sync.Mutex
	m map[string]*torlib.MetaInfo
}

// TestClient returns an thread-safe, in-memory client for simulating upload / download.
func TestClient() Client {
	return &testClient{m: make(map[string]*torlib.MetaInfo)}
}

func (c *testClient) Upload(mi *torlib.MetaInfo) error {
	c.Lock()
	defer c.Unlock()
	if _, ok := c.m[mi.Name()]; ok {
		return ErrExists
	}
	c.m[mi.Name()] = mi
	return nil
}

func (c *testClient) Download(name string) (*torlib.MetaInfo, error) {
	c.Lock()
	defer c.Unlock()
	mi, ok := c.m[name]
	if !ok {
		return nil, ErrNotFound
	}
	return mi, nil
}
