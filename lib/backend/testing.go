package backend

import (
	"io/ioutil"
	"sync"

	"code.uber.internal/infra/kraken/lib/fileio"
)

type testClient struct {
	sync.Mutex
	blobs map[string][]byte
}

// TestClient returns a thread-safe, in-memory implementation of Client for
// testing purposes.
func TestClient() Client {
	return &testClient{blobs: make(map[string][]byte)}
}

func (c *testClient) Upload(name string, src fileio.Reader) error {
	c.Lock()
	defer c.Unlock()

	b, err := ioutil.ReadAll(src)
	if err != nil {
		return err
	}
	c.blobs[name] = b
	return nil
}

func (c *testClient) Download(name string, dst fileio.Writer) error {
	c.Lock()
	defer c.Unlock()

	b, ok := c.blobs[name]
	if !ok {
		return ErrBlobNotFound
	}
	_, err := dst.Write(b)
	return err
}
