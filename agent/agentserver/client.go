package agentserver

import (
	"fmt"
	"io"
	"net/url"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/utils/httputil"
)

// Client provides a wrapper for HTTP operations on an agent.
type Client struct {
	addr string
}

// NewClient creates a new client for an agent at addr.
func NewClient(addr string) *Client {
	return &Client{addr}
}

// Download returns the blob for namespace / d. Callers should close the
// returned ReadCloser when done reading the blob.
func (c *Client) Download(namespace string, d core.Digest) (io.ReadCloser, error) {
	resp, err := httputil.Get(
		fmt.Sprintf(
			"http://%s/namespace/%s/blobs/%s",
			c.addr, url.PathEscape(namespace), d))
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

// Delete deletes the torrent for d.
func (c *Client) Delete(d core.Digest) error {
	_, err := httputil.Delete(fmt.Sprintf("http://%s/blobs/%s", c.addr, d))
	return err
}
