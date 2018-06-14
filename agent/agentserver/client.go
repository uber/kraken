package agentserver

import (
	"fmt"
	"io"
	"net/http"
	"net/url"

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

// Download returns the blob for namespace / name. Callers should close the
// returned ReadCloser when done reading the blob.
func (c *Client) Download(namespace, name string) (io.ReadCloser, error) {
	resp, err := httputil.Get(fmt.Sprintf(
		"http://%s/namespace/%s/blobs/%s",
		c.addr,
		url.PathEscape(namespace),
		name),
		httputil.SendAcceptedCodes(http.StatusOK, http.StatusAccepted))
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

// Delete deletes the torrent for name.
func (c *Client) Delete(name string) error {
	_, err := httputil.Delete(fmt.Sprintf("http://%s/blobs/%s", c.addr, name))
	return err
}
