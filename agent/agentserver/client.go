package agentserver

import (
	"fmt"
	"io"
	"net/http"
	"time"

	"code.uber.internal/infra/kraken/utils/backoff"
	"code.uber.internal/infra/kraken/utils/httputil"
)

// Client provides a wrapper for HTTP operations on an agent.
type Client struct {
	backoff *backoff.Backoff
	addr    string
}

type clientOpts struct {
	timeout time.Duration
}

// ClientOption defines an option for the Client.
type ClientOption func(*clientOpts)

// WithTimeout sets timeout t for Client downloads.
func WithTimeout(t time.Duration) ClientOption {
	return func(o *clientOpts) { o.timeout = t }
}

// NewClient creates a new client for an agent at addr.
func NewClient(addr string, opts ...ClientOption) *Client {

	settings := &clientOpts{
		timeout: 15 * time.Minute,
	}
	for _, opt := range opts {
		opt(settings)
	}

	b := backoff.New(backoff.Config{
		RetryTimeout: settings.timeout,
	})

	return &Client{b, addr}
}

// Download returns the blob for namespace / name. Callers should close the
// returned ReadCloser when done reading the blob.
func (c *Client) Download(namespace, name string) (io.ReadCloser, error) {
	a := c.backoff.Attempts()
	for a.WaitForNext() {
		resp, err := httputil.Get(
			fmt.Sprintf("http://%s/namespace/%s/blobs/%s", c.addr, namespace, name),
			httputil.SendAcceptedCodes(http.StatusOK, http.StatusAccepted))
		if err != nil {
			return nil, err
		}
		if resp.StatusCode == http.StatusAccepted {
			continue
		}
		return resp.Body, nil
	}
	return nil, a.Err()
}

// Delete deletes the torrent for name.
func (c *Client) Delete(name string) error {
	_, err := httputil.Delete(fmt.Sprintf("http://%s/blobs/%s", c.addr, name))
	return err
}
