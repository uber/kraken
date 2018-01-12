package agentserver

import (
	"fmt"
	"io"
	"net/http"
	"time"

	"code.uber.internal/infra/kraken/utils/backoff"
	"code.uber.internal/infra/kraken/utils/httputil"
)

// ClientConfig defines configuration for Client.
type ClientConfig struct {
	Backoff backoff.Config `yaml:"backoff"`
}

func (c ClientConfig) applyDefaults() ClientConfig {
	if c.Backoff.RetryTimeout == 0 {
		c.Backoff.RetryTimeout = 15 * time.Minute
	}
	return c
}

// Client provides a wrapper for HTTP operations on an agent.
type Client struct {
	config  ClientConfig
	backoff *backoff.Backoff
	addr    string
}

// NewClient creates a new client for an agent at addr.
func NewClient(config ClientConfig, addr string) *Client {
	return &Client{config.applyDefaults(), backoff.New(config.Backoff), addr}
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
