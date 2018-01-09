package agentserver

import (
	"errors"
	"fmt"
	"io"
	"time"

	"code.uber.internal/infra/kraken/utils/backoff"
	"code.uber.internal/infra/kraken/utils/httputil"
)

// ClientConfig defines configuration for Client.
type ClientConfig struct {
	Backoff backoff.Config `yaml:"backoff"`
	Timeout time.Duration  `yaml:"timeout"`
}

func (c ClientConfig) applyDefaults() ClientConfig {
	if c.Timeout == 0 {
		c.Timeout = 15 * time.Minute
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
	timer := time.NewTimer(c.config.Timeout)
	defer timer.Stop()

	var attempt int
	for {
		resp, err := httputil.Get(
			fmt.Sprintf("http://%s/namespace/%s/blobs/%s", c.addr, namespace, name))
		if err == nil {
			return resp.Body, nil
		}
		if !httputil.IsAccepted(err) {
			return nil, err
		}
		select {
		case <-time.After(c.backoff.Duration(attempt)):
		case <-timer.C:
			return nil, errors.New("retries timed out")
		}
		attempt++
	}
}
