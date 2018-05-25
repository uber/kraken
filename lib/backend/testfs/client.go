package testfs

import (
	"errors"
	"fmt"
	"io"
	"strconv"

	"code.uber.internal/infra/kraken/lib/backend/backenderrors"
	"code.uber.internal/infra/kraken/lib/backend/blobinfo"
	"code.uber.internal/infra/kraken/utils/httputil"
)

// Config defines Client configuration.
type Config struct {
	Addr string `yaml:"addr"`
}

// Client wraps HTTP calls to Server.
type Client struct {
	config Config
}

// NewClient returns a new Client.
func NewClient(config Config) (*Client, error) {
	if config.Addr == "" {
		return nil, errors.New("no addr configured")
	}
	return &Client{config}, nil
}

// Addr returns the configured server address.
func (c *Client) Addr() string {
	return c.config.Addr
}

// Stat returns blob info for name.
func (c *Client) Stat(name string) (*blobinfo.Info, error) {
	resp, err := httputil.Head(fmt.Sprintf("http://%s/files/%s", c.config.Addr, name))
	if err != nil {
		if httputil.IsNotFound(err) {
			return nil, backenderrors.ErrBlobNotFound
		}
		return nil, err
	}
	size, err := strconv.ParseInt(resp.Header.Get("Size"), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parse size: %s", err)
	}
	return blobinfo.New(size), nil
}

// Upload uploads src to name.
func (c *Client) Upload(name string, src io.Reader) error {
	_, err := httputil.Post(
		fmt.Sprintf("http://%s/files/%s", c.config.Addr, name),
		httputil.SendBody(src))
	return err
}

// Download downloads name to dst.
func (c *Client) Download(name string, dst io.Writer) error {
	resp, err := httputil.Get(fmt.Sprintf("http://%s/files/%s", c.config.Addr, name))
	if err != nil {
		if httputil.IsNotFound(err) {
			return backenderrors.ErrBlobNotFound
		}
		return fmt.Errorf("http: %s", err)
	}
	defer resp.Body.Close()
	if _, err := io.Copy(dst, resp.Body); err != nil {
		return fmt.Errorf("copy: %s", err)
	}
	return nil
}
