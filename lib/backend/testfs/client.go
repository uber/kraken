package testfs

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"code.uber.internal/infra/kraken/lib/backend/backenderrors"
	"code.uber.internal/infra/kraken/lib/fileio"
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

func (c *Client) upload(name string, src io.Reader) error {
	_, err := httputil.Post(
		fmt.Sprintf("http://%s/files/%s", c.config.Addr, name),
		httputil.SendBody(src))
	return err
}

// UploadFile uploads src to name.
func (c *Client) UploadFile(name string, src fileio.Reader) error {
	return c.upload(name, src)
}

// UploadBytes uploads b to name.
func (c *Client) UploadBytes(name string, b []byte) error {
	return c.upload(name, bytes.NewReader(b))
}

func (c *Client) download(name string, dst io.Writer) error {
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

// DownloadFile downloads name to dst.
func (c *Client) DownloadFile(name string, dst fileio.Writer) error {
	return c.download(name, dst)
}

// DownloadBytes downloads name.
func (c *Client) DownloadBytes(name string) ([]byte, error) {
	var b bytes.Buffer
	if err := c.download(name, &b); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}
