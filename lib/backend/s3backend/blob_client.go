package s3backend

import (
	"io"
	"path"
)

// Client implements download / uploading blobs in a single directory from / to S3.
type Client struct {
	config Config
	client *client
}

// NewClient returns a new Client.
func NewClient(config Config, auth AuthConfig, ns string) (*Client, error) {
	client, err := newClient(config, auth, ns)
	if err != nil {
		return nil, err
	}
	return &Client{config, client}, nil
}

// Download downloads name into dst.
func (c *Client) Download(name string, dst io.Writer) error {
	return c.client.download(path.Join(c.config.RootDirectory, name), dst)
}

// Upload uploads src into name.
func (c *Client) Upload(name string, src io.Reader) error {
	return c.client.upload(path.Join(c.config.RootDirectory, name), src)
}
