package s3backend

import (
	"io"

	"code.uber.internal/infra/kraken/lib/backend/nameparse"
)

// DockerBlobClient implements downloading/uploading object from/to S3
type DockerBlobClient struct {
	config Config
	client *client
}

// NewDockerBlobClient creates s3 blob client from input parameters
func NewDockerBlobClient(config Config) (*DockerBlobClient, error) {
	client, err := newClient(config)
	if err != nil {
		return nil, err
	}
	return &DockerBlobClient{client: client}, nil
}

func (c *DockerBlobClient) path(name string) (string, error) {
	return nameparse.ShardDigestPath(c.config.RootDirectory, name)
}

// Download downloads a blob for name into dst. name should be a sha256 digest
// of the desired blob.
func (c *DockerBlobClient) Download(name string, dst io.Writer) error {
	path, err := c.path(name)
	if err != nil {
		return err
	}
	return c.client.download(path, dst)
}

// Upload uploads src to name. name should be a sha256 digest of src.
func (c *DockerBlobClient) Upload(name string, src io.Reader) error {
	path, err := c.path(name)
	if err != nil {
		return err
	}
	return c.client.upload(path, src)
}
