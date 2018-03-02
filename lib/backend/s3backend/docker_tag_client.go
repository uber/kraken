package s3backend

import (
	"fmt"
	"io"

	"code.uber.internal/infra/kraken/lib/backend/nameparse"
)

// DockerTagClient is a tag client for s3
type DockerTagClient struct {
	client *client
}

// NewDockerTagClient creates a new DockerTagClient.
func NewDockerTagClient(config Config, auth AuthConfig, ns string) (*DockerTagClient, error) {
	client, err := newClient(config, auth, ns)
	if err != nil {
		return nil, fmt.Errorf("invalid config: %s", err)
	}
	return &DockerTagClient{client: client}, nil
}

func (c *DockerTagClient) path(name string) (string, error) {
	return nameparse.RepoTagPath(c.client.config.RootDirectory, name)
}

// Download downloads a blob for name into dst. name should be a sha256 digest
// of the desired blob.
func (c *DockerTagClient) Download(name string, dst io.Writer) error {
	path, err := c.path(name)
	if err != nil {
		return err
	}
	return c.client.download(path, dst)
}

// Upload uploads src to name. name should be a sha256 digest of src.
func (c *DockerTagClient) Upload(name string, src io.Reader) error {
	path, err := c.path(name)
	if err != nil {
		return err
	}
	return c.client.upload(path, src)
}
