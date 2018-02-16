package hdfsbackend

import (
	"errors"
	"fmt"
	"io"
)

// DockerBlobClient is an HDFS client for uploading / download blobs to a docker
// registry.
type DockerBlobClient struct {
	client *client
}

// NewDockerBlobClient creates a new DockerBlobClient.
func NewDockerBlobClient(config Config) (*DockerBlobClient, error) {
	config, err := config.applyDefaults()
	if err != nil {
		return nil, fmt.Errorf("invalid config: %s", err)
	}
	return &DockerBlobClient{newClient(config)}, nil
}

func (c *DockerBlobClient) path(name string) (string, error) {
	if len(name) < 2 {
		return "", errors.New("name is too short, must be >= 2 characters")
	}
	path := fmt.Sprintf(
		"webhdfs/v1/infra/dockerRegistry/docker/registry/v2/blobs/sha256/%s/%s/data",
		name[:2], name)
	return path, nil
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
