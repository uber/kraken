package hdfsbackend

import (
	"bytes"
	"errors"
	"fmt"

	"code.uber.internal/infra/kraken/lib/fileio"
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

// DownloadFile downloads a blob for name into dst. name should be a sha256 digest
// of the desired blob.
func (c *DockerBlobClient) DownloadFile(name string, dst fileio.Writer) error {
	path, err := c.path(name)
	if err != nil {
		return err
	}
	return c.client.download(path, dst)
}

// DownloadBytes is the same as DownloadFile, but returns data directly. Should
// only be used for small blobs.
func (c *DockerBlobClient) DownloadBytes(name string) ([]byte, error) {
	path, err := c.path(name)
	if err != nil {
		return nil, err
	}
	return c.client.downloadBytes(path)
}

// UploadFile uploads src to name. name should be a sha256 digest of src.
func (c *DockerBlobClient) UploadFile(name string, src fileio.Reader) error {
	path, err := c.path(name)
	if err != nil {
		return err
	}
	return c.client.upload(path, src)
}

// UploadBytes is the same as UploadFile, but uploads data from an in-memory
// buffer. Should only be used for small blobs.
func (c *DockerBlobClient) UploadBytes(name string, b []byte) error {
	path, err := c.path(name)
	if err != nil {
		return err
	}
	return c.client.upload(path, bytes.NewReader(b))
}
