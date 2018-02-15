package hdfsbackend

import (
	"bytes"
	"errors"
	"fmt"
	"strings"

	"code.uber.internal/infra/kraken/lib/fileio"
)

// DockerTagClient is an HDFS client for uploading / downloading tags to a docker
// registry.
type DockerTagClient struct {
	client *client
}

// NewDockerTagClient creates a new DockerTagClient.
func NewDockerTagClient(config Config) (*DockerTagClient, error) {
	config, err := config.applyDefaults()
	if err != nil {
		return nil, fmt.Errorf("invalid config: %s", err)
	}
	return &DockerTagClient{newClient(config)}, nil
}

func (c *DockerTagClient) path(name string) (string, error) {
	tokens := strings.Split(name, ":")
	if len(tokens) != 2 {
		return "", errors.New("name must be in format 'repo:tag'")
	}
	repo := tokens[0]
	tag := tokens[1]
	if len(repo) == 0 {
		return "", errors.New("repo must be non-empty")
	}
	if len(tag) == 0 {
		return "", errors.New("tag must be non-empty")
	}
	path := fmt.Sprintf(
		"webhdfs/v1/infra/dockerRegistry/docker/registry/v2/repositories/%s/_manifests/tags/%s/current/link",
		repo, tag)
	return path, nil
}

// DownloadFile downloads the value of name into dst. name should be in the
// format "repo:tag".
func (c *DockerTagClient) DownloadFile(name string, dst fileio.Writer) error {
	path, err := c.path(name)
	if err != nil {
		return err
	}
	return c.client.download(path, dst)
}

// DownloadBytes is the same as DownloadFile, but returns data directly.
func (c *DockerTagClient) DownloadBytes(name string) ([]byte, error) {
	path, err := c.path(name)
	if err != nil {
		return nil, err
	}
	return c.client.downloadBytes(path)
}

// UploadFile uploads src as the value of name. name should be in the format "repo:tag".
func (c *DockerTagClient) UploadFile(name string, src fileio.Reader) error {
	path, err := c.path(name)
	if err != nil {
		return err
	}
	return c.client.upload(path, src)
}

// UploadBytes is the same as UploadFile, but uploads data from an in-memory buffer.
func (c *DockerTagClient) UploadBytes(name string, b []byte) error {
	path, err := c.path(name)
	if err != nil {
		return err
	}
	return c.client.upload(path, bytes.NewReader(b))
}
