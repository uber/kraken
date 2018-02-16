package hdfsbackend

import (
	"errors"
	"fmt"
	"io"
	"strings"
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

// Download downloads the value of name into dst. name should be in the
// format "repo:tag".
func (c *DockerTagClient) Download(name string, dst io.Writer) error {
	path, err := c.path(name)
	if err != nil {
		return err
	}
	return c.client.download(path, dst)
}

// Upload uploads src as the value of name. name should be in the format "repo:tag".
func (c *DockerTagClient) Upload(name string, src io.Reader) error {
	path, err := c.path(name)
	if err != nil {
		return err
	}
	return c.client.upload(path, src)
}
