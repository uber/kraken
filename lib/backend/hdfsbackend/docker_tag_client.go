package hdfsbackend

import (
	"fmt"
	"io"

	"code.uber.internal/infra/kraken/lib/backend/pathutil"
)

// DockerTagClient is an HDFS client for uploading / downloading tags to a docker registry.
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
	repo, tag, err := pathutil.ParseRepoTag(name)
	if len(tag) == 0 {
		return "", fmt.Errorf("parse repo tag: %s", err)
	}
	path := fmt.Sprintf(
		"webhdfs/v1/infra/dockerRegistry/docker/registry/v2/repositories/%s/_manifests/tags/%s/current/link",
		repo, tag)
	return path, nil
}

// Download downloads the value of name into dst. Name should be in the format "repo:tag".
func (c *DockerTagClient) Download(name string, dst io.Writer) error {
	path, err := c.path(name)
	if err != nil {
		return fmt.Errorf("tag path: %s", err)
	}

	return c.client.download(path, dst)
}

// Upload uploads src as the value of name. Name should be in the format "repo:tag".
func (c *DockerTagClient) Upload(name string, src io.Reader) error {
	path, err := c.path(name)
	if err != nil {
		return fmt.Errorf("tag path: %s", err)
	}
	return c.client.upload(path, src)
}
