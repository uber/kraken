package proxybackend

import (
	"errors"
	"fmt"
	"io"

	"code.uber.internal/infra/kraken/core"

	"code.uber.internal/infra/kraken/lib/backend/backenderrors"
	"code.uber.internal/infra/kraken/lib/backend/nameparse"
	"code.uber.internal/infra/kraken/utils/httputil"
)

const _dockerContentDigestHeader = "Docker-Content-Digest"

// DockerTagClient is an client for downloading/uploading tags from a remote docker registry.
type DockerTagClient struct {
	config Config
}

// NewDockerTagClient creates a new DockerTagClient.
func NewDockerTagClient(config Config) (*DockerTagClient, error) {
	config, err := config.applyDefaults()
	if err != nil {
		return nil, fmt.Errorf("invalid config: %s", err)
	}
	return &DockerTagClient{
		config: config,
	}, nil
}

// getTagURL returns tag lookup URL to a remote registry.
func (c *DockerTagClient) getTagURL(name string) (string, error) {
	repo, tag, err := nameparse.ParseRepoTag(name)
	if err != nil {
		return "", fmt.Errorf("parse repo tag: %s", err)
	}
	tagURL := fmt.Sprintf("http://%s/v2/%s/manifests/%s", c.config.Addr, repo, tag)
	return tagURL, nil
}

// Download downloads the value of name into dst. Name should be in the format "repo:tag".
func (c *DockerTagClient) Download(name string, dst io.Writer) error {
	tagURL, err := c.getTagURL(name)
	if err != nil {
		return fmt.Errorf("get tag url: %s", err)
	}

	resp, err := httputil.Get(
		tagURL,
		httputil.SendHeaders(map[string]string{
			"Accept": "application/vnd.docker.distribution.manifest.v2+json",
		}))
	if err != nil {
		if httputil.IsNotFound(err) {
			return backenderrors.ErrBlobNotFound
		}
		return err
	}
	d := resp.Header.Get(_dockerContentDigestHeader)
	if _, err := core.NewDigestFromString(d); err != nil {
		return fmt.Errorf("parse manifest digest: %s", err)
	}

	if _, err := dst.Write([]byte(d)); err != nil {
		return fmt.Errorf("copy response: %s", err)
	}
	return nil
}

// Upload uploads src as the value of name. Name should be in the format "repo:tag".
func (c *DockerTagClient) Upload(name string, src io.Reader) error {
	return errors.New("not supported")
}
