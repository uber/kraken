package namepath

import (
	"errors"
	"fmt"
	"path"
	"strings"
)

// New creates a Pather scoped to root.
func New(root, id string) (Pather, error) {
	switch id {
	case "docker_tag":
		return DockerTag{root}, nil
	case "sharded_docker_blob":
		return ShardedDockerBlob{root}, nil
	case "identity":
		return Identity{root}, nil
	default:
		return nil, fmt.Errorf("unknown pather identifier: %s", id)
	}
}

// Pather defines an interface for converting names into paths.
type Pather interface {
	Path(name string) (string, error)
}

// DockerTag generates paths for Docker tags.
type DockerTag struct {
	root string
}

// Path interprets name as a "repo:tag" and generates a registry path for it.
func (t DockerTag) Path(name string) (string, error) {
	tokens := strings.Split(name, ":")
	if len(tokens) != 2 {
		return "", errors.New("name must be in format 'repo:tag'")
	}
	repo := tokens[0]
	if len(repo) == 0 {
		return "", errors.New("repo must be non-empty")
	}
	tag := tokens[1]
	if len(tag) == 0 {
		return "", errors.New("tag must be non-empty")
	}
	return path.Join(t.root, fmt.Sprintf(
		"docker/registry/v2/repositories/%s/_manifests/tags/%s/current/link", repo, tag)), nil
}

// ShardedDockerBlob generates sharded paths for Docker blobs.
type ShardedDockerBlob struct {
	root string
}

// Path interprets name as a SHA256 digest and returns a registry path which is
// sharded by the first two bytes.
func (b ShardedDockerBlob) Path(name string) (string, error) {
	if len(name) <= 2 {
		return "", errors.New("name is too short, must be > 2 characters")
	}
	return path.Join(b.root, fmt.Sprintf(
		"docker/registry/v2/blobs/sha256/%s/%s/data", name[:2], name)), nil
}

// Identity is the identity Pather.
type Identity struct {
	root string
}

// Path always returns root/name.
func (i Identity) Path(name string) (string, error) {
	return path.Join(i.root, name), nil
}
