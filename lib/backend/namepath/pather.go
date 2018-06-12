package namepath

import (
	"errors"
	"fmt"
	"path"
	"strings"
)

// Pather id strings.
const (
	DockerTag         = "docker_tag"
	ShardedDockerBlob = "sharded_docker_blob"
	Identity          = "identity"
)

// New creates a Pather scoped to root.
func New(root, id string) (Pather, error) {
	switch id {
	case DockerTag:
		return DockerTagPather{root}, nil
	case ShardedDockerBlob:
		return ShardedDockerBlobPather{root}, nil
	case Identity:
		return IdentityPather{root}, nil
	default:
		return nil, fmt.Errorf("unknown pather identifier: %s", id)
	}
}

// Pather defines an interface for converting names into paths.
type Pather interface {
	// BlobPath converts name into a blob path.
	BlobPath(name string) (string, error)

	// DirPath converts dir into a path where blobs are stored.
	DirPath(dir string) (string, error)
}

// DockerTagPather generates paths for Docker tags.
type DockerTagPather struct {
	root string
}

// BlobPath interprets name as a "repo:tag" and generates a registry path for it.
func (t DockerTagPather) BlobPath(name string) (string, error) {
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
	repoDir, err := t.DirPath(repo)
	if err != nil {
		return "", fmt.Errorf("dir path: %s", err)
	}
	return path.Join(repoDir, tag, "current/link"), nil
}

// DirPath interprets dir as a repo, and generates the path where all tags for
// repo are stored.
func (t DockerTagPather) DirPath(dir string) (string, error) {
	repoDir := fmt.Sprintf("docker/registry/v2/repositories/%s/_manifests/tags", dir)
	return path.Join(t.root, repoDir), nil
}

// ShardedDockerBlobPather generates sharded paths for Docker blobs.
type ShardedDockerBlobPather struct {
	root string
}

// BlobPath interprets name as a SHA256 digest and returns a registry path
// which is sharded by the first two bytes.
func (b ShardedDockerBlobPather) BlobPath(name string) (string, error) {
	if len(name) <= 2 {
		return "", errors.New("name is too short, must be > 2 characters")
	}
	return path.Join(b.root, fmt.Sprintf(
		"docker/registry/v2/blobs/sha256/%s/%s/data", name[:2], name)), nil
}

// DirPath is not supported.
func (b ShardedDockerBlobPather) DirPath(dir string) (string, error) {
	return "", errors.New("not supported")
}

// IdentityPather is the identity Pather.
type IdentityPather struct {
	root string
}

// BlobPath always returns root/name.
func (i IdentityPather) BlobPath(name string) (string, error) {
	return path.Join(i.root, name), nil
}

// DirPath always returns root/dir.
func (i IdentityPather) DirPath(dir string) (string, error) {
	return path.Join(i.root, dir), nil
}
