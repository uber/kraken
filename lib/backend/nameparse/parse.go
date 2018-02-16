package nameparse

import (
	"errors"
	"fmt"
	"strings"
)

// ParseRepoTag parses give name to repo and tag. Name should be in the format "repo:tag".
func ParseRepoTag(name string) (string, string, error) {
	tokens := strings.Split(name, ":")
	if len(tokens) != 2 {
		return "", "", errors.New("name must be in format 'repo:tag'")
	}
	repo := tokens[0]
	if len(repo) == 0 {
		return "", "", errors.New("repo must be non-empty")
	}
	tag := tokens[1]
	if len(tag) == 0 {
		return "", "", errors.New("tag must be non-empty")
	}
	return repo, tag, nil
}

// RepoTagPath parses input name into Repo:Tag pair and embed it into
// a docker path for repo:tag
func RepoTagPath(dirprefix string, name string) (string, error) {
	repo, tag, err := ParseRepoTag(name)
	if err != nil {
		return "", err
	}
	path := fmt.Sprintf(
		dirprefix+"docker/registry/v2/repositories/%s/_manifests/tags/%s/current/link",
		repo, tag)

	return path, nil
}

// ShardDigestPath parses input name into a Repo:Tag pair and embed it into
// a docker path for blob locations
func ShardDigestPath(dirprefix string, name string) (string, error) {
	if len(name) <= 2 {
		return "", errors.New("name is too short, must be >= 2 characters")
	}
	path := fmt.Sprintf(
		dirprefix+"docker/registry/v2/blobs/sha256/%s/%s/data",
		name[:2], name)
	return path, nil
}
