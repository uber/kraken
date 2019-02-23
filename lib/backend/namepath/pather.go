// Copyright (c) 2016-2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package namepath

import (
	"errors"
	"fmt"
	"path"
	"regexp"
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
	case "":
		return nil, fmt.Errorf("invalid pather identifier: empty")
	default:
		return nil, fmt.Errorf("unknown pather identifier: %s", id)
	}
}

// Pather defines an interface for converting names into paths.
type Pather interface {
	// BasePath returns the base path of where blobs are stored.
	BasePath() string

	// BlobPath converts name into a blob path.
	BlobPath(name string) (string, error)

	// NameFromBlobPath converts blob path bp back into the original blob name.
	NameFromBlobPath(bp string) (string, error)
}

// DockerTagPather generates paths for Docker tags.
type DockerTagPather struct {
	root string
}

// BasePath returns the docker registry repositories prefix.
func (p DockerTagPather) BasePath() string {
	return path.Join(p.root, "docker/registry/v2/repositories")
}

// BlobPath interprets name as a "repo:tag" and generates a registry path for it.
func (p DockerTagPather) BlobPath(name string) (string, error) {
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
	return path.Join(p.BasePath(), repo, "_manifests/tags", tag, "current/link"), nil
}

// NameFromBlobPath converts a tag path back into repo:tag format.
func (p DockerTagPather) NameFromBlobPath(bp string) (string, error) {
	re := regexp.MustCompile(p.BasePath() + "/(.+)/_manifests/tags/(.+)/current/link")
	matches := re.FindStringSubmatch(bp)
	if len(matches) != 3 {
		return "", errors.New("invalid docker tag path format")
	}
	repo := matches[1]
	tag := matches[2]
	return fmt.Sprintf("%s:%s", repo, tag), nil
}

// ShardedDockerBlobPather generates sharded paths for Docker blobs.
type ShardedDockerBlobPather struct {
	root string
}

// BasePath returns the docker registry blobs prefix.
func (p ShardedDockerBlobPather) BasePath() string {
	return path.Join(p.root, "docker/registry/v2/blobs")
}

// BlobPath interprets name as a SHA256 digest and returns a registry path
// which is sharded by the first two bytes.
func (p ShardedDockerBlobPather) BlobPath(name string) (string, error) {
	if len(name) <= 2 {
		return "", errors.New("name is too short, must be > 2 characters")
	}
	return path.Join(p.BasePath(), "sha256", name[:2], name, "data"), nil
}

// NameFromBlobPath converts a sharded blob path back into raw hex format.
func (p ShardedDockerBlobPather) NameFromBlobPath(bp string) (string, error) {
	re := regexp.MustCompile(p.BasePath() + "/sha256/../(.+)/data")
	matches := re.FindStringSubmatch(bp)
	if len(matches) != 2 {
		return "", errors.New("invalid sharded docker blob path format")
	}
	return matches[1], nil
}

// IdentityPather is the identity Pather.
type IdentityPather struct {
	root string
}

// BasePath returns the root.
func (p IdentityPather) BasePath() string {
	return p.root
}

// BlobPath always returns root/name.
func (p IdentityPather) BlobPath(name string) (string, error) {
	return path.Join(p.root, name), nil
}

// NameFromBlobPath strips the root from bp.
func (p IdentityPather) NameFromBlobPath(bp string) (string, error) {
	if !strings.HasPrefix(bp, p.root) {
		return "", errors.New("invalid identity path format")
	}
	return bp[len(p.root)+1:], nil
}
