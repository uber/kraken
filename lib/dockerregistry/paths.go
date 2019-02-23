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
package dockerregistry

import (
	"fmt"
	"regexp"

	"github.com/uber/kraken/core"
)

const _repositoryRoot = "/docker/registry/v2/repositories"

// InvalidRegistryPathError indicates path error
type InvalidRegistryPathError struct {
	pathType PathType
	path     string
}

func (e InvalidRegistryPathError) Error() string {
	return fmt.Sprintf("invalid registry path: %s, type: %s", e.path, e.pathType)
}

// PathType describes the type of a path
// i.e. _manfiests, _layers, _uploads, and blobs
type PathType string

func (pt PathType) String() string {
	return string(pt)
}

const (
	_repositories    PathType = "repositories"
	_blobs           PathType = "blobs"
	_manifests       PathType = "_manifests"
	_uploads         PathType = "_uploads"
	_layers          PathType = "_layers"
	_invalidPathType PathType = "invalidPathType"
)

// PathSubType describes the subtype of a path
// i.e. tags, revisions, data
type PathSubType string

const (
	_revisions          PathSubType = "revisions"
	_tags               PathSubType = "tags"
	_data               PathSubType = "data"
	_link               PathSubType = "link"
	_startedat          PathSubType = "startedat"
	_hashstates         PathSubType = "hashstates"
	_invalidPathSubType PathSubType = "invalidPathSubType"
)

// ParsePath returns PathType, PathSubtype, and error given path string
func ParsePath(path string) (PathType, PathSubType, error) {
	if ok, subtype := matchManifestsPath(path); ok {
		return _manifests, subtype, nil
	}
	if ok, subtype := matchUploadsPath(path); ok {
		return _uploads, subtype, nil
	}
	if ok, subtype := matchLayersPath(path); ok {
		return _layers, subtype, nil
	}
	if ok, subtype := matchBlobsPath(path); ok {
		return _blobs, subtype, nil
	}
	return _invalidPathType, _invalidPathSubType, InvalidRegistryPathError{"all", path}
}

// GetRepo returns repo name
func GetRepo(path string) (string, error) {
	re := regexp.MustCompile("^.+/repositories/(.+)/(?:_manifests|_layers|_uploads)")
	matches := re.FindStringSubmatch(path)
	if len(matches) < 2 {
		return "", InvalidRegistryPathError{_repositories, path}
	}
	return matches[1], nil
}

// GetBlobDigest returns blob digest
func GetBlobDigest(path string) (core.Digest, error) {
	re := regexp.MustCompile("^.+/blobs/sha256/[0-9a-z]{2}/([0-9a-z]+)/data$")
	matches := re.FindStringSubmatch(path)
	if len(matches) < 2 {
		return core.Digest{}, InvalidRegistryPathError{_blobs, path}
	}
	d, err := core.NewSHA256DigestFromHex(matches[1])
	if err != nil {
		return core.Digest{}, fmt.Errorf("new digest: %s", err)
	}
	return d, nil
}

// GetLayerDigest returns digest of the layer
func GetLayerDigest(path string) (core.Digest, error) {
	re := regexp.MustCompile("^.+/_layers/sha256/([0-9a-z]+)/(?:link|data)$")
	matches := re.FindStringSubmatch(path)
	if len(matches) < 2 {
		return core.Digest{}, InvalidRegistryPathError{_layers, path}
	}
	d, err := core.NewSHA256DigestFromHex(matches[1])
	if err != nil {
		return core.Digest{}, fmt.Errorf("new digest: %s", err)
	}
	return d, nil
}

// GetManifestDigest returns manifest or tag digest
func GetManifestDigest(path string) (core.Digest, error) {
	re := regexp.MustCompile("^.+/_manifests/(?:revisions|tags/.+/index)/sha256/([0-9a-z]+)/link$")
	matches := re.FindStringSubmatch(path)
	if len(matches) < 2 {
		return core.Digest{}, InvalidRegistryPathError{_manifests, path}
	}
	d, err := core.NewSHA256DigestFromHex(matches[1])
	if err != nil {
		return core.Digest{}, fmt.Errorf("new digest: %s", err)
	}
	return d, nil
}

// GetManifestTag returns tag name
func GetManifestTag(path string) (string, bool, error) {
	re := regexp.MustCompile("^.+/_manifests/tags/([^/]+)/(current|index/sha256/[0-9a-z]+)/link$")
	matches := re.FindStringSubmatch(path)
	if len(matches) < 3 {
		return "", false, InvalidRegistryPathError{_manifests, path}
	}
	if matches[2] == "current" {
		return matches[1], true, nil
	}
	return matches[1], false, nil
}

// GetUploadUUID returns upload UUID
func GetUploadUUID(path string) (string, error) {
	re := regexp.MustCompile("^.+/_uploads/([^/]+)/(?:data$|startedat$|hashstates/[a-zA-Z0-9]+(?:/[0-9]+)?$)")
	matches := re.FindStringSubmatch(path)
	if len(matches) < 2 {
		return "", InvalidRegistryPathError{_uploads, path}
	}
	return matches[1], nil
}

// GetUploadAlgoAndOffset returns the algorithm and offset of the hashstates
func GetUploadAlgoAndOffset(path string) (string, string, error) {
	re := regexp.MustCompile("^.+/_uploads/[^/]+/hashstates/([a-zA-Z0-9]+)/([0-9]+)$")
	matches := re.FindStringSubmatch(path)
	if len(matches) < 3 {
		return "", "", InvalidRegistryPathError{_uploads, path}
	}
	return matches[1], matches[2], nil
}

// matchManifestsPath returns true if it is a valid /_manifests path and returns the path subtype
// Possible subtypes are tags and revisions
func matchManifestsPath(path string) (bool, PathSubType) {
	re := regexp.MustCompile("^.+/_manifests/(tags|revisions)(?:/.+/link)?$")
	matches := re.FindStringSubmatch(path)
	if len(matches) < 2 {
		return false, _invalidPathSubType
	}
	return true, PathSubType(matches[1])
}

// matchBlobsPath returns true if it if a valid /blobs path and returns a subtype
func matchBlobsPath(path string) (bool, PathSubType) {
	re := regexp.MustCompile("^.+/blobs/sha256/[0-9a-z]{2}/[0-9a-z]+/data$")
	ok := re.Match([]byte(path))
	if !ok {
		return false, _invalidPathSubType
	}
	return true, PathSubType(_data)
}

// matchLayersPath returns true if it is a valid /_layers path and returns a subtype
func matchLayersPath(path string) (bool, PathSubType) {
	re := regexp.MustCompile("^.+/_layers/sha256/[0-9a-z]+/(link|data)$")
	matches := re.FindStringSubmatch(path)
	if len(matches) < 2 {
		return false, _invalidPathSubType
	}
	return true, PathSubType(matches[1])
}

// matchUploadsPath returns true if it is a valid /_uploads path and returns the path subtype
// Possible subtypes are data, startedat and hashstates
func matchUploadsPath(path string) (bool, PathSubType) {
	re := regexp.MustCompile("^.+/_uploads/[^/]+/(data$|startedat$|hashstates)")
	matches := re.FindStringSubmatch(path)
	if len(matches) < 2 {
		return false, _invalidPathSubType
	}

	subtype := PathSubType(matches[1])
	switch subtype {
	case _hashstates:
		re := regexp.MustCompile("^.+/_uploads/[^/]+/hashstates/[a-zA-Z0-9]+(?:/[0-9]+)?$")
		if !re.Match([]byte(path)) {
			return false, _invalidPathSubType
		}
	}
	return true, subtype
}
