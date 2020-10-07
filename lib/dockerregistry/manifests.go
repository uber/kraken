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
	"strings"
	"time"

	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/dockerregistry/transfer"
	"github.com/uber/kraken/utils/log"
)

const (
	pullTimer            = "dockertag.time.pull"
	createSuccessCounter = "dockertag.success.create"
	createFailureCounter = "dockertag.failure.create"
	getSuccessCounter    = "dockertag.success.get"
	getFailureCounter    = "dockertag.failure.get"
)

type manifests struct {
	transferer transfer.ImageTransferer
}

func newManifests(transferer transfer.ImageTransferer) *manifests {
	return &manifests{transferer}
}

// getDigest downloads and returns manifest digest.
// This is the only place storage driver would download a manifest blob via
// torrent scheduler or origin because it has namespace information.
// The caller of storage driver would first call this function to resolve
// the manifest link (and downloads manifest blob),
// then call Stat or Reader which would assume the blob is on disk already.
func (t *manifests) getDigest(path string, subtype PathSubType) ([]byte, error) {
	repo, err := GetRepo(path)
	if err != nil {
		return nil, fmt.Errorf("get repo: %s", err)
	}

	var digest core.Digest
	switch subtype {
	case _tags:
		tag, _, err := GetManifestTag(path)
		if err != nil {
			return nil, fmt.Errorf("get manifest tag: %s", err)
		}
		digest, err = t.transferer.GetTag(fmt.Sprintf("%s:%s", repo, tag))
		if err != nil {
			return nil, fmt.Errorf("transferer get tag: %w", err)
		}
	case _revisions:
		var err error
		digest, err = GetManifestDigest(path)
		if err != nil {
			return nil, fmt.Errorf("get manifest digest: %s", err)
		}
	default:
		return nil, &InvalidRequestError{path}
	}

	blob, err := t.transferer.Download(repo, digest)
	if err != nil {
		return nil, fmt.Errorf("transferer download: %w", err)
	}
	defer blob.Close()

	return []byte(digest.String()), nil
}

func (t *manifests) putContent(path string, subtype PathSubType) error {
	switch subtype {
	case _tags:
		repo, err := GetRepo(path)
		if err != nil {
			return fmt.Errorf("get repo: %s", err)
		}
		tag, isCurrent, err := GetManifestTag(path)
		if err != nil {
			return fmt.Errorf("get manifest tag: %s", err)
		}
		if isCurrent {
			return nil
		}
		digest, err := GetManifestDigest(path)
		if err != nil {
			return fmt.Errorf("get manifest digest: %s", err)
		}
		if err := t.transferer.PutTag(fmt.Sprintf("%s:%s", repo, tag), digest); err != nil {
			return fmt.Errorf("post tag: %w", err)
		}
		return nil
	}
	// Intentional no-op.
	return nil
}

func (t *manifests) stat(path string) (storagedriver.FileInfo, error) {
	repo, err := GetRepo(path)
	if err != nil {
		return nil, fmt.Errorf("get repo: %s", err)
	}
	tag, _, err := GetManifestTag(path)
	if err != nil {
		return nil, fmt.Errorf("get manifest tag: %s", err)
	}
	if _, err := t.transferer.GetTag(fmt.Sprintf("%s:%s", repo, tag)); err != nil {
		return nil, fmt.Errorf("get tag: %w", err)
	}
	return storagedriver.FileInfoInternal{
		FileInfoFields: storagedriver.FileInfoFields{
			Path:    path,
			Size:    64,
			ModTime: time.Now(),
			IsDir:   false,
		},
	}, nil
}

func (t *manifests) list(path string) ([]string, error) {
	prefix := path[len(_repositoryRoot):]
	tags, err := t.transferer.ListTags(prefix)
	if err != nil {
		return nil, err
	}
	for i, tag := range tags {
		// Strip repo prefix.
		parts := strings.Split(tags[i], ":")
		if len(parts) != 2 {
			log.With("tag", tag).Warn("Repo list skipping tag, expected repo:tag format")
			continue
		}
		tags[i] = parts[1]
	}
	return tags, nil
}
