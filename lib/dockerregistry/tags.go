package dockerregistry

import (
	"fmt"
	"time"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/dockerregistry/transfer"
	"code.uber.internal/infra/kraken/utils/log"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
)

const (
	pullTimer            = "dockertag.time.pull"
	createSuccessCounter = "dockertag.success.create"
	createFailureCounter = "dockertag.failure.create"
	getSuccessCounter    = "dockertag.success.get"
	getFailureCounter    = "dockertag.failure.get"
)

// Tags handles tag lookups
// a tag is a file with tag_path = <tag_dir>/<repo>/<tag>
// content of the file is sha1(<tag_path>), which is the name of a (torrent) file in cache_dir
// torrent file <cache_dir>/<sha1(<tag_path>)> is a link between tag and manifest
// the content of it is the manifest digest of the tag
type Tags struct {
	transferer transfer.ImageTransferer
}

// Tag stores information about one tag.
type Tag struct {
	repo    string
	tagName string
	modTime time.Time
}

// TagSlice is used for sorting tags
type TagSlice []Tag

func (s TagSlice) Less(i, j int) bool { return s[i].modTime.Before(s[j].modTime) }
func (s TagSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s TagSlice) Len() int           { return len(s) }

// NewTags returns a new Tags.
func NewTags(transferer transfer.ImageTransferer) *Tags {
	return &Tags{transferer}
}

// GetDigest downloads and returns manifest digest.
// This is the only place storage driver would download a manifest blob via
// torrent scheduler or origin because it has namespace information.
// The caller of storage driver would first call this function to resolve
// the manifest link (and downloads manifest blob),
// then call Stat or Reader which would assume the blob is on disk already.
func (t *Tags) GetDigest(path string, subtype PathSubType) (data []byte, err error) {
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
			return nil, fmt.Errorf("transferer get tag: %s", err)
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
		log.Errorf("Failed to download %s: %s", digest, err)
		return nil, storagedriver.PathNotFoundError{
			DriverName: "kraken",
			Path:       digest.String(),
		}
	}
	defer blob.Close()

	return []byte(digest.String()), nil
}

// PutContent creates tags.
func (t *Tags) PutContent(path string, subtype PathSubType) error {
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
		if err := t.transferer.PostTag(fmt.Sprintf("%s:%s", repo, tag), digest); err != nil {
			return fmt.Errorf("post tag: %s", err)
		}
		return nil
	}
	// No-op.
	return nil
}
