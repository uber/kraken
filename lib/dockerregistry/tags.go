package dockerregistry

import (
	"fmt"
	"time"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/dockerregistry/transfer"
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

// GetContent returns manifest digest for path.
func (t *Tags) GetContent(path string, subtype PathSubType) (data []byte, err error) {
	switch subtype {
	case _tags:
		repo, err := GetRepo(path)
		if err != nil {
			return nil, fmt.Errorf("get repo: %s", err)
		}
		tag, _, err := GetManifestTag(path)
		if err != nil {
			return nil, fmt.Errorf("get manifest tag: %s", err)
		}
		digest, err := t.transferer.GetTag(repo, tag)
		if err != nil {
			return nil, fmt.Errorf("transferer get tag: %s", err)
		}
		return []byte(digest.String()), nil
	case _revisions:
		digest, err := GetManifestDigest(path)
		if err != nil {
			return nil, fmt.Errorf("get manifest digest: %s", err)
		}
		return []byte(fmt.Sprintf("sha256:%s", digest)), nil
	}
	return nil, &InvalidRequestError{path}
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
		if err := t.transferer.PostTag(repo, tag, core.NewSHA256DigestFromHex(digest)); err != nil {
			return fmt.Errorf("post tag: %s", err)
		}
		return nil
	}
	// No-op.
	return nil
}
