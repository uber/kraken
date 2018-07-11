package transfer

import (
	"errors"
	"fmt"
	"path"
	"strings"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/backend/namepath"
	"code.uber.internal/infra/kraken/lib/store"
)

type testTransferer struct {
	tagPather namepath.Pather
	tags      map[string]core.Digest
	cas       *store.CAStore
}

// NewTestTransferer creates a Transferer which stores blobs in cas and tags in
// memory for testing purposes.
func NewTestTransferer(cas *store.CAStore) ImageTransferer {
	tagPather, err := namepath.New("", namepath.DockerTag)
	if err != nil {
		panic(err)
	}
	return &testTransferer{
		tagPather: tagPather,
		tags:      make(map[string]core.Digest),
		cas:       cas,
	}
}

// Stat returns blob info from local cache.
func (t *testTransferer) Stat(namespace string, d core.Digest) (*core.BlobInfo, error) {
	fi, err := t.cas.GetCacheFileStat(d.Hex())
	if err != nil {
		return nil, fmt.Errorf("stat cache file: %s", err)
	}
	return core.NewBlobInfo(fi.Size()), nil
}

func (t *testTransferer) Download(namespace string, d core.Digest) (store.FileReader, error) {
	return t.cas.GetCacheFileReader(d.Hex())
}

func (t *testTransferer) Upload(namespace string, d core.Digest, blob store.FileReader) error {
	return t.cas.CreateCacheFile(d.Hex(), blob)
}

func (t *testTransferer) GetTag(tag string) (core.Digest, error) {
	p, err := t.tagPather.BlobPath(tag)
	if err != nil {
		return core.Digest{}, err
	}
	d, ok := t.tags[p]
	if !ok {
		return core.Digest{}, errors.New("tag not found")
	}
	return d, nil
}

func (t *testTransferer) PostTag(tag string, d core.Digest) error {
	p, err := t.tagPather.BlobPath(tag)
	if err != nil {
		return err
	}
	t.tags[p] = d
	return nil
}

func (t *testTransferer) ListTags(prefix string) ([]string, error) {
	prefix = path.Join(t.tagPather.BasePath(), prefix)
	var tags []string
	for path := range t.tags {
		if strings.HasPrefix(path, prefix) {
			tag, err := t.tagPather.NameFromBlobPath(path)
			if err != nil {
				return nil, fmt.Errorf("invalid tag path %s: %s", path, err)
			}
			tags = append(tags, tag)
		}
	}
	return tags, nil
}
