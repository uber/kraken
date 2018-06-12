package transfer

import (
	"errors"
	"strings"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/store"
)

type testTransferer struct {
	tags map[string]core.Digest
	fs   store.FileStore
}

// NewTestTransferer creates a Transferer which stores blobs in fs and tags in
// memory for testing purposes.
func NewTestTransferer(fs store.FileStore) ImageTransferer {
	return &testTransferer{
		tags: make(map[string]core.Digest),
		fs:   fs,
	}
}

func (t *testTransferer) Download(namespace string, d core.Digest) (store.FileReader, error) {
	return t.fs.GetCacheFileReader(d.Hex())
}

func (t *testTransferer) Upload(namespace string, d core.Digest, blob store.FileReader) error {
	return t.fs.CreateCacheFile(d.Hex(), blob)
}

func (t *testTransferer) GetTag(tag string) (core.Digest, error) {
	d, ok := t.tags[tag]
	if !ok {
		return core.Digest{}, errors.New("tag not found")
	}
	return d, nil
}

func (t *testTransferer) PostTag(tag string, d core.Digest) error {
	t.tags[tag] = d
	return nil
}

func (t *testTransferer) ListRepository(repo string) ([]string, error) {
	var tags []string
	for tag := range t.tags {
		if strings.HasPrefix(tag, repo) {
			tags = append(tags, tag[len(repo)+1:])
		}
	}
	return tags, nil
}
