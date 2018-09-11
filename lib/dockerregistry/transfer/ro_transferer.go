package transfer

import (
	"errors"
	"fmt"
	"os"

	"code.uber.internal/infra/kraken/build-index/tagclient"
	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/lib/torrent/scheduler"
	"github.com/uber-go/tally"
)

var _ ImageTransferer = (*ReadOnlyTransferer)(nil)

// ReadOnlyTransferer gets and posts manifest to tracker, and transfers blobs as torrent.
type ReadOnlyTransferer struct {
	stats tally.Scope
	cads  *store.CADownloadStore
	tags  tagclient.Client
	sched scheduler.Scheduler
}

// NewReadOnlyTransferer creates a new ReadOnlyTransferer.
func NewReadOnlyTransferer(
	stats tally.Scope,
	cads *store.CADownloadStore,
	tags tagclient.Client,
	sched scheduler.Scheduler) *ReadOnlyTransferer {

	stats = stats.Tagged(map[string]string{
		"module": "rotransferer",
	})

	return &ReadOnlyTransferer{stats, cads, tags, sched}
}

// Stat returns blob info from local cache, and triggers download if the blob is
// not available locally.
func (t *ReadOnlyTransferer) Stat(namespace string, d core.Digest) (*core.BlobInfo, error) {
	fi, err := t.cads.Cache().GetFileStat(d.Hex())
	if os.IsNotExist(err) || t.cads.InDownloadError(err) {
		if err := t.sched.Download(namespace, d.Hex()); err != nil {
			return nil, fmt.Errorf("scheduler: %s", err)
		}
		fi, err = t.cads.Cache().GetFileStat(d.Hex())
		if err != nil {
			return nil, fmt.Errorf("stat cache: %s", err)
		}
	} else if err != nil {
		return nil, fmt.Errorf("stat cache: %s", err)
	}
	return core.NewBlobInfo(fi.Size()), nil
}

// Download downloads blobs as torrent.
func (t *ReadOnlyTransferer) Download(namespace string, d core.Digest) (store.FileReader, error) {
	f, err := t.cads.Cache().GetFileReader(d.Hex())
	if os.IsNotExist(err) || t.cads.InDownloadError(err) {
		if err := t.sched.Download(namespace, d.Hex()); err != nil {
			return nil, fmt.Errorf("scheduler: %s", err)
		}
		f, err = t.cads.Cache().GetFileReader(d.Hex())
		if err != nil {
			return nil, fmt.Errorf("cache: %s", err)
		}
	} else if err != nil {
		return nil, fmt.Errorf("cache: %s", err)
	}
	return f, nil
}

// Upload uploads blobs to a torrent network.
func (t *ReadOnlyTransferer) Upload(namespace string, d core.Digest, blob store.FileReader) error {
	return errors.New("unsupported operation")
}

// GetTag gets manifest digest for tag.
func (t *ReadOnlyTransferer) GetTag(tag string) (core.Digest, error) {
	d, err := t.tags.Get(tag)
	if err != nil {
		if err == tagclient.ErrTagNotFound {
			t.stats.Counter("tag_not_found").Inc(1)
			return core.Digest{}, ErrTagNotFound
		}
		t.stats.Counter("get_tag_error").Inc(1)
		return core.Digest{}, fmt.Errorf("client get tag: %s", err)
	}
	return d, nil
}

// PostTag is not supported.
func (t *ReadOnlyTransferer) PostTag(tag string, d core.Digest) error {
	return errors.New("not supported")
}

// ListTags is not supported.
func (t *ReadOnlyTransferer) ListTags(prefix string) ([]string, error) {
	return nil, errors.New("not supported")
}
