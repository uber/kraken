package transfer

import (
	"errors"
	"fmt"
	"os"

	"code.uber.internal/infra/kraken/build-index/tagclient"
	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/lib/torrent/scheduler"
)

var _ ImageTransferer = (*AgentTransferer)(nil)

// AgentTransferer gets and posts manifest to tracker, and transfers blobs as torrent.
type AgentTransferer struct {
	cads  *store.CADownloadStore
	tags  tagclient.Client
	sched scheduler.Scheduler
}

// NewAgentTransferer creates a new AgentTransferer.
func NewAgentTransferer(
	cads *store.CADownloadStore,
	tags tagclient.Client,
	sched scheduler.Scheduler) *AgentTransferer {

	return &AgentTransferer{cads, tags, sched}
}

// Download downloads blobs as torrent.
func (t *AgentTransferer) Download(namespace string, d core.Digest) (store.FileReader, error) {
	f, err := t.cads.Cache().GetFileReader(d.Hex())
	if err != nil {
		if os.IsNotExist(err) {
			if err := t.sched.Download(namespace, d.Hex()); err != nil {
				return nil, fmt.Errorf("scheduler: %s", err)
			}
			f, err = t.cads.Cache().GetFileReader(d.Hex())
			if err != nil {
				return nil, fmt.Errorf("cache: %s", err)
			}
		} else {
			return nil, fmt.Errorf("cache: %s", err)
		}
	}
	return f, nil
}

// Upload uploads blobs to a torrent network.
func (t *AgentTransferer) Upload(namespace string, d core.Digest, blob store.FileReader) error {
	return errors.New("unsupported operation")
}

// GetTag gets manifest digest for tag.
func (t *AgentTransferer) GetTag(tag string) (core.Digest, error) {
	return t.tags.Get(tag)
}

// PostTag is not supported.
func (t *AgentTransferer) PostTag(tag string, d core.Digest) error {
	return errors.New("not supported")
}

// ListRepository is not supported.
func (t *AgentTransferer) ListRepository(repo string) ([]string, error) {
	return nil, errors.New("not supported")
}
