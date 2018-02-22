package transfer

import (
	"bytes"
	"errors"
	"fmt"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/backend"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/lib/torrent"
)

var _ ImageTransferer = (*AgentTransferer)(nil)

// AgentTransferer gets and posts manifest to tracker, and transfers blobs as torrent.
type AgentTransferer struct {
	fs            store.FileStore
	tagClient     backend.Client
	blobNamespace string
	torrentClient torrent.Client
}

// NewAgentTransferer creates a new AgentTransferer.
func NewAgentTransferer(
	fs store.FileStore,
	tagClient backend.Client,
	blobNamespace string,
	torrentClient torrent.Client) *AgentTransferer {

	return &AgentTransferer{fs, tagClient, blobNamespace, torrentClient}
}

// Download downloads blobs as torrent.
func (t *AgentTransferer) Download(name string) (store.FileReader, error) {
	// TODO(codyg): Plumb docker namespace here.
	if err := t.torrentClient.Download(t.blobNamespace, name); err != nil {
		return nil, fmt.Errorf("torrent: %s", err)
	}
	f, err := t.fs.GetCacheFileReader(name)
	if err != nil {
		return nil, fmt.Errorf("file store: %s", err)
	}
	return f, nil
}

// Upload uploads blobs to a torrent network.
func (t *AgentTransferer) Upload(name string, blob store.FileReader, size int64) error {
	return errors.New("unsupported operation")
}

// GetTag gets manifest digest, given repo and tag.
func (t *AgentTransferer) GetTag(repo, tag string) (core.Digest, error) {
	var b bytes.Buffer
	if err := t.tagClient.Download(fmt.Sprintf("%s:%s", repo, tag), &b); err != nil {
		return core.Digest{}, fmt.Errorf("download tag through client: %s", err)
	}

	d, err := core.NewDigestFromString(b.String())
	if err != nil {
		return core.Digest{}, fmt.Errorf("construct manifest digest: %s", err)
	}
	return d, nil
}

// PostTag posts tag:manifest_digest mapping to addr given repo and tag.
func (t *AgentTransferer) PostTag(repo, tag string, manifestDigest core.Digest) error {
	return errors.New("not supported")
}
