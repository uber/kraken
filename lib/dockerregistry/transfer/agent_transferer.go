package transfer

import (
	"errors"
	"fmt"
	"io"

	"code.uber.internal/infra/kraken/lib/dockerregistry/transfer/manifestclient"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/lib/torrent"
)

var _ ImageTransferer = (*AgentTransferer)(nil)

// AgentTransferer gets and posts manifest to tracker, and transfers blobs as torrent
type AgentTransferer struct {
	fs             store.FileStore
	torrentClient  torrent.Client
	manifestClient manifestclient.Client
}

// NewAgentTransferer creates a new AgentTransferer.
func NewAgentTransferer(
	fs store.FileStore,
	torrentClient torrent.Client,
	manifestClient manifestclient.Client) *AgentTransferer {

	return &AgentTransferer{fs, torrentClient, manifestClient}
}

// Download downloads blobs as torrent
func (t *AgentTransferer) Download(name string) (store.FileReader, error) {
	// TODO(codyg): Plumb docker namespace here.
	if err := t.torrentClient.Download("TODO", name); err != nil {
		return nil, fmt.Errorf("torrent: %s", err)
	}
	f, err := t.fs.GetCacheFileReader(name)
	if err != nil {
		return nil, fmt.Errorf("file store: %s", err)
	}
	return f, nil
}

// Upload uploads blobs to a torrent network
func (t *AgentTransferer) Upload(name string, blob store.FileReader, size int64) error {
	return errors.New("unsupported operation")
}

// GetManifest gets manifest from the tracker
func (t *AgentTransferer) GetManifest(repo, tag string) (io.ReadCloser, error) {
	return t.manifestClient.GetManifest(repo, tag)
}

// PostManifest posts manifest to tracker
func (t *AgentTransferer) PostManifest(repo, tag string, manifest io.Reader) error {
	return t.manifestClient.PostManifest(repo, tag, manifest)
}
