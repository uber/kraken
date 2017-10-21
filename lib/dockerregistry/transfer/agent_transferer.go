package transfer

import (
	"errors"
	"io"

	"code.uber.internal/infra/kraken/lib/torrent"
)

var _ ImageTransferer = (*AgentTransferer)(nil)

// AgentTransferer gets and posts manifest to tracker, and transfers blobs as torrent
type AgentTransferer struct {
	client torrent.Client
}

// NewAgentTransferer creates a new agent transferer given an agent torrent client
func NewAgentTransferer(client torrent.Client) *AgentTransferer {
	return &AgentTransferer{client}
}

// Download downloads blobs as torrent
func (tt *AgentTransferer) Download(digest string) (io.ReadCloser, error) {
	return tt.client.DownloadTorrent(digest)
}

// Upload uploads blobs to a torrent network
func (tt *AgentTransferer) Upload(digest string, reader io.Reader, size int64) error {
	return errors.New("unsupported in TorrentImageTransferer")
}

// GetManifest gets manifest from the tracker
// TODO (@evelynl): maybe change torrent client to use ManifestClient
func (tt *AgentTransferer) GetManifest(repo, tag string) (io.ReadCloser, error) {
	return tt.client.GetManifest(repo, tag)
}

// PostManifest posts manifest to tracker
// TODO (@evelynl): maybe change torrent client to use ManifestClient
func (tt *AgentTransferer) PostManifest(repo, tag, manifest string, reader io.Reader) error {
	return tt.client.PostManifest(repo, tag, manifest, reader)
}
