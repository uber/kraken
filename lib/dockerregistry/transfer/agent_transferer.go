package transfer

import (
	"errors"

	"code.uber.internal/infra/kraken/lib/torrent"
)

var _ ImageTransferer = (*AgentTransferer)(nil)

// AgentTransferer gets and posts manifest to tracker, and transfers blobs as torrent
type AgentTransferer struct {
	client torrent.Client
}

// Download downloads blobs as torrent
func (tt *AgentTransferer) Download(digest string) error {
	return tt.client.DownloadTorrent(digest)
}

// Upload uploads blobs to a torrent network
func (tt *AgentTransferer) Upload(digest string) error {
	return errors.New("unsupported in TorrentImageTransferer")
}

// GetManifest gets manifest from the tracker
// TODO (@evelynl): maybe change torrent client to use ManifestClient
func (tt *AgentTransferer) GetManifest(repo, tag string) (digest string, err error) {
	return tt.client.GetManifest(repo, tag)
}

// PostManifest posts manifest to tracker
// TODO (@evelynl): maybe change torrent client to use ManifestClient
func (tt *AgentTransferer) PostManifest(repo, tag, manifest string) error {
	return tt.client.PostManifest(repo, tag, manifest)
}
