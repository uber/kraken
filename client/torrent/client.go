package torrent

import (
	"code.uber.internal/infra/kraken/client/torrent/scheduler"
	"code.uber.internal/infra/kraken/torlib"
)

// Client TODO
type Client struct {
	config Config
	peerID scheduler.PeerID
}

// NewClient TODO
func NewClient(config *Config) (*Client, error) {
	// TODO
	return nil, nil
}

// Start TODO
func (c *Client) Start() {
	// TODO
}

// Stop TODO
func (c *Client) Stop() {
	// TODO
}

// AddTorrent TODO
func (c *Client) AddTorrent(spec *Spec) error {
	// TODO
	return nil
}

// DropTorrent TODO
func (c *Client) DropTorrent(infoHash torlib.InfoHash) error {
	// TODO
	return nil
}
