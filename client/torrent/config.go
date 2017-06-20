package torrent

import (
	"code.uber.internal/go-common.git/x/log"

	"code.uber.internal/infra/kraken/client/torrent/storage"
)

// Config provides a configuraiton for a torrent client
type Config struct {

	// Store torrent file data in this directory unless TorrentDataOpener is
	// specified.
	DataDir string `long:"data-dir" description:"directory to store downloaded torrent data"`

	// The address to listen for new uTP and TCP bittorrent protocol
	// connections. DHT shares a UDP socket with uTP unless configured
	// otherwise.
	ListenAddr string `long:"listen-addr" value-name:"HOST:PORT"`

	// User-provided Client peer ID. If not present, one is generated automatically.
	PeerID string

	// Called to instantiate storage for each added torrent. Builtin backends
	// are in the storage package. If not set, the "file" implementation is
	// used.
	DefaultStorage storage.TorrentStorage

	// Perform logging and any other behaviour that will help debug.
	Debug bool `help:"enable debug logging"`

	//Track connection by priority, drop lower priority connections
	RespectPeerPriority bool `long:"peer-priority"`

	//logger config
	Logging log.Configuration
}
