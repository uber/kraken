package storage

import (
	"fmt"
	"strings"

	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/infra/kraken/config/tracker"
	"code.uber.internal/infra/kraken/torlib"

	_ "github.com/go-sql-driver/mysql" // need this side effect import for mysql
)

// Storage defines an interface for CRUD operations on peers and torrents
type Storage interface {
	//name of a storage engine
	Name() string
	//GetPeers returns a list of peers
	GetPeers(infohash string) ([]*torlib.PeerInfo, error)
	//UpdatePeer updates peer
	UpdatePeer(peer *torlib.PeerInfo) error

	//GetTorrent returns torrent's metainfo as raw string
	GetTorrent(name string) (string, error)
	//CreateTorrent creates torrent in tracker's storage given metainfo
	CreateTorrent(meta *torlib.MetaInfo) error

	//GetManifest returns stored manifest as raw string given tag
	GetManifest(tag string) (string, error)
	//CreateManifest creates manfist given tag and manifest
	CreateManifest(tag, manifestRaw string) error
	//DeleteManifest deletes manifest from tracker given tag
	DeleteManifest(tag string) error
}

// DataStoreFactory is storage factory function type
type DataStoreFactory func(appCfg config.AppConfig) (Storage, error)

var datastoreFactories = make(map[string]DataStoreFactory)

// Register registers a name and a factory in a system
func Register(name string, factory DataStoreFactory) {
	if factory == nil {
		log.Panicf("Datastore factory %s does not exist.", name)
	}
	_, registered := datastoreFactories[name]
	if registered {
		log.Errorf("Ignored registered datastore factory %s", name)
	}
	datastoreFactories[name] = factory
}

// Init registers all storages in a system
func Init() {
	Register("mysql", NewMySQLStorage)
}

// CreateStorage initilizes all available storages in a system
func CreateStorage(appCfg config.AppConfig) (Storage, error) {

	engineFactory, ok := datastoreFactories[appCfg.DBConfig.EngineName]
	if !ok {
		availableDatastores := make([]string, len(datastoreFactories))
		for k := range datastoreFactories {
			availableDatastores = append(availableDatastores, k)
		}
		return nil, fmt.Errorf(
			"Invalid Datastore name. Must be one of: %s",
			strings.Join(availableDatastores, ", "))
	}

	// Run the factory with the configuration.
	return engineFactory(appCfg)
}
