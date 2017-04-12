package storage

import (
	"database/sql"
	"fmt"
	"strings"

	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/infra/kraken/config/tracker"
	_ "github.com/go-sql-driver/mysql" // need this side effect import for mysql
	"github.com/pressly/goose"
)

// TPrivate denotes a private torrent flag
var TPrivate uint = 0x00000001

// POrigin denotes an origin peer
var POrigin uint = 0x00000001

// TorrentInfo defines metadata for a torrent
type TorrentInfo struct {
	TorrentName string
	InfoHash    string
	Author      string
	NumPieces   int
	PieceLength int
	Flags       uint
}

// PeerInfo defines metadata for a peer
type PeerInfo struct {
	InfoHash        string `bencode:"-"`
	PeerID          string `bencode:"peer id"`
	IP              string `bencode:"ip"`
	Port            int64  `bencode:"port"`
	BytesUploaded   int64  `bencode:"-"`
	BytesDownloaded int64  `bencode:"-"`
	BytesLeft       int64  `bencode:"-"`
	Event           string `bencode:"-"`
	Flags           uint   `bencode:"-"`
}

// Storage defines an interface for CRUD operations on peers and torrents
type Storage interface {
	//name of a storage engine
	Name() string
	//Read peer info
	Read(infoHash string) ([]PeerInfo, error)
	//Upsert Peer info
	Update(peerInfo *PeerInfo) error
	//Delete all peers by hash_info
	DeleteAllHashes(infoHash string) error
	//Delete all peers by peerId
	DeleteAllPeers(peerID string) error
	//Delete all peers by torrent name
	DeleteAllPieces(torrentName string)

	//Read torrent info
	ReadTorrent(torrentName string) (*TorrentInfo, error)
	//Create torrent
	CreateTorrent(torrentInfo *TorrentInfo) error
	//Delete torrent by torrent name
	DeleteTorrent(torrentName string) error
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
		log.Error("Datastore factory %s already registered. Ignoring %s.", name)
	}
	datastoreFactories[name] = factory
}

// Init registers all storages in a system
func Init() {
	Register("mysql", NewMySQLStorage)
}

// RunDBMigration detect and Run DB migration if it is needed
func RunDBMigration(appCfg config.AppConfig) error {

	dsnTemplate := appCfg.DBConfig.GetDSN()
	username := appCfg.Nemo.Username["kraken"]

	dsn := fmt.Sprintf(dsnTemplate, username, appCfg.Nemo.Password[username])

	// Open our database connection
	db, err := sql.Open(appCfg.DBConfig.EngineName, dsn)
	if err != nil {
		log.Error("Failed to connect to datastore: ", err.Error())
		return err
	}
	defer db.Close()

	err = goose.SetDialect("mysql")

	if err != nil {
		log.Error("do not support the driver: ", err.Error())
		return err
	}
	arguments := []string{}
	// Get the latest possible migration
	err = goose.Run("up", db, appCfg.DBConfig.MigrationsPath, arguments...)
	if err != nil {
		log.Error("could not run a migration: ", err)
		return err
	}

	return nil
}

// NewMySQLStorage creates and returns new MySQL storage
func NewMySQLStorage(appCfg config.AppConfig) (Storage, error) {

	dsnTemplate := appCfg.DBConfig.GetDSN()
	username := appCfg.Nemo.Username["kraken"]

	dsn := fmt.Sprintf(dsnTemplate, username, appCfg.Nemo.Password[username])

	db, err := sql.Open(appCfg.DBConfig.EngineName, dsn)
	if err != nil {
		log.Error("Failed to connect to datastore: ", err.Error())
		return nil, err
	}

	return &MySQLDataStore{
		appCfg: appCfg,
		db:     db,
	}, nil
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
