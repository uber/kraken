package storage

import (
	"database/sql"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"

	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/infra/kraken/config/tracker"
	"code.uber.internal/infra/kraken/torlib"
	"code.uber.internal/infra/kraken/utils"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/pressly/goose"
)

// MySQLStorage is a MySQL implementaion of a Storage interface
type MySQLStorage struct {
	appCfg config.AppConfig
	db     *sqlx.DB
}

// NewMySQLStorage creates and returns new MySQL storage
func NewMySQLStorage(appCfg config.AppConfig) (Storage, error) {

	dsnTemplate := appCfg.DBConfig.GetDSN()
	username := appCfg.Nemo.Username["kraken"]

	// check if we need to str format,
	// we don't have to do that in integration testing suite
	// as DSN being returned from docker container contains already
	// username and password
	dsn := dsnTemplate
	n := strings.Count(dsnTemplate, "%s")
	if n > 0 {
		dsn = fmt.Sprintf(dsnTemplate, username, appCfg.Nemo.Password[username])
	}

	db, err := sqlx.Open(appCfg.DBConfig.EngineName, dsn)
	if err != nil {
		log.Error("Failed to connect to datastore: ", err.Error())
		return nil, err
	}

	return &MySQLStorage{
		appCfg: appCfg,
		db:     db,
	}, nil
}

// RunDBMigration detect and Run DB migration if it is needed
func RunDBMigration(appCfg config.AppConfig) error {

	dsnTemplate := appCfg.DBConfig.GetDSN()
	username := appCfg.Nemo.Username["kraken"]

	// check if we need to str format,
	// we don't have to do that in integration testing suite
	// as DSN being returned from docker container contains already
	// username and password
	dsn := dsnTemplate
	n := strings.Count(dsnTemplate, "%s")
	if n > 0 {
		dsn = fmt.Sprintf(dsnTemplate, username, appCfg.Nemo.Password[username])
	}

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

// Name returns a Storage string identifier
func (ds *MySQLStorage) Name() string {
	return "MySQLDataStore"
}

// GetPeers implements Storage.GetPeers
func (ds *MySQLStorage) GetPeers(infoHash string) ([]*torlib.PeerInfo, error) {
	var peers []*torlib.PeerInfo
	err := ds.db.Select(&peers, "select * from peer where infoHash=?", infoHash)
	if err != nil {
		log.Errorf("Failed to get peers: %s", err)
		return nil, err
	}

	return peers, nil
}

// UpdatePeer updates PeerInfo in a storage
func (ds *MySQLStorage) UpdatePeer(peer *torlib.PeerInfo) error {
	_, err := ds.db.NamedExec(`insert into peer(infoHash, peerId, dc, ip, port, bytes_downloaded, flags)
	values(:infoHash, :peerId, :dc, :ip, :port, :bytes_downloaded, :flags) on duplicate key update
	dc =:dc, ip =:ip, port =:port, bytes_downloaded =:bytes_downloaded, flags=:flags`,
		map[string]interface{}{
			"infoHash":         peer.InfoHash,
			"peerId":           peer.PeerID,
			"dc":               peer.DC,
			"ip":               peer.IP,
			"port":             strconv.FormatInt(peer.Port, 10),
			"bytes_downloaded": strconv.FormatInt(peer.BytesDownloaded, 10),
			"flags":            strconv.FormatUint(uint64(peer.Flags), 10),
		})

	if err != nil {
		log.Error(err)
		return err
	}

	return nil
}

// GetTorrent reads torrent's metadata identified by a torrent name
func (ds *MySQLStorage) GetTorrent(name string) (string, error) {
	var metaRaw []string
	err := ds.db.Select(&metaRaw, "select metaInfo from torrent where name=?", name)
	if err != nil {
		log.Error(err)
		return "", err
	}
	if len(metaRaw) > 1 {
		log.Fatalf("Duplicated torrent %s", name)
	}

	if len(metaRaw) <= 0 {
		return "", errors.Wrap(os.ErrNotExist, fmt.Sprintf("Cannot find torrent %s", name))
	}
	return metaRaw[0], nil
}

// CreateTorrent creates a torrent in storage
func (ds *MySQLStorage) CreateTorrent(meta *torlib.MetaInfo) error {
	serialized, err := meta.Serialize()
	if err != nil {
		log.Error(err)
		return err
	}

	_, err = ds.db.NamedExec(`insert into torrent(name, infoHash, author, metaInfo)
	values(:name, :infoHash, :author, :metaInfo) on duplicate key update flags = flags`,
		map[string]interface{}{
			"name":     meta.GetName(),
			"infoHash": meta.GetInfoHash().HexString(),
			"author":   meta.CreatedBy,
			"metaInfo": serialized,
		})

	if err != nil {
		log.Error(err)
		return err
	}
	return nil
}

// GetManifest reads manifest from storage
func (ds *MySQLStorage) GetManifest(tag string) (string, error) {
	var manifestRaw []string
	err := ds.db.Select(&manifestRaw, "select data from manifest where tag=?", tag)
	if err != nil {
		log.Error(err)
		return "", err
	}

	if len(manifestRaw) > 1 {
		log.Fatalf("Duplicated tag %s", tag)
	}

	if len(manifestRaw) <= 0 {
		return "", errors.Wrap(os.ErrNotExist, fmt.Sprintf("Cannot find manifest %s", tag))
	}

	return manifestRaw[0], nil
}

// CreateManifest create a new entry in manifest table and then increment refcount for all layers
func (ds *MySQLStorage) CreateManifest(tag, manifestRaw string) error {
	manifestV2, manifestDigest, err := utils.ParseManifestV2([]byte(manifestRaw))
	if err != nil {
		log.Error(err)
		return err
	}

	tors, err := utils.GetManifestV2References(manifestV2, manifestDigest)
	if err != nil {
		log.Error(err)
		return err
	}
	return ds.createManifest(tag, manifestRaw, tors)
}

func (ds *MySQLStorage) createManifest(tag, manifestRaw string, tors []string) error {
	// Sort layerNames in increasing order to avoid transaction deadlock
	sort.Strings(tors)

	tx, err := ds.db.Beginx()
	if err != nil {
		log.Error(err)
		return err
	}

	// Insert manifest
	_, err = tx.NamedExec("insert into manifest(tag, data) values(:tag, :data)",
		map[string]interface{}{
			"tag":  tag,
			"data": manifestRaw,
		})

	if err != nil {
		log.Error(err)
		tx.Rollback()
		return err
	}

	// Increment refcount for all torrents
	for _, tor := range tors {
		_, err = tx.Exec("update torrent set refCount = refCount + 1 where name=?", tor)
		if err != nil {
			log.Error(err)
			tx.Rollback()
			return err
		}
	}

	err = tx.Commit()
	if err != nil {
		log.Error(err)
		return err
	}
	return nil
}

// DeleteManifest delete manifest and then deref all its referenced contents
func (ds *MySQLStorage) DeleteManifest(tag string) error {
	manifest, err := ds.GetManifest(tag)
	if err != nil {
		log.Error(err)
		return err
	}

	manifestV2, manifestDigest, err := utils.ParseManifestV2([]byte(manifest))
	if err != nil {
		log.Error(err)
		return err
	}

	tors, err := utils.GetManifestV2References(manifestV2, manifestDigest)
	if err != nil {
		log.Error(err)
		return err
	}
	return ds.deleteManifest(tag, tors)
}

func (ds *MySQLStorage) deleteManifest(tag string, tors []string) (err error) {
	// Sort layerNames in increasing order to avoid transaction deadlock
	sort.Strings(tors)

	tx, err := ds.db.Beginx()
	if err != nil {
		log.Error(err)
		return err
	}

	// Delete manifest
	_, err = tx.Exec("delete from manifest where tag=?", tag)
	if err != nil {
		log.Error(err)
		tx.Rollback()
		return err
	}

	// Deref all layers
	for _, tor := range tors {
		_, err = tx.Exec("update torrent set refCount = refCount - 1 where name=? and refCount > 0", tor)
		if err != nil {
			log.Error(err)
			tx.Rollback()
			return err
		}

		// try delete torrent when refcount is zero
		_, err = tx.Exec("delete from torrent where name=? and refCount=0", tor)
		if err != nil {
			log.Error(err)
			tx.Rollback()
			return err
		}
	}

	err = tx.Commit()
	if err != nil {
		log.Error(err)
		return err
	}

	// Try delete all layers on origins
	wg := sync.WaitGroup{}
	wg.Add(len(tors))
	for _, tor := range tors {
		go func(tor string) {
			defer wg.Done()
			ds.tryDeleteTorrentOnOrigins(tor)
		}(tor)
	}
	wg.Wait()
	return nil
}

func (ds *MySQLStorage) tryDeleteTorrentOnOrigins(name string) (err error) {
	tx, err := ds.db.Beginx()
	if err != nil {
		log.Error(err)
		return err
	}
	// The transaction only contains a select statement
	defer tx.Commit()

	var count []int
	err = tx.Select(&count, "select refCount from torrent where name=?", name)
	if err != nil {
		return err
	}

	if len(count) > 1 {
		log.Fatalf("Duplicated torrent %s", name)
	}

	// torrent in db
	if len(count) == 1 {
		if count[0] <= 0 {
			return fmt.Errorf("Invalid refCount %d for torrent %s", count[0], name)
		}
		return nil
	}

	// Row not exist, this means we have deleted it

	// TODO (@evelynl):
	// 1. Call origin's endpoint to remove the data with best effort
	// This is not the best practice because we are doing IO within a transcation
	// Temporary we can have a timeout to mitigate potential risk of locking the row forever
	// 2. Also need to apply rendezvous hashing function to findout which origin to call delete
	// So we need a table for origin hosts
	return nil
}
