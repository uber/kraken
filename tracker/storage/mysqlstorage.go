package storage

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"sync"

	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/go-common.git/x/mysql"
	"code.uber.internal/infra/kraken/torlib"
	"code.uber.internal/infra/kraken/utils"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/pressly/goose"
)

// MySQLStorage is a MySQL implementaion of a Storage interface
type MySQLStorage struct {
	config MySQLConfig
	db     *sqlx.DB
}

// NewMySQLStorage creates and returns new MySQL storage.
func NewMySQLStorage(nemo mysql.Configuration, config MySQLConfig) (*MySQLStorage, error) {
	dsn, err := nemo.GetDefaultDSN()
	if err != nil {
		return nil, fmt.Errorf("error getting dsn: %s", err)
	}
	db, err := sqlx.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to mysql: %s", err)
	}
	return &MySQLStorage{config, db}, nil
}

// RunMigration runs MySQL database migration if it is needed.
func (s *MySQLStorage) RunMigration() error {
	if err := goose.SetDialect("mysql"); err != nil {
		return err
	}
	if err := goose.Run("up", s.db.DB, s.config.MigrationsDir); err != nil {
		return err
	}
	return nil
}

// GetPeers implements Storage.GetPeers
func (s *MySQLStorage) GetPeers(infoHash string) ([]*torlib.PeerInfo, error) {
	var peers []*torlib.PeerInfo
	err := s.db.Select(&peers, "select * from peer where infoHash=?", infoHash)
	if err != nil {
		log.Errorf("Failed to get peers: %s", err)
		return nil, err
	}

	return peers, nil
}

// UpdatePeer updates PeerInfo in a storage
func (s *MySQLStorage) UpdatePeer(peer *torlib.PeerInfo) error {
	_, err := s.db.NamedExec(`insert into peer(infoHash, peerId, dc, ip, port, bytes_downloaded, flags)
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
func (s *MySQLStorage) GetTorrent(name string) (string, error) {
	var metaRaw []string
	err := s.db.Select(&metaRaw, "select metaInfo from torrent where name=?", name)
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
func (s *MySQLStorage) CreateTorrent(meta *torlib.MetaInfo) error {
	serialized, err := meta.Serialize()
	if err != nil {
		log.Error(err)
		return err
	}

	_, err = s.db.NamedExec(`insert into torrent(name, infoHash, author, metaInfo)
	values(:name, :infoHash, :author, :metaInfo) on duplicate key update flags = flags`,
		map[string]interface{}{
			"name":     meta.Name(),
			"infoHash": meta.InfoHash.HexString(),
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
func (s *MySQLStorage) GetManifest(tag string) (string, error) {
	var manifestRaw []string
	err := s.db.Select(&manifestRaw, "select data from manifest where tag=?", tag)
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
func (s *MySQLStorage) CreateManifest(tag, manifestRaw string) error {
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
	return s.createManifest(tag, manifestRaw, tors)
}

func (s *MySQLStorage) createManifest(tag, manifestRaw string, tors []string) error {
	// Sort layerNames in increasing order to avoid transaction deadlock
	sort.Strings(tors)

	tx, err := s.db.Beginx()
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
func (s *MySQLStorage) DeleteManifest(tag string) error {
	manifest, err := s.GetManifest(tag)
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
	return s.deleteManifest(tag, tors)
}

func (s *MySQLStorage) deleteManifest(tag string, tors []string) (err error) {
	// Sort layerNames in increasing order to avoid transaction deadlock
	sort.Strings(tors)

	tx, err := s.db.Beginx()
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
			s.tryDeleteTorrentOnOrigins(tor)
		}(tor)
	}
	wg.Wait()
	return nil
}

func (s *MySQLStorage) tryDeleteTorrentOnOrigins(name string) (err error) {
	tx, err := s.db.Beginx()
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
