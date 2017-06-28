package storage

import (
	"database/sql"
	"fmt"
	"sort"
	"sync"

	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/infra/kraken/config/tracker"
	"code.uber.internal/infra/kraken/utils"
)

//Peer statements
const selectPeerStatememtStr string = `select 
 infoHash, peerId, ip, port, bytes_uploaded, bytes_downloaded, bytes_left, event, flags
from peer where infoHash = ?`

const upsertPeerStatememtStr string = `insert into
 peer(infoHash, peerId, ip, port, bytes_uploaded, bytes_downloaded, bytes_left, event, flags)
 values(?, ?, ?, ?, ?, ?, ?, ?, ?) on duplicate key update ip = ?, port = ?,
 bytes_uploaded = ?, bytes_downloaded = ?, bytes_left = ?, event = ?, flags = ?`

const deletePeerByTorrentStr string = "delete from peer where torrentName = ?"
const deletePeerByHashInfoStr string = "delete from peer where hashInfo = ?"
const deletePeerByPeerIDStr string = "delete from peer where peerId = ?"

//Torrent statements
const selectTorrentStatememtStr string = `select
 torrentName, infoHash, author, numPieces, pieceLength, refcount, flags from torrent where torrentName = ?`
const insertTorrentStatememtStr string = `insert into
 torrent(torrentName, infoHash, author, numPieces, pieceLength, flags) values(?, ?, ?, ?, ?, ?)
 on duplicate key update refcount = refcount`
const deleteTorrentByRefCount string = `delete from torrent where torrentName = ? and refcount = 0`

//Manifest statements
const selectManifestStatememtStr string = `select 
 tagName, manifest, flags from manifest where tagName = ?`
const insertManifestStatememtStr string = `insert into
 manifest(tagName, manifest, flags) values(?, ?, ?)`
const deleteManifestStatementStr string = `delete from manifest where tagName = ?`

const incrementRefCount string = `update torrent set refcount = refcount + 1 where torrentName = ?`
const decrementRefCount string = `update torrent set refcount = refcount - 1 where torrentName = ? and refcount > 0`
const selectRefCount string = `select refcount from torrent where torrentName = ?`

// MySQLDataStore is a MySQL implementaion of a Storage interface
type MySQLDataStore struct {
	appCfg config.AppConfig
	db     *sql.DB
}

// Name returns a Storage string identifier
func (ds *MySQLDataStore) Name() string {
	return "MySQLDataStore"
}

// Read reads PeerInfo identified by infoHash key from a storage
func (ds *MySQLDataStore) Read(infoHash string) ([]*PeerInfo, error) {
	var peers []*PeerInfo

	rows, err := ds.db.Query(selectPeerStatememtStr, infoHash)
	if err != nil {
		log.Errorf("Failed to connect to query datastore: %s", err.Error())
		return peers, err
	}
	defer rows.Close()

	for rows.Next() {
		p := new(PeerInfo)
		if err := rows.Scan(
			&p.InfoHash,
			&p.PeerID,
			&p.IP,
			&p.Port,
			&p.BytesUploaded,
			&p.BytesDownloaded,
			&p.BytesLeft,
			&p.Event,
			&p.Flags); err != nil {

			return peers, err
		}
		peers = append(peers, p)
	}
	if err := rows.Err(); err != nil {
		log.Error(err)
		return peers, err
	}
	return peers, nil
}

// Update updataes PeerInfo in a storage
func (ds *MySQLDataStore) Update(peerInfo *PeerInfo) error {
	_, err := ds.db.Exec(
		upsertPeerStatememtStr,
		//insert
		peerInfo.InfoHash,
		peerInfo.PeerID,
		peerInfo.IP,
		peerInfo.Port,
		peerInfo.BytesUploaded,
		peerInfo.BytesDownloaded,
		peerInfo.BytesLeft,
		peerInfo.Event,
		peerInfo.Flags,

		//update
		peerInfo.IP,
		peerInfo.Port,
		peerInfo.BytesUploaded,
		peerInfo.BytesDownloaded,
		peerInfo.BytesLeft,
		peerInfo.Event,
		peerInfo.Flags,
	)

	if err != nil {
		log.Error(err)
		return err
	}

	return nil
}

// DeleteAllHashes deletes all peers for a particular hash
func (ds *MySQLDataStore) DeleteAllHashes(infoHash string) error {
	_, err := ds.db.Exec(deletePeerByHashInfoStr, infoHash)
	if err != nil {
		log.Error(err)
		return err
	}
	return nil
}

// DeleteAllPeers deletes all peers with a particular peerID
func (ds *MySQLDataStore) DeleteAllPeers(peerID string) error {
	_, err := ds.db.Exec(deletePeerByHashInfoStr, peerID)
	if err != nil {
		log.Error(err)
		return err
	}
	return nil
}

// DeleteAllPieces deletes all pieces for a torrent
func (ds *MySQLDataStore) DeleteAllPieces(torrentName string) {
}

// ReadTorrent reads torrent's metadata identified by a torrent name
func (ds *MySQLDataStore) ReadTorrent(torrentName string) (*TorrentInfo, error) {
	rows, err := ds.db.Query(selectTorrentStatememtStr, torrentName)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		t := new(TorrentInfo)
		if err := rows.Scan(
			&t.TorrentName,
			&t.InfoHash,
			&t.Author,
			&t.NumPieces,
			&t.PieceLength,
			&t.RefCount,
			&t.Flags); err != nil {
			return nil, err
		}
		return t, nil
	}
	if err := rows.Err(); err != nil {
		log.Error(err)
		return nil, err
	}
	return nil, nil
}

// CreateTorrent creates a torrent in storage
func (ds *MySQLDataStore) CreateTorrent(torrentInfo *TorrentInfo) error {
	_, err := ds.db.Exec(
		insertTorrentStatememtStr,
		torrentInfo.TorrentName,
		torrentInfo.InfoHash,
		torrentInfo.Author,
		torrentInfo.NumPieces,
		torrentInfo.PieceLength,
		torrentInfo.Flags)
	if err != nil {
		log.Error(err)
		return err
	}
	return nil
}

// ReadManifest reads manifest from storage
func (ds *MySQLDataStore) ReadManifest(tagName string) (*Manifest, error) {
	rows, err := ds.db.Query(selectManifestStatememtStr, tagName)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		m := new(Manifest)
		if err := rows.Scan(
			&m.TagName,
			&m.Manifest,
			&m.Flags); err != nil {
			return nil, err
		}
		return m, nil
	}
	if err := rows.Err(); err != nil {
		log.Error(err)
		return nil, err
	}
	return nil, nil
}

// CreateManifest create a new entry in manifest table and then increment refcount for all layers
func (ds *MySQLDataStore) CreateManifest(manifest *Manifest) error {
	manifestV2, manifestDigest, err := utils.ParseManifestV2([]byte(manifest.Manifest))
	if err != nil {
		log.Error(err)
		return err
	}

	refs, err := utils.GetManifestV2References(manifestV2, manifestDigest)
	if err != nil {
		log.Error(err)
		return err
	}
	return ds.createManifest(manifest, refs)
}

func (ds *MySQLDataStore) createManifest(manifest *Manifest, refs []string) error {
	// Sort layerNames in increasing order to avoid transaction deadlock
	sort.Strings(refs)

	tx, err := ds.db.Begin()
	if err != nil {
		log.Error(err)
		return err
	}

	// Insert manifest
	_, err = tx.Exec(
		insertManifestStatememtStr,
		manifest.TagName,
		manifest.Manifest,
		manifest.Flags)

	if err != nil {
		log.Error(err)
		tx.Rollback()
		return err
	}

	// Increment refcount for all layers
	for _, ref := range refs {
		_, err = tx.Exec(incrementRefCount, ref)
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
func (ds *MySQLDataStore) DeleteManifest(tagName string) error {
	manifest, err := ds.ReadManifest(tagName)
	if err != nil {
		log.Error(err)
		return err
	}

	manifestV2, manifestDigest, err := utils.ParseManifestV2([]byte(manifest.Manifest))
	if err != nil {
		log.Error(err)
		return err
	}

	refs, err := utils.GetManifestV2References(manifestV2, manifestDigest)
	if err != nil {
		log.Error(err)
		return err
	}
	return ds.deleteManifest(tagName, refs)
}

func (ds *MySQLDataStore) deleteManifest(name string, refs []string) (err error) {
	// Sort layerNames in increasing order to avoid transaction deadlock
	sort.Strings(refs)

	var tx *sql.Tx
	tx, err = ds.db.Begin()
	if err != nil {
		log.Error(err)
		return err
	}

	// Delete manifest
	_, err = tx.Exec(deleteManifestStatementStr, name)
	if err != nil {
		log.Error(err)
		tx.Rollback()
		return err
	}

	// Deref all layers
	for _, ref := range refs {
		_, err = tx.Exec(decrementRefCount, ref)
		if err != nil {
			log.Error(err)
			tx.Rollback()
			return err
		}

		// try delete torrent when refcount is zero
		_, err = tx.Exec(deleteTorrentByRefCount, ref)
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
	wg.Add(len(refs))
	for _, ref := range refs {
		go func(ref string) {
			defer wg.Done()
			ds.tryDeleteTorrentOnOrigins(ref)
		}(ref)
	}
	wg.Wait()
	return nil
}

func (ds *MySQLDataStore) tryDeleteTorrentOnOrigins(name string) (err error) {
	var tx *sql.Tx
	tx, err = ds.db.Begin()
	if err != nil {
		log.Error(err)
		return err
	}

	defer func() {
		if err != nil {
			log.Error(err)
			tx.Rollback()
			return
		}
		err = tx.Commit()
		if err != nil {
			log.Error(err)
		}
	}()

	rows, err := tx.Query(selectRefCount, name)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var count int
		if err := rows.Scan(&count); err != nil {
			return err
		}

		// Refcount < 0, this should not happen, log this error
		if count < 0 {
			err = fmt.Errorf("Negative refcount for %s", name)
			return err
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
