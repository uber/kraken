package storage

import (
	"database/sql"

	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/infra/kraken/config/tracker"
)

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

const selectTorrentStatememtStr string = `select
 torrentName, infoHash, author, numPieces, pieceLength, flags from torrent where torrentName = ?`
const insertTorrentStatememtStr string = `insert ignore into
 torrent(torrentName, infoHash, author, numPieces, pieceLength, flags) values(?, ?, ?, ?, ?, ?)`
const deleteTorrentStatememtStr string = "delete from torrent where torrentName = ?"

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
func (ds *MySQLDataStore) Read(infoHash string) ([]PeerInfo, error) {
	var peers []PeerInfo

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
		peers = append(peers, *p)
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

// DeleteTorrent deletes torrent from storage
func (ds *MySQLDataStore) DeleteTorrent(torrentName string) error {
	_, err := ds.db.Exec(
		deleteTorrentStatememtStr, torrentName)
	if err != nil {
		log.Error(err)
		return err
	}
	return nil
}
