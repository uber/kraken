package storage

import (
	"os"
	"testing"

	"gopkg.in/DATA-DOG/go-sqlmock.v1"

	"code.uber.internal/go-common.git/x/log"
)

var (
	store   *MySQLDataStore
	mock    sqlmock.Sqlmock
	peer    *PeerInfo
	torrent *TorrentInfo
)

func TestMain(m *testing.M) {
	db, sqlMock, err := sqlmock.New()
	mock = sqlMock

	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	store = &MySQLDataStore{
		db: db,
	}

	torrent = TorrentFixture()
	peer = PeerForTorrentFixture(torrent)

	os.Exit(m.Run())
}

func TestShouldInsertPeerInfo(t *testing.T) {
	mock.ExpectExec("insert into peer").WithArgs(
		peer.InfoHash, peer.PeerID, peer.IP, peer.Port,
		peer.BytesUploaded, peer.BytesDownloaded,
		peer.BytesLeft, peer.Event, peer.Flags, // insert part
		peer.IP, peer.Port, peer.BytesUploaded,
		peer.BytesDownloaded, peer.BytesLeft, peer.Event,
		peer.Flags).WillReturnResult(sqlmock.NewResult(1, 1))

	// now we execute our method
	err := store.Update(peer)

	if err != nil {
		t.Errorf("Update has faileds: %s", err)
	}

	// we make sure that all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expections: %s", err)
	}
}

func TestShouldCreateTorrent(t *testing.T) {
	mock.ExpectExec("insert into torrent").WithArgs(
		torrent.TorrentName, torrent.InfoHash, torrent.Author,
		torrent.NumPieces, torrent.PieceLength, torrent.Flags).WillReturnResult(sqlmock.NewResult(1, 1))

	// now we execute our method
	err := store.CreateTorrent(torrent)

	if err != nil {
		t.Errorf("Update has faileds: %s", err)
	}

	// we make sure that all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expections: %s", err)
	}
}

func TestShouldCreateManifest(t *testing.T) {
	manifestStr := `{
                 "schemaVersion": 2,
                 "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
                 "config": {
                    "mediaType": "application/octet-stream",
                    "size": 11936,
                    "digest": "sha256:d2176faa6180566e5e6727e101ba26b13c19ef35f171c9b4419c4d50626aad9d"
                 },
                 "layers": [{
                    "mediaType": "application/vnd.docker.image.rootfs.diff.tar.gzip",
                    "size": 52998821,
                    "digest": "sha256:1508613826413590a9fdb496cbedb0c2ebf564cfbcd2c85c2a07bb3a40813233"
                 },
                 {
                    "mediaType": "application/vnd.docker.image.rootfs.diff.tar.gzip",
                    "size": 115242848,
                    "digest": "sha256:f1f1d5da237f1b069eae23cdc9b291e217a4c1fda8f29262c4275a786a4dd322"
                  }]}`
	manifest := &Manifest{
		TagName:  "tagName1",
		Manifest: manifestStr,
		Flags:    0,
	}

	mock.ExpectBegin()
	mock.ExpectExec("insert into manifest").WithArgs("tagName1", manifestStr, 0).WillReturnResult(sqlmock.NewResult(1, 1))
	// torrents are updated in the alphabetical order of their names
	mock.ExpectExec("update torrent set refcount").WithArgs("1508613826413590a9fdb496cbedb0c2ebf564cfbcd2c85c2a07bb3a40813233").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("update torrent set refcount").WithArgs("d2176faa6180566e5e6727e101ba26b13c19ef35f171c9b4419c4d50626aad9d").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("update torrent set refcount").WithArgs("d93b7da35a7f1d51fb163895714dc1923ad235683116e5553a585d0d8f1b1756").WillReturnResult(sqlmock.NewResult(1, 1)) // manifest digest
	mock.ExpectExec("update torrent set refcount").WithArgs("f1f1d5da237f1b069eae23cdc9b291e217a4c1fda8f29262c4275a786a4dd322").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	err := store.CreateManifest(manifest)

	if err != nil {
		t.Errorf("Update has faileds: %s", err)
	}

	// we make sure that all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expections: %s", err)
	}
}

func TestShouldDeleteManifest(t *testing.T) {
	layers := []string{
		"d2176faa6180566e5e6727e101ba26b13c19ef35f171c9b4419c4d50626aad9d",
		"1508613826413590a9fdb496cbedb0c2ebf564cfbcd2c85c2a07bb3a40813233",
		"f1f1d5da237f1b069eae23cdc9b291e217a4c1fda8f29262c4275a786a4dd322",
		"d93b7da35a7f1d51fb163895714dc1923ad235683116e5553a585d0d8f1b1756",
	}

	mock.ExpectBegin()
	mock.ExpectExec("delete from manifest").WithArgs("tagName1").WillReturnResult(sqlmock.NewResult(1, 1))
	// torrents are updated in the alphabetical order of their names
	mock.ExpectExec("update torrent set refcount").WithArgs("1508613826413590a9fdb496cbedb0c2ebf564cfbcd2c85c2a07bb3a40813233").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("delete from torrent").WithArgs("1508613826413590a9fdb496cbedb0c2ebf564cfbcd2c85c2a07bb3a40813233").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("update torrent set refcount").WithArgs("d2176faa6180566e5e6727e101ba26b13c19ef35f171c9b4419c4d50626aad9d").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("delete from torrent").WithArgs("d2176faa6180566e5e6727e101ba26b13c19ef35f171c9b4419c4d50626aad9d").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("update torrent set refcount").WithArgs("d93b7da35a7f1d51fb163895714dc1923ad235683116e5553a585d0d8f1b1756").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("delete from torrent").WithArgs("d93b7da35a7f1d51fb163895714dc1923ad235683116e5553a585d0d8f1b1756").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("update torrent set refcount").WithArgs("f1f1d5da237f1b069eae23cdc9b291e217a4c1fda8f29262c4275a786a4dd322").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("delete from torrent").WithArgs("f1f1d5da237f1b069eae23cdc9b291e217a4c1fda8f29262c4275a786a4dd322").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	err := store.deleteManifest("tagName1", layers)

	if err != nil {
		t.Errorf("Update has faileds: %s", err)
	}

	// we make sure that all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expections: %s", err)
	}
}

func TestShouldTryDeleteTorrent(t *testing.T) {
	mock.ExpectBegin()
	mock.ExpectQuery("select refcount from torrent").WithArgs("1508613826413590a9fdb496cbedb0c2ebf564cfbcd2c85c2a07bb3a40813233").WillReturnRows(sqlmock.NewRows([]string{}))
	mock.ExpectCommit()

	err := store.tryDeleteTorrentOnOrigins("1508613826413590a9fdb496cbedb0c2ebf564cfbcd2c85c2a07bb3a40813233")

	if err != nil {
		t.Errorf("Update has faileds: %s", err)
	}

	// we make sure that all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expections: %s", err)
	}
}
