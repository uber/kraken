package storage

import (
	"os"
	"testing"

	"gopkg.in/DATA-DOG/go-sqlmock.v1"

	"code.uber.internal/go-common.git/x/log"
)

var (
	store   Storage
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
		torrent.NumPieces, torrent.PieceLength, torrent.RefCount,
		torrent.Flags).WillReturnResult(sqlmock.NewResult(1, 1))

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
