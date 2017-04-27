package storage

import (
	"os"
	"strconv"
	"testing"

	"code.uber.internal/go-common.git/x/log"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"
)

var (
	store          Storage
	mock           sqlmock.Sqlmock
	peerFixture    *PeerInfo
	torrentFixture *TorrentInfo
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

	//TODO: turn it into a proper fixrture object,
	infoHash := "12345678901234567890"
	peerID := "09876543210987654321"
	portStr := "6881"
	ip := "255.255.255.255"
	downloaded := "1234"
	uploaded := "5678"
	left := "910"
	event := "stopped"

	port, _ := strconv.ParseInt(portStr, 10, 64)
	bytesUploaded, _ := strconv.ParseInt(uploaded, 10, 64)
	bytesDownloaded, _ := strconv.ParseInt(downloaded, 10, 64)
	bytesLeft, _ := strconv.ParseInt(left, 10, 64)

	peerFixture = &PeerInfo{
		InfoHash:        infoHash,
		PeerID:          peerID,
		IP:              ip,
		Port:            port,
		BytesUploaded:   bytesUploaded,
		BytesDownloaded: bytesDownloaded,
		BytesLeft:       bytesLeft,
		Event:           event,
		Flags:           0}

	torrentFixture = &TorrentInfo{
		TorrentName: "torrent",
		InfoHash:    infoHash,
		Author:      "a guy",
		NumPieces:   123,
		PieceLength: 20000,
		RefCount:    1,
		Flags:       0}

	os.Exit(m.Run())
}

func TestShouldInsertPeerInfo(t *testing.T) {
	mock.ExpectExec("insert into peer").WithArgs(
		peerFixture.InfoHash, peerFixture.PeerID, peerFixture.IP, peerFixture.Port,
		peerFixture.BytesUploaded, peerFixture.BytesDownloaded,
		peerFixture.BytesLeft, peerFixture.Event, peerFixture.Flags, // insert part
		peerFixture.IP, peerFixture.Port, peerFixture.BytesUploaded,
		peerFixture.BytesDownloaded, peerFixture.BytesLeft, peerFixture.Event,
		peerFixture.Flags).WillReturnResult(sqlmock.NewResult(1, 1))

	// now we execute our method
	err := store.Update(peerFixture)

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
		torrentFixture.TorrentName, torrentFixture.InfoHash, torrentFixture.Author,
		torrentFixture.NumPieces, torrentFixture.PieceLength, torrentFixture.RefCount,
		torrentFixture.Flags).WillReturnResult(sqlmock.NewResult(1, 1))

	// now we execute our method
	err := store.CreateTorrent(torrentFixture)

	if err != nil {
		t.Errorf("Update has faileds: %s", err)
	}

	// we make sure that all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expections: %s", err)
	}
}
