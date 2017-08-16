package storage

import (
	"os"
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"

	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/infra/kraken/torlib"
)

var (
	storage *MySQLStorage
	mock    sqlmock.Sqlmock
)

func TestMain(m *testing.M) {
	db, sqlMock, err := sqlmock.New()
	mock = sqlMock

	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	dbx := sqlx.NewDb(db, "mysql")
	defer dbx.Close()

	storage = &MySQLStorage{
		db: dbx,
	}

	os.Exit(m.Run())
}

func TestMySQLGetPeers(t *testing.T) {
	assert := require.New(t)
	ih := "0"
	peer0 := "peer0"
	ip0 := "127.0.0.1"
	port0 := 8080
	peer1 := "peer1"
	ip1 := "127.0.0.3"
	port1 := 5000

	rows := sqlmock.NewRows([]string{"infoHash", "peerId", "ip", "port"}).AddRow(ih, peer0, ip0, port0).AddRow(ih, peer1, ip1, port1)
	mock.ExpectQuery("^select (.+) from peer where").WithArgs(ih).WillReturnRows(rows)

	peers, err := storage.GetPeers(ih)

	assert.Nil(err)
	assert.Equal(2, len(peers))
	assert.Equal(peers[0].PeerID, peer0)
	assert.Equal(peers[0].IP, "127.0.0.1")
	assert.Equal(peers[0].Port, int64(8080))
	assert.Equal(peers[0].InfoHash, ih)
	assert.Equal(peers[1].PeerID, peer1)
	assert.Nil(mock.ExpectationsWereMet())
}

func TestMySQLUpdatePeer(t *testing.T) {
	assert := require.New(t)
	p := &torlib.PeerInfo{
		InfoHash:        "1",
		PeerID:          "peer0",
		DC:              "sjc1",
		IP:              "127.0.0.1",
		Port:            int64(8080),
		BytesDownloaded: int64(1),
		Flags:           uint(2),
	}
	mock.ExpectExec("insert into peer").WithArgs(
		// insert
		p.InfoHash, p.PeerID, p.DC, p.IP, "8080", "1", "2",
		// update
		p.DC, p.IP, "8080", "1", "2",
	).WillReturnResult(sqlmock.NewResult(1, 1))

	assert.Nil(storage.UpdatePeer(p))
	assert.Nil(mock.ExpectationsWereMet())
}

func TestMySQLGetTorrent(t *testing.T) {
	assert := require.New(t)
	name := "torrent0"
	metaInfo := "this is a test"
	rows := sqlmock.NewRows([]string{"metaInfo"}).AddRow(metaInfo)
	mock.ExpectQuery("select metaInfo from torrent where").WithArgs(name).WillReturnRows(rows)

	str, err := storage.GetTorrent(name)
	assert.Nil(err)
	assert.Equal(metaInfo, str)
	assert.Nil(mock.ExpectationsWereMet())

	mock.ExpectQuery("select metaInfo from torrent where").WithArgs(name).WillReturnRows(sqlmock.NewRows([]string{"metaInfo"}))
	_, err = storage.GetTorrent(name)
	assert.Equal("Cannot find torrent torrent0: file does not exist", err.Error())
	assert.Nil(mock.ExpectationsWereMet())
}

func TestMySQLCreateTorrent(t *testing.T) {
	assert := require.New(t)
	str := `d8:announce10:trackerurl4:infod6:lengthi2e4:name8:torrent012:piece lengthi1e6:pieces0:eePASS`
	mi, err := torlib.NewMetaInfoFromBytes([]byte(str))
	assert.Nil(err)
	metaRaw, err := mi.Serialize()
	assert.Nil(err)

	mock.ExpectExec("insert into torrent").WithArgs(
		mi.GetName(),
		mi.GetInfoHash().HexString(),
		"",
		metaRaw,
	).WillReturnResult(sqlmock.NewResult(1, 1))

	assert.Nil(storage.CreateTorrent(mi))
	assert.Nil(mock.ExpectationsWereMet())
}

func TestMySQLGetManifest(t *testing.T) {
	assert := require.New(t)
	tag := "tag0"
	data := "this is a manifest"
	rows := sqlmock.NewRows([]string{"data"}).AddRow(data)
	mock.ExpectQuery("select data from manifest where").WithArgs(tag).WillReturnRows(rows)

	str, err := storage.GetManifest(tag)
	assert.Nil(err)
	assert.Equal(data, str)
	assert.Nil(mock.ExpectationsWereMet())

	mock.ExpectQuery("select data from manifest where").WithArgs(tag).WillReturnRows(sqlmock.NewRows([]string{"data"}))

	_, err = storage.GetManifest(tag)
	assert.NotNil(err)
	assert.Equal("Cannot find manifest tag0: file does not exist", err.Error())
	assert.Nil(mock.ExpectationsWereMet())
}

func TestMySQLCreateManifest(t *testing.T) {
	assert := require.New(t)
	tag := "tag1"
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

	mock.ExpectBegin()
	mock.ExpectExec("insert into manifest").WithArgs(tag, manifestStr).WillReturnResult(sqlmock.NewResult(1, 1))
	// torrents are updated in the alphabetical order of their names
	mock.ExpectExec("update torrent set refCount").WithArgs("1508613826413590a9fdb496cbedb0c2ebf564cfbcd2c85c2a07bb3a40813233").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("update torrent set refCount").WithArgs("d2176faa6180566e5e6727e101ba26b13c19ef35f171c9b4419c4d50626aad9d").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("update torrent set refCount").WithArgs("d93b7da35a7f1d51fb163895714dc1923ad235683116e5553a585d0d8f1b1756").WillReturnResult(sqlmock.NewResult(1, 1)) // manifest digest
	mock.ExpectExec("update torrent set refCount").WithArgs("f1f1d5da237f1b069eae23cdc9b291e217a4c1fda8f29262c4275a786a4dd322").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	err := storage.CreateManifest(tag, manifestStr)
	assert.Nil(err)
	assert.Nil(mock.ExpectationsWereMet())
}

func TestMySQLDeleteManifest(t *testing.T) {
	assert := require.New(t)
	layers := []string{
		"d2176faa6180566e5e6727e101ba26b13c19ef35f171c9b4419c4d50626aad9d",
		"1508613826413590a9fdb496cbedb0c2ebf564cfbcd2c85c2a07bb3a40813233",
		"f1f1d5da237f1b069eae23cdc9b291e217a4c1fda8f29262c4275a786a4dd322",
		"d93b7da35a7f1d51fb163895714dc1923ad235683116e5553a585d0d8f1b1756",
	}

	mock.ExpectBegin()
	mock.ExpectExec("delete from manifest").WithArgs("tag1").WillReturnResult(sqlmock.NewResult(1, 1))
	// torrents are updated in the alphabetical order of their names
	mock.ExpectExec("update torrent set refCount").WithArgs("1508613826413590a9fdb496cbedb0c2ebf564cfbcd2c85c2a07bb3a40813233").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("delete from torrent").WithArgs("1508613826413590a9fdb496cbedb0c2ebf564cfbcd2c85c2a07bb3a40813233").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("update torrent set refCount").WithArgs("d2176faa6180566e5e6727e101ba26b13c19ef35f171c9b4419c4d50626aad9d").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("delete from torrent").WithArgs("d2176faa6180566e5e6727e101ba26b13c19ef35f171c9b4419c4d50626aad9d").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("update torrent set refCount").WithArgs("d93b7da35a7f1d51fb163895714dc1923ad235683116e5553a585d0d8f1b1756").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("delete from torrent").WithArgs("d93b7da35a7f1d51fb163895714dc1923ad235683116e5553a585d0d8f1b1756").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("update torrent set refCount").WithArgs("f1f1d5da237f1b069eae23cdc9b291e217a4c1fda8f29262c4275a786a4dd322").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("delete from torrent").WithArgs("f1f1d5da237f1b069eae23cdc9b291e217a4c1fda8f29262c4275a786a4dd322").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	err := storage.deleteManifest("tag1", layers)
	assert.Nil(err)
	assert.Nil(mock.ExpectationsWereMet())
}

func TestMySQLTryDeleteTorrent(t *testing.T) {
	assert := require.New(t)
	assert.True(t.Run("refCound is 1", func(t *testing.T) {
		rows := sqlmock.NewRows([]string{"refCount"}).AddRow(1)
		mock.ExpectBegin()
		mock.ExpectQuery("select refCount from torrent").WithArgs("1508613826413590a9fdb496cbedb0c2ebf564cfbcd2c85c2a07bb3a40813233").WillReturnRows(rows)
		mock.ExpectCommit()

		err := storage.tryDeleteTorrentOnOrigins("1508613826413590a9fdb496cbedb0c2ebf564cfbcd2c85c2a07bb3a40813233")
		assert.Nil(err)
		assert.Nil(mock.ExpectationsWereMet())
	}))

	assert.True(t.Run("refCount is -1", func(t *testing.T) {
		rows := sqlmock.NewRows([]string{"refCount"}).AddRow(-1)
		mock.ExpectBegin()
		mock.ExpectQuery("select refCount from torrent").WithArgs("1508613826413590a9fdb496cbedb0c2ebf564cfbcd2c85c2a07bb3a40813233").WillReturnRows(rows)
		mock.ExpectCommit()

		err := storage.tryDeleteTorrentOnOrigins("1508613826413590a9fdb496cbedb0c2ebf564cfbcd2c85c2a07bb3a40813233")
		assert.NotNil(err)
		assert.Equal("Invalid refCount -1 for torrent 1508613826413590a9fdb496cbedb0c2ebf564cfbcd2c85c2a07bb3a40813233", err.Error())
		assert.Nil(mock.ExpectationsWereMet())
	}))
}
