package storage

import (
	"os"
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"

	"code.uber.internal/infra/kraken/torlib"

	"code.uber.internal/infra/kraken/utils/log"
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

func TestMySQLGetTorrent(t *testing.T) {
	assert := require.New(t)
	name := "torrent0"
	metaInfo := "this is a test"
	rows := sqlmock.NewRows([]string{"metaInfo"}).AddRow(metaInfo)
	mock.ExpectQuery("select metaInfo from torrent where").WithArgs(name).WillReturnRows(rows)

	str, err := storage.GetTorrent(name)
	assert.NoError(err)
	assert.Equal(metaInfo, str)
	assert.NoError(mock.ExpectationsWereMet())

	mock.ExpectQuery("select metaInfo from torrent where").WithArgs(name).WillReturnRows(sqlmock.NewRows([]string{"metaInfo"}))
	_, err = storage.GetTorrent(name)
	assert.Error(ErrNotFound, err)
	assert.NoError(mock.ExpectationsWereMet())
}

func TestMySQLCreateTorrent(t *testing.T) {
	assert := require.New(t)
	mi := torlib.MetaInfoFixture()
	metaRaw, err := mi.Serialize()
	assert.NoError(err)

	mock.ExpectExec("insert into torrent").WithArgs(
		mi.Name(),
		mi.InfoHash.HexString(),
		"",
		metaRaw,
	).WillReturnResult(sqlmock.NewResult(1, 1))

	assert.NoError(storage.CreateTorrent(mi))
	assert.NoError(mock.ExpectationsWereMet())
}

func TestMySQLGetManifest(t *testing.T) {
	assert := require.New(t)
	tag := "tag0"
	data := "this is a manifest"
	rows := sqlmock.NewRows([]string{"data"}).AddRow(data)
	mock.ExpectQuery("select data from manifest where").WithArgs(tag).WillReturnRows(rows)

	str, err := storage.GetManifest(tag)
	assert.NoError(err)
	assert.Equal(data, str)
	assert.NoError(mock.ExpectationsWereMet())

	mock.ExpectQuery("select data from manifest where").WithArgs(tag).WillReturnRows(sqlmock.NewRows([]string{"data"}))

	_, err = storage.GetManifest(tag)
	assert.NotNil(err)
	assert.Equal("Cannot find manifest tag0: file does not exist", err.Error())
	assert.NoError(mock.ExpectationsWereMet())
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
	assert.NoError(err)
	assert.NoError(mock.ExpectationsWereMet())
}

func TestMySQLDeleteManifest(t *testing.T) {
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
	mock.ExpectExec("update torrent set refCount").WithArgs("1508613826413590a9fdb496cbedb0c2ebf564cfbcd2c85c2a07bb3a40813233").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("update torrent set refCount").WithArgs("d2176faa6180566e5e6727e101ba26b13c19ef35f171c9b4419c4d50626aad9d").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("update torrent set refCount").WithArgs("d93b7da35a7f1d51fb163895714dc1923ad235683116e5553a585d0d8f1b1756").WillReturnResult(sqlmock.NewResult(1, 1)) // manifest digest
	mock.ExpectExec("update torrent set refCount").WithArgs("f1f1d5da237f1b069eae23cdc9b291e217a4c1fda8f29262c4275a786a4dd322").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	rows := sqlmock.NewRows([]string{"data"}).AddRow(manifestStr)
	mock.ExpectQuery("select data from manifest where").WithArgs(tag).WillReturnRows(rows)

	mock.ExpectBegin()
	mock.ExpectExec("delete from manifest").WithArgs(tag).WillReturnResult(sqlmock.NewResult(1, 1))
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

	err := storage.CreateManifest(tag, manifestStr)
	assert.NoError(err)

	err = storage.DeleteManifest(tag)
	assert.NoError(err)
	assert.NoError(mock.ExpectationsWereMet())
}

func TestMySQLTryDeleteTorrent(t *testing.T) {
	assert := require.New(t)
	assert.True(t.Run("refCound is 1", func(t *testing.T) {
		rows := sqlmock.NewRows([]string{"refCount"}).AddRow(1)
		mock.ExpectBegin()
		mock.ExpectQuery("select refCount from torrent").WithArgs("1508613826413590a9fdb496cbedb0c2ebf564cfbcd2c85c2a07bb3a40813233").WillReturnRows(rows)
		mock.ExpectCommit()

		err := storage.tryDeleteTorrentOnOrigins("1508613826413590a9fdb496cbedb0c2ebf564cfbcd2c85c2a07bb3a40813233")
		assert.NoError(err)
		assert.NoError(mock.ExpectationsWereMet())
	}))

	assert.True(t.Run("refCount is -1", func(t *testing.T) {
		rows := sqlmock.NewRows([]string{"refCount"}).AddRow(-1)
		mock.ExpectBegin()
		mock.ExpectQuery("select refCount from torrent").WithArgs("1508613826413590a9fdb496cbedb0c2ebf564cfbcd2c85c2a07bb3a40813233").WillReturnRows(rows)
		mock.ExpectCommit()

		err := storage.tryDeleteTorrentOnOrigins("1508613826413590a9fdb496cbedb0c2ebf564cfbcd2c85c2a07bb3a40813233")
		assert.NotNil(err)
		assert.Equal("Invalid refCount -1 for torrent 1508613826413590a9fdb496cbedb0c2ebf564cfbcd2c85c2a07bb3a40813233", err.Error())
		assert.NoError(mock.ExpectationsWereMet())
	}))
}

func TestMySQLCreateTwoTorrentsWithSameNameReturnsErrExist(t *testing.T) {
	require := require.New(t)

	s, err := NewMySQLStorage(nemoConfigFixture(), mysqlConfigFixture())
	require.NoError(err)
	require.NoError(s.RunMigration())

	m1 := torlib.MetaInfoFixture()
	m2 := torlib.MetaInfoFixture()
	m2.Info.Name = m1.Info.Name

	require.NoError(s.CreateTorrent(m1))
	require.Equal(ErrExists, s.CreateTorrent(m2))
}
