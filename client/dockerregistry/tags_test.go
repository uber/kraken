package dockerregistry

import (
	"io/ioutil"
	"log"
	"os"
	"testing"

	"crypto/sha1"

	"encoding/binary"
	"encoding/hex"

	"path"

	"code.uber.internal/infra/kraken/client/store"
	"code.uber.internal/infra/kraken/client/torrentclient"
	"code.uber.internal/infra/kraken/configuration"
	"github.com/stretchr/testify/assert"
)

func getFileStoreClient() (*configuration.Config, *store.LocalFileStore, *torrentclient.Client) {
	cp := configuration.GetConfigFilePath("agent/test.yaml")
	c := configuration.NewConfig(cp)
	c.DisableTorrent = true
	var err error
	err = os.MkdirAll(c.DownloadDir, 0755)
	if err != nil {
		log.Fatal(err)
	}
	err = os.MkdirAll(c.CacheDir, 0755)
	if err != nil {
		log.Fatal(err)
	}
	err = os.MkdirAll(c.UploadDir, 0755)
	if err != nil {
		log.Fatal(err)
	}
	err = os.MkdirAll(c.TagDir, 0755)
	if err != nil {
		log.Fatal(err)
	}
	c.UploadDir, err = ioutil.TempDir(c.UploadDir, "testtags")
	if err != nil {
		log.Fatal(err)
	}
	c.CacheDir, err = ioutil.TempDir(c.CacheDir, "testtags")
	if err != nil {
		log.Fatal(err)
	}
	c.DownloadDir, err = ioutil.TempDir(c.DownloadDir, "testtags")
	if err != nil {
		log.Fatal(err)
	}
	c.TagDir, err = ioutil.TempDir(c.TagDir, "testtags")
	if err != nil {
		log.Fatal(err)
	}
	s := store.NewLocalFileStore(c)
	client, err := torrentclient.NewClient(c, s, 120)
	if err != nil {
		log.Fatal(err)
	}
	return c, s, client
}

func removeTestTorrentDirs(c *configuration.Config) {
	os.RemoveAll(c.DownloadDir)
	os.RemoveAll(c.CacheDir)
	os.RemoveAll(c.UploadDir)
	os.RemoveAll(c.TagDir)
}

func setup() (*Tags, func()) {
	config, filestore, client := getFileStoreClient()
	tags, err := NewTags(config, filestore, client)
	if err != nil {
		log.Fatal(err)
	}
	return tags, func() {
		removeTestTorrentDirs(config)
	}
}

func TestGetHash(t *testing.T) {
	tags, teardown := setup()
	defer teardown()

	expected := sha1.Sum([]byte("somerepo/sometag"))
	sha := tags.getTagHash("somerepo", "sometag")
	assert.Equal(t, hex.EncodeToString(expected[:]), string(sha[:]))
}

func TestCreateTag(t *testing.T) {
	tags, teardown := setup()
	defer teardown()

	sha := tags.getTagHash("repocreate", "tagcreate")
	assert.Nil(t, tags.createTag("repocreate", "tagcreate"))
	data, err := ioutil.ReadFile(path.Join(tags.config.TagDir, "repocreate", "tagcreate"))
	assert.Nil(t, err)
	assert.Equal(t, string(sha[:]), string(data[:]))
}

func TestGetAllLayers(t *testing.T) {
	tags, teardown := setup()
	defer teardown()

	manifest := "testgetalllayermanifest"
	manifestTemp := manifest + ".temp"
	tags.store.CreateUploadFile(manifestTemp, 0)
	writer, _ := tags.store.GetUploadFileReadWriter(manifestTemp)
	data, _ := ioutil.ReadFile("./test/testmanifest.json")
	_, err := writer.Write(data)
	writer.Close()
	assert.Nil(t, tags.store.MoveUploadFileToCache(manifestTemp, manifest))
	expected := []string{
		"testgetalllayermanifest",                                          // manifest
		"1f02865f52ae11e4f76d7c9b6373011cc54ce302c65ce9c54092209d58f1a2c9", // config
		"0a8490d0dfd399b3a50e9aaa81dba0d425c3868762d46526b41be00886bcc28b", // layer
		"e7e0d0aad96b0a9e5a0e04239b56a1c4423db1040369c3bba970327bf99ffea4", // layer
	}
	layers, err := tags.getAllLayers(manifest)
	assert.Nil(t, err)
	assert.Equal(t, expected, layers)
}

func TestLinkManifest(t *testing.T) {
	tags, teardown := setup()
	defer teardown()

	manifest := "testlinkmanifest"
	manifestTemp := manifest + ".temp"
	tags.store.CreateUploadFile(manifestTemp, 0)
	writer, _ := tags.store.GetUploadFileReadWriter(manifestTemp)
	data, _ := ioutil.ReadFile("./test/testmanifest.json")
	_, err := writer.Write(data)
	writer.Close()
	assert.Nil(t, tags.store.MoveUploadFileToCache(manifestTemp, manifest))

	for _, digest := range []string{
		"1f02865f52ae11e4f76d7c9b6373011cc54ce302c65ce9c54092209d58f1a2c9",
		"0a8490d0dfd399b3a50e9aaa81dba0d425c3868762d46526b41be00886bcc28b",
		"e7e0d0aad96b0a9e5a0e04239b56a1c4423db1040369c3bba970327bf99ffea4",
	} {
		digestTemp := digest + ".temp"
		tags.store.CreateUploadFile(digestTemp, 0)
		tags.store.MoveUploadFileToCache(digestTemp, digest)
	}

	_, err = tags.linkManifest("linkrepo", "linktag", manifest)
	assert.Nil(t, err)

	for _, digest := range []string{
		"testlinkmanifest",
		"1f02865f52ae11e4f76d7c9b6373011cc54ce302c65ce9c54092209d58f1a2c9",
		"0a8490d0dfd399b3a50e9aaa81dba0d425c3868762d46526b41be00886bcc28b",
		"e7e0d0aad96b0a9e5a0e04239b56a1c4423db1040369c3bba970327bf99ffea4",
	} {
		ref := path.Join(tags.config.CacheDir, digest+"_refcount")
		b, _ := ioutil.ReadFile(ref)
		refCount, _ := binary.Varint(b)
		assert.Equal(t, int64(1), refCount)
	}
}
