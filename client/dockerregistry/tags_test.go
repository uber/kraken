package dockerregistry

import (
	"crypto/sha1"
	"encoding/binary"
	"encoding/hex"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	"code.uber.internal/infra/kraken/client/store"
	"code.uber.internal/infra/kraken/client/torrentclient"
	"code.uber.internal/infra/kraken/configuration"

	"github.com/stretchr/testify/assert"
)

func getFileStoreClient() (*configuration.Config, *store.LocalFileStore, *torrentclient.Client) {
	cp := configuration.GetConfigFilePath("agent/test.yaml")
	c := configuration.NewConfigWithPath(cp)
	c.DisableTorrent = true
	c.TagDeletion = struct {
		Enable         bool `yaml:"enable"`
		Interval       int  `yaml:"interval"`
		RetentionCount int  `yaml:"retention_count"`
		RetentionTime  int  `yaml:"retention_time"`
	}{
		Enable:         true,
		RetentionCount: 10,
	}
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

func setup() (*DockerTags, func()) {
	config, filestore, client := getFileStoreClient()
	tags, err := NewDockerTags(config, filestore, client)
	if err != nil {
		log.Fatal(err)
	}
	return tags.(*DockerTags), func() {
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

func TestCreateTag(t *testing.T) {
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

	_, err = tags.CreateTag("linkrepo", "linktag", manifest)
	assert.Nil(t, err)

	// test create tag again, it should return file exists error
	_, err = tags.CreateTag("linkrepo", "linktag", manifest)
	assert.NotNil(t, err)
	assert.True(t, os.IsExist(err))

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

func TestListTags(t *testing.T) {
	tags, teardown := setup()
	defer teardown()

	repoTagMap := map[string][]string{
		"repo1": {
			"tag1",
			"tag2",
			"tag3",
		},
		"repo2": {
			"tag4",
			"tag5",
		},
	}

	for r, ts := range repoTagMap {
		for _, tag := range ts {
			tags.createTag(r, tag, nil)
		}
	}

	// create empty repo
	os.Mkdir(path.Join(tags.config.TagDir, "repo4"), 0755)

	repo1tagsExp := []string{
		"tag1", "tag2", "tag3",
	}
	repo1tags, err := tags.ListTags("repo1")
	assert.Nil(t, err)
	assert.Equal(t, repo1tagsExp, repo1tags)

	repo2tagsExp := []string{
		"tag4", "tag5",
	}
	repo2tags, err := tags.ListTags("repo2")
	assert.Nil(t, err)
	assert.Equal(t, repo2tagsExp, repo2tags)

	_, err = tags.ListTags("notfound")
	assert.NotNil(t, err)
	assert.True(t, os.IsNotExist(err))

	repo4tags, err := tags.ListTags("repo4")
	assert.Nil(t, err)
	assert.Nil(t, repo4tags)
}

func TestListRepos(t *testing.T) {
	tags, teardown := setup()
	defer teardown()

	repoTagMap := map[string][]string{
		"repo1": {
			"tag1",
			"tag2",
			"tag3",
		},
		"repo2": {
			"tag4",
			"tag5",
		},
		"repo3/subrepo": {
			"tag6",
			"tag7",
		},
	}

	for r, ts := range repoTagMap {
		for _, tag := range ts {
			assert.Nil(t, tags.createTag(r, tag, nil))
		}
	}

	// create empty repo
	os.Mkdir(path.Join(tags.config.TagDir, "repo4"), 0755)

	reposExp := []string{
		"repo1", "repo2", "repo3/subrepo", "repo4",
	}
	repos, err := tags.ListRepos()
	assert.Nil(t, err)
	assert.Equal(t, reposExp, repos)
}

func TestGetManifest(t *testing.T) {
	tags, teardown := setup()
	defer teardown()

	manifest := "testgetmanifest"
	manifestTemp := manifest + ".temp"
	tags.store.CreateUploadFile(manifestTemp, 0)
	writer, _ := tags.store.GetUploadFileReadWriter(manifestTemp)
	data, _ := ioutil.ReadFile("./test/testmanifest.json")
	writer.Write(data)
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

	_, err := tags.CreateTag("repo", "tag", manifest)
	assert.Nil(t, err)
	m, err := tags.getManifest("repo", "tag")
	assert.Nil(t, err)
	assert.Equal(t, manifest, m)
}

func TestDeleteTag(t *testing.T) {
	tags, teardown := setup()
	defer teardown()

	manifest := "testdeletetagmanifest"
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

	repoTagMap := map[string][]string{
		"repo1": {
			"tag1",
			"tag2",
			"tag3",
		},
		"repo2": {
			"tag4",
			"tag5",
		},
		"repo3": {
			"tag6",
		},
	}

	for r, ts := range repoTagMap {
		for _, tag := range ts {
			_, err = tags.CreateTag(r, tag, manifest)
			assert.Nil(t, err)
		}
	}

	for _, digest := range []string{
		manifest,
		"1f02865f52ae11e4f76d7c9b6373011cc54ce302c65ce9c54092209d58f1a2c9",
		"0a8490d0dfd399b3a50e9aaa81dba0d425c3868762d46526b41be00886bcc28b",
		"e7e0d0aad96b0a9e5a0e04239b56a1c4423db1040369c3bba970327bf99ffea4",
	} {
		ref := path.Join(tags.config.CacheDir, digest+"_refcount")
		b, _ := ioutil.ReadFile(ref)
		refCount, _ := binary.Varint(b)
		assert.Equal(t, int64(6), refCount)
	}

	// tag deletion dsiabled
	tags.config.TagDeletion.Enable = false
	err = tags.DeleteTag("repo1", "tag1")
	assert.NotNil(t, err)
	assert.Equal(t, "Tag Deletion not enabled", err.Error())

	// delete repo1/tag1
	// reference for manifest, config and layer blobs should decrease 1
	tags.config.TagDeletion.Enable = true
	err = tags.DeleteTag("repo1", "tag1")
	assert.Nil(t, err)
	repo1tags, _ := tags.ListTags("repo1")
	assert.Equal(t, []string{"tag2", "tag3"}, repo1tags)

	// delete repo2/tag1, not found
	err = tags.DeleteTag("repo2", "tag1")
	assert.NotNil(t, err)
	assert.True(t, os.IsNotExist(err))

	// delete repo3/tag6
	err = tags.DeleteTag("repo3", "tag6")
	assert.Nil(t, err)
	_, err = tags.ListTags("repo3")
	assert.NotNil(t, err)
	assert.True(t, os.IsNotExist(err))

	for _, digest := range []string{
		manifest,
		"1f02865f52ae11e4f76d7c9b6373011cc54ce302c65ce9c54092209d58f1a2c9",
		"0a8490d0dfd399b3a50e9aaa81dba0d425c3868762d46526b41be00886bcc28b",
		"e7e0d0aad96b0a9e5a0e04239b56a1c4423db1040369c3bba970327bf99ffea4",
	} {
		ref := path.Join(tags.config.CacheDir, digest+"_refcount")
		b, _ := ioutil.ReadFile(ref)
		refCount, _ := binary.Varint(b)
		assert.Equal(t, int64(4), refCount)
	}

	infos, _ := ioutil.ReadDir(tags.config.TrashDir)
	var deletedTags []string
	for _, info := range infos {
		deletedTags = append(deletedTags, info.Name())
	}
	assert.True(t, strings.HasPrefix(deletedTags[0], string(tags.getTagHash("repo3", "tag6")[:])))
	assert.True(t, strings.HasPrefix(deletedTags[1], string(tags.getTagHash("repo1", "tag1")[:])))
}

func TestDeleteExpiredTags(t *testing.T) {
	tags, teardown := setup()
	defer teardown()

	manifest := "testdeletetagmanifest"
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

	tagList := []Tag{
		{
			repo:    "repo1",
			tagName: "tag4",
			modTime: time.Now().AddDate(0, 0, -20),
		},
		{
			repo:    "repo1",
			tagName: "tag6",
			modTime: time.Now().AddDate(0, 0, -5),
		},
		{
			repo:    "repo1",
			tagName: "tag1",
			modTime: time.Now(),
		},
		{
			repo:    "repo2",
			tagName: "tag3",
			modTime: time.Now().AddDate(0, 0, -10),
		},
		{
			repo:    "repo2",
			tagName: "tag2",
			modTime: time.Now(),
		},
		{
			repo:    "repo3",
			tagName: "tag5",
			modTime: time.Now().AddDate(0, 0, -10),
		},
	}

	for _, tag := range tagList {
		tagFp := tags.getTagPath(tag.repo, tag.tagName)
		assert.Nil(t, err)
		_, err = tags.CreateTag(tag.repo, tag.tagName, manifest)
		assert.Nil(t, err)
		err = os.Chtimes(tagFp, tag.modTime, tag.modTime)
		assert.Nil(t, err)
	}

	for _, digest := range []string{
		manifest,
		"1f02865f52ae11e4f76d7c9b6373011cc54ce302c65ce9c54092209d58f1a2c9",
		"0a8490d0dfd399b3a50e9aaa81dba0d425c3868762d46526b41be00886bcc28b",
		"e7e0d0aad96b0a9e5a0e04239b56a1c4423db1040369c3bba970327bf99ffea4",
	} {
		ref := path.Join(tags.config.CacheDir, digest+"_refcount")
		b, _ := ioutil.ReadFile(ref)
		refCount, _ := binary.Varint(b)
		assert.Equal(t, int64(6), refCount)
	}

	tags.DeleteExpiredTags(2, time.Now())
	repo1tags, _ := tags.ListTags("repo1")
	assert.Equal(t, []string{"tag1", "tag6"}, repo1tags)
	repo2tags, _ := tags.ListTags("repo2")
	assert.Equal(t, []string{"tag2", "tag3"}, repo2tags)
	repo3tags, _ := tags.ListTags("repo3")
	assert.Equal(t, []string{"tag5"}, repo3tags)

	for _, digest := range []string{
		manifest,
		"1f02865f52ae11e4f76d7c9b6373011cc54ce302c65ce9c54092209d58f1a2c9",
		"0a8490d0dfd399b3a50e9aaa81dba0d425c3868762d46526b41be00886bcc28b",
		"e7e0d0aad96b0a9e5a0e04239b56a1c4423db1040369c3bba970327bf99ffea4",
	} {
		ref := path.Join(tags.config.CacheDir, digest+"_refcount")
		b, _ := ioutil.ReadFile(ref)
		refCount, _ := binary.Varint(b)
		assert.Equal(t, int64(5), refCount)
	}

	tags.DeleteExpiredTags(1, time.Now().AddDate(0, 0, -7))
	repo1tags, _ = tags.ListTags("repo1")
	assert.Equal(t, []string{"tag1", "tag6"}, repo1tags)
	repo2tags, _ = tags.ListTags("repo2")
	assert.Equal(t, []string{"tag2"}, repo2tags)
	repo3tags, _ = tags.ListTags("repo3")
	assert.Equal(t, []string{"tag5"}, repo3tags)

	for _, digest := range []string{
		manifest,
		"1f02865f52ae11e4f76d7c9b6373011cc54ce302c65ce9c54092209d58f1a2c9",
		"0a8490d0dfd399b3a50e9aaa81dba0d425c3868762d46526b41be00886bcc28b",
		"e7e0d0aad96b0a9e5a0e04239b56a1c4423db1040369c3bba970327bf99ffea4",
	} {
		ref := path.Join(tags.config.CacheDir, digest+"_refcount")
		b, _ := ioutil.ReadFile(ref)
		refCount, _ := binary.Varint(b)
		assert.Equal(t, int64(4), refCount)
	}
}

func TestGetOrDownloadAllLayersAndCreateTag(t *testing.T) {
	tags, teardown := setup()
	defer teardown()

	manifest := "testdownloadalllayers"
	manifestTemp := manifest + ".temp"
	tags.store.CreateUploadFile(manifestTemp, 0)
	writer, _ := tags.store.GetUploadFileReadWriter(manifestTemp)
	data, _ := ioutil.ReadFile("./test/testmanifest.json")
	writer.Write(data)
	writer.Close()
	assert.Nil(t, tags.store.MoveUploadFileToCache(manifestTemp, manifest))

	for _, digest := range []string{
		"1f02865f52ae11e4f76d7c9b6373011cc54ce302c65ce9c54092209d58f1a2c9",
		"e7e0d0aad96b0a9e5a0e04239b56a1c4423db1040369c3bba970327bf99ffea4",
	} {
		digestTemp := digest + ".temp"
		tags.store.CreateUploadFile(digestTemp, 0)
		tags.store.MoveUploadFileToCache(digestTemp, digest)
	}

	tagSha := string(tags.getTagHash("newrepo", "newtag")[:])
	tagShaTemp := tagSha + ".temp"
	tags.store.CreateUploadFile(tagShaTemp, 0)
	writer, _ = tags.store.GetUploadFileReadWriter(tagShaTemp)
	writer.Write([]byte(manifest))
	writer.Close()
	assert.Nil(t, tags.store.MoveUploadFileToCache(tagShaTemp, tagSha))

	err := tags.getOrDownloadAllLayersAndCreateTag("newrepo", "newtag")
	assert.NotNil(t, err)
	assert.Equal(t, "Torrent disabled", err.Error())

	missingDigest := "0a8490d0dfd399b3a50e9aaa81dba0d425c3868762d46526b41be00886bcc28b"
	missingDigestTemp := missingDigest + ".temp"
	tags.store.CreateUploadFile(missingDigestTemp, 0)
	tags.store.MoveUploadFileToCache(missingDigestTemp, missingDigest)

	l := sync.Mutex{}
	numSuccess := 0
	numFailed := 0
	wg := &sync.WaitGroup{}
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()
			err := tags.getOrDownloadAllLayersAndCreateTag("newrepo", "newtag")
			if err != nil {
				l.Lock()
				numFailed++
				assert.True(t, os.IsExist(err))
				l.Unlock()
			} else {
				l.Lock()
				numSuccess++
				l.Unlock()
			}
		}()
	}

	wg.Wait()
	assert.Equal(t, 1, numSuccess)
	assert.Equal(t, 9, numFailed)

	for _, digest := range []string{
		manifest,
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

func TestGetTag(t *testing.T) {
	tags, teardown := setup()
	defer teardown()

	_, err := tags.GetTag("repo", "tag")
	assert.NotNil(t, err)
	assert.Equal(t, "Torrent disabled", err.Error())

	manifest := "testgettag"
	tagSha := string(tags.getTagHash("repo", "tag")[:])
	tagShaTemp := tagSha + ".temp"
	tags.store.CreateUploadFile(tagShaTemp, 0)
	writer, _ := tags.store.GetUploadFileReadWriter(tagShaTemp)
	writer.Write([]byte(manifest))
	writer.Close()
	assert.Nil(t, tags.store.MoveUploadFileToCache(tagShaTemp, tagSha))

	reader, err := tags.GetTag("repo", "tag")
	assert.Nil(t, err)
	data, _ := ioutil.ReadAll(reader)
	reader.Close()
	assert.Equal(t, manifest, string(data[:]))
}
