package dockerregistry

import (
	"encoding/binary"
	"io/ioutil"
	"os"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestGetAllLayers(t *testing.T) {
	tags, cleanup := genDockerTags()
	defer cleanup()

	manifest := "09b4be55821450cbf046f7ed71c7a1d9512b442c7967004651f7bff084a285c1"
	manifestTemp := manifest + ".temp"
	tags.store.CreateUploadFile(manifestTemp, 0)
	writer, _ := tags.store.GetUploadFileReadWriter(manifestTemp)
	data, _ := ioutil.ReadFile("./test/testmanifest.json")
	_, err := writer.Write(data)
	writer.Close()
	assert.Nil(t, tags.store.MoveUploadFileToCache(manifestTemp, manifest))
	expected := []string{
		manifest, // manifest
		"1f02865f52ae11e4f76d7c9b6373011cc54ce302c65ce9c54092209d58f1a2c9", // config
		"0a8490d0dfd399b3a50e9aaa81dba0d425c3868762d46526b41be00886bcc28b", // layer
		"e7e0d0aad96b0a9e5a0e04239b56a1c4423db1040369c3bba970327bf99ffea4", // layer
	}
	layers, err := tags.getAllLayers(manifest)
	assert.Nil(t, err)
	assert.Equal(t, expected, layers)
}

func TestCreateTag(t *testing.T) {
	tags, cleanup := genDockerTags()
	defer cleanup()

	manifest := "09b4be55821450cbf046f7ed71c7a1d9512b442c7967004651f7bff084a285c1"
	manifestTemp := manifest + ".temp"
	assert.Nil(t, tags.store.CreateUploadFile(manifestTemp, 0))
	writer, _ := tags.store.GetUploadFileReadWriter(manifestTemp)
	data, _ := ioutil.ReadFile("./test/testmanifest.json")
	_, err := writer.Write(data)
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

	// missing one layer
	// ref++ for missingDigest would fail but it will increase ref count for digest before this failure
	err = tags.CreateTag("linkrepo", "linktag", manifest)
	assert.NotNil(t, err)
	assert.True(t, os.IsNotExist(err))

	for _, digest := range []string{
		manifest,
		"1f02865f52ae11e4f76d7c9b6373011cc54ce302c65ce9c54092209d58f1a2c9",
	} {
		ref := path.Join(tags.store.Config().CacheDir, digest+"_refcount")
		b, err := ioutil.ReadFile(ref)
		assert.Nil(t, err)
		refCount, _ := binary.Varint(b)
		assert.Equal(t, int64(1), refCount)
	}

	missingDigest := "0a8490d0dfd399b3a50e9aaa81dba0d425c3868762d46526b41be00886bcc28b"
	missingDigestTemp := missingDigest + ".temp"
	tags.store.CreateUploadFile(missingDigestTemp, 0)
	tags.store.MoveUploadFileToCache(missingDigestTemp, missingDigest)

	err = tags.CreateTag("linkrepo", "linktag", manifest)
	assert.Nil(t, err)

	// test create tag again, it should return file exists error
	err = tags.CreateTag("linkrepo", "linktag", manifest)
	assert.NotNil(t, err)
	assert.True(t, os.IsExist(err))

	for _, digest := range []string{
		manifest,
		"1f02865f52ae11e4f76d7c9b6373011cc54ce302c65ce9c54092209d58f1a2c9",
	} {
		ref := path.Join(tags.store.Config().CacheDir, digest+"_refcount")
		b, _ := ioutil.ReadFile(ref)
		refCount, _ := binary.Varint(b)
		assert.Equal(t, int64(2), refCount)
	}

	for _, digest := range []string{
		missingDigest,
		"e7e0d0aad96b0a9e5a0e04239b56a1c4423db1040369c3bba970327bf99ffea4",
	} {
		ref := path.Join(tags.store.Config().CacheDir, digest+"_refcount")
		b, _ := ioutil.ReadFile(ref)
		refCount, _ := binary.Varint(b)
		assert.Equal(t, int64(1), refCount)
	}
}

func TestListTags(t *testing.T) {
	tags, cleanup := genDockerTags()
	defer cleanup()

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
			tags.createTag(r, tag, "", nil)
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
	tags, cleanup := genDockerTags()
	defer cleanup()

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
			assert.Nil(t, tags.createTag(r, tag, "", nil))
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
	tags, cleanup := genDockerTags()
	defer cleanup()

	manifest := "09b4be55821450cbf046f7ed71c7a1d9512b442c7967004651f7bff084a285c1"
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

	err := tags.CreateTag("repo", "tag", manifest)
	assert.Nil(t, err)
	m, err := tags.getOrDownloadManifest("repo", "tag")
	assert.Nil(t, err)
	assert.Equal(t, manifest, m)
}

func TestDeleteTag(t *testing.T) {
	tags, cleanup := genDockerTags()
	defer cleanup()

	manifest := "09b4be55821450cbf046f7ed71c7a1d9512b442c7967004651f7bff084a285c1"
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
			err = tags.CreateTag(r, tag, manifest)
			assert.Nil(t, err)
		}
	}

	for _, digest := range []string{
		manifest,
		"1f02865f52ae11e4f76d7c9b6373011cc54ce302c65ce9c54092209d58f1a2c9",
		"0a8490d0dfd399b3a50e9aaa81dba0d425c3868762d46526b41be00886bcc28b",
		"e7e0d0aad96b0a9e5a0e04239b56a1c4423db1040369c3bba970327bf99ffea4",
	} {
		ref := path.Join(tags.store.Config().CacheDir, digest+"_refcount")
		b, _ := ioutil.ReadFile(ref)
		refCount, _ := binary.Varint(b)
		assert.Equal(t, int64(6), refCount)
	}

	// tag deletion disabled
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
		ref := path.Join(tags.store.Config().CacheDir, digest+"_refcount")
		b, _ := ioutil.ReadFile(ref)
		refCount, _ := binary.Varint(b)
		assert.Equal(t, int64(4), refCount)
	}
}

func TestDeleteExpiredTags(t *testing.T) {
	tags, cleanup := genDockerTags()
	defer cleanup()

	manifest := "09b4be55821450cbf046f7ed71c7a1d9512b442c7967004651f7bff084a285c1"
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
		err = tags.CreateTag(tag.repo, tag.tagName, manifest)
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
		ref := path.Join(tags.store.Config().CacheDir, digest+"_refcount")
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
		ref := path.Join(tags.store.Config().CacheDir, digest+"_refcount")
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
		ref := path.Join(tags.store.Config().CacheDir, digest+"_refcount")
		b, _ := ioutil.ReadFile(ref)
		refCount, _ := binary.Varint(b)
		assert.Equal(t, int64(4), refCount)
	}
}

func TestGetOrDownloadAllLayersAndCreateTag(t *testing.T) {
	tags, cleanup := genDockerTags()
	defer cleanup()

	repo := "newrepo"
	tag := "newtag"
	manifest := "09b4be55821450cbf046f7ed71c7a1d9512b442c7967004651f7bff084a285c1"
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

	// cannot get manifest from tracker
	err := tags.getOrDownloadAllLayersAndCreateTag(repo, tag)
	assert.NotNil(t, err)

	// create fake tag file because we need to get manifest
	os.MkdirAll(path.Dir(tags.getTagPath(repo, tag)), 0755)
	ioutil.WriteFile(tags.getTagPath(repo, tag), []byte(manifest), 0755)

	err = tags.getOrDownloadAllLayersAndCreateTag(repo, tag)
	assert.NotNil(t, err)

	missingDigest := "0a8490d0dfd399b3a50e9aaa81dba0d425c3868762d46526b41be00886bcc28b"
	missingDigestTemp := missingDigest + ".temp"
	tags.store.CreateUploadFile(missingDigestTemp, 0)
	tags.store.MoveUploadFileToCache(missingDigestTemp, missingDigest)

	// already created tag
	err = tags.getOrDownloadAllLayersAndCreateTag(repo, tag)
	assert.NotNil(t, err)
	assert.True(t, os.IsExist(err))

	for _, digest := range []string{
		manifest,
		"1f02865f52ae11e4f76d7c9b6373011cc54ce302c65ce9c54092209d58f1a2c9",
		"0a8490d0dfd399b3a50e9aaa81dba0d425c3868762d46526b41be00886bcc28b",
		"e7e0d0aad96b0a9e5a0e04239b56a1c4423db1040369c3bba970327bf99ffea4",
	} {
		ref := path.Join(tags.store.Config().CacheDir, digest+"_refcount")
		b, _ := ioutil.ReadFile(ref)
		refCount, _ := binary.Varint(b)
		// since the tag is already created, ref++ wont happen
		assert.Equal(t, int64(0), refCount)
	}
}

func TestGetTag(t *testing.T) {
	tags, cleanup := genDockerTags()
	defer cleanup()

	_, err := tags.GetTag("repo", "tag")
	assert.NotNil(t, err)

	manifest := "testgettag"
	assert.Nil(t, tags.createTag("repo", "tag", manifest, nil))

	m, err := tags.GetTag("repo", "tag")
	assert.Nil(t, err)
	assert.Equal(t, manifest, m)
}
