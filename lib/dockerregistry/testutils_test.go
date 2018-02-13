package dockerregistry

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"

	"github.com/uber-go/tally"

	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/utils"
	"code.uber.internal/infra/kraken/utils/testutil"
)

const (
	repoName         = "alpine"
	tagName          = "latest"
	layerContent     = "this is a test layer"
	hashStateContent = "this is a test hashstate"
	uploadContent    = "this is a test upload"
	uploadUUID       = "a20fe261-0060-467f-a44e-46eba3798d63"
)

func genDockerTags() (*DockerTags, func()) {
	var cleanup testutil.Cleanup
	defer cleanup.Recover()

	fsConfig, c := store.ConfigFixture()
	cleanup.Add(c)

	fs, err := store.NewLocalFileStore(fsConfig, tally.NewTestScope("", nil), true)
	if err != nil {
		panic(err)
	}
	cleanup.Add(fs.Close)

	tag, err := ioutil.TempDir("/tmp", "tag")
	if err != nil {
		panic(err)
	}
	cleanup.Add(func() { os.RemoveAll(tag) })

	config := Config{}
	config.TagDir = tag
	config.TagDeletion.Enable = true

	tags, err := NewDockerTags(config, fs, &mockImageTransferer{}, tally.NoopScope)
	if err != nil {
		panic(err)
	}
	return tags.(*DockerTags), cleanup.Run
}

type testImageUploadBundle struct {
	repo     string
	tag      string
	upload   string
	manifest string
	layers   []string
}

func genStorageDriver() (*KrakenStorageDriver, testImageUploadBundle, func()) {
	sd, cleanup := StorageDriverFixture()

	// Create upload
	path := genUploadStartedAtPath(uploadUUID)
	if err := sd.uploads.PutUploadContent(path, _startedat, nil); err != nil {
		log.Panic(err)
	}
	path = genUploadHashStatesPath(uploadUUID)
	if err := sd.uploads.PutUploadContent(path, _hashstates, []byte(hashStateContent)); err != nil {
		log.Panic(err)
	}
	path = genUploadDataPath(uploadUUID)

	writer, err := sd.store.GetUploadFileReadWriter(uploadUUID)
	if err != nil {
		log.Panic(err)
	}
	defer writer.Close()
	writer.Write([]byte(uploadContent))

	manifestContent, err := ioutil.ReadFile("test/testmanifest.json")
	if err != nil {
		log.Panic(err)
	}

	dockermanifest, manifestDigest, err := utils.ParseManifestV2(manifestContent)
	if err != nil {
		log.Panic(err)
	}

	// Create layers
	layers := dockermanifest.References()
	layerDigests := []string{}
	for _, layer := range layers {
		layerDigest := layer.Digest.Hex()
		layerDigestTemp := layerDigest + "-tmp"
		if err := sd.store.CreateUploadFile(layerDigestTemp, int64(len(layerContent))); err != nil {
			log.Panic(err)
		}
		writer, err := sd.store.GetUploadFileReadWriter(layerDigestTemp)
		if err != nil {
			log.Panic(err)
		}
		defer writer.Close()
		writer.Write([]byte(layerContent))
		if err := sd.store.MoveUploadFileToCache(layerDigestTemp, layerDigest); err != nil {
			log.Panic(err)
		}
		layerDigests = append(layerDigests, layerDigest)
	}

	// Create manifest
	manifestDigestTemp := manifestDigest + "-tmp"
	if err := sd.store.CreateUploadFile(manifestDigestTemp, int64(len(manifestContent))); err != nil {
		log.Panic(err)
	}
	writer, err = sd.store.GetUploadFileReadWriter(manifestDigestTemp)
	if err != nil {
		log.Panic(err)
	}
	defer writer.Close()
	writer.Write([]byte(manifestContent))
	if err := sd.store.MoveUploadFileToCache(manifestDigestTemp, manifestDigest); err != nil {
		log.Panic(err)
	}

	// Create tag
	if err := sd.tags.(*DockerTags).createTag(repoName, tagName, manifestDigest, layerDigests); err != nil {
		log.Panic(err)
	}

	return sd, testImageUploadBundle{
		repo:     repoName,
		tag:      tagName,
		manifest: manifestDigest,
		layers:   layerDigests,
		upload:   uploadUUID,
	}, cleanup
}

func genLayerLinkPath(layerDigest string) string {
	return fmt.Sprintf("/docker/registry/v2/repositories/alpine/_layers/sha256/%s/link", layerDigest)
}

func genUploadStartedAtPath(uuid string) string {
	return fmt.Sprintf("/docker/registry/v2/repositories/alpine/_uploads/%s/startedat", uuid)
}

func genUploadHashStatesPath(uuid string) string {
	return fmt.Sprintf("localstore/_uploads/%s/hashstates/sha256/1928129", uuid)
}

func genUploadDataPath(uuid string) string {
	return fmt.Sprintf("/docker/registry/v2/repositories/alpine/_uploads/%s/data", uuid)
}

func genManifestTagCurrentLinkPath(repo, tag, manifest string) string {
	return fmt.Sprintf("/docker/registry/v2/repositories/%s/_manifests/tags/%s/current/link", repo, tag)
}

func genManifestTagShaLinkPath(repo, tag, manifest string) string {
	return fmt.Sprintf("/docker/registry/v2/repositories/%s/_manifests/tags/%s/index/sha256/%s/link", repo, tag, manifest)
}

func genManifestRevisionLinkPath(repo, manifest string) string {
	return fmt.Sprintf("/docker/registry/v2/repositories/%s/_manifests/revisions/sha256/%s/link", repo, manifest)
}

func genBlobDataPath(digest string) string {
	return fmt.Sprintf("/docker/registry/v2/blobs/sha256/%s/%s/data", string([]byte(digest)[:2]), digest)
}

func getShardedRelativePath(name string) string {
	filePath := ""
	for i := 0; i < 2 && i < len(name)/2; i++ {
		// (1 byte = 2 char of file name assumming file name is in HEX)
		dirName := name[i*2 : i*2+2]
		filePath = path.Join(filePath, dirName)
	}

	return path.Join(filePath, name)
}
