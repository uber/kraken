package dockerregistry

import (
	"fmt"
	"log"
	"path"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/dockerregistry/transfer"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/utils/dockerutil"

	"github.com/docker/distribution/context"
	"github.com/uber-go/tally"
)

const (
	repoName         = "alpine"
	tagName          = "latest"
	hashStateContent = "this is a test hashstate"
	uploadContent    = "this is a test upload"
	uploadUUID       = "a20fe261-0060-467f-a44e-46eba3798d63"
)

// TODO(codyg): Get rid of this and all of the above constants.
type testImageUploadBundle struct {
	repo     string
	tag      string
	upload   string
	manifest string
	layer1   *core.BlobFixture
	layer2   *core.BlobFixture
}

type testDriver struct {
	cas        *store.CAStore
	transferer transfer.ImageTransferer
}

func newTestDriver() (*testDriver, func()) {
	cas, cleanup := store.CAStoreFixture()
	transferer := transfer.NewTestTransferer(cas)
	return &testDriver{cas, transferer}, cleanup
}

func (d *testDriver) setup() (*KrakenStorageDriver, testImageUploadBundle) {
	sd := NewReadWriteStorageDriver(Config{}, d.cas, d.transferer, tally.NoopScope)

	// Create upload
	path := genUploadStartedAtPath(uploadUUID)
	if err := sd.uploads.putContent(path, _startedat, nil); err != nil {
		log.Panic(err)
	}
	path = genUploadHashStatesPath(uploadUUID)
	if err := sd.uploads.putContent(path, _hashstates, []byte(hashStateContent)); err != nil {
		log.Panic(err)
	}
	path = genUploadDataPath(uploadUUID)

	writer, err := d.cas.GetUploadFileReadWriter(uploadUUID)
	if err != nil {
		log.Panic(err)
	}
	defer writer.Close()
	writer.Write([]byte(uploadContent))

	config := core.NewBlobFixture()
	layer1 := core.NewBlobFixture()
	layer2 := core.NewBlobFixture()

	manifestDigest, manifestRaw := dockerutil.ManifestFixture(
		config.Digest, layer1.Digest, layer2.Digest)

	for _, blob := range []*core.BlobFixture{config, layer1, layer2} {
		err := d.transferer.Upload("unused", blob.Digest, store.NewBufferFileReader(blob.Content))
		if err != nil {
			log.Panic(err)
		}
	}
	err = d.transferer.Upload("unused", manifestDigest, store.NewBufferFileReader(manifestRaw))
	if err != nil {
		log.Panic(err)
	}

	if err := d.transferer.PostTag(fmt.Sprintf("%s:%s", repoName, tagName), manifestDigest); err != nil {
		log.Panic(err)
	}

	return sd, testImageUploadBundle{
		repo:     repoName,
		tag:      tagName,
		manifest: manifestDigest.Hex(),
		layer1:   layer1,
		layer2:   layer2,
		upload:   uploadUUID,
	}
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

func genManifestListPath(repo string) string {
	return fmt.Sprintf("/docker/registry/v2/repositories/%s/_manifests/tags", repo)
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

func contextFixture() context.Context {
	return context.WithValues(context.Background(), map[string]interface{}{"vars.name": "dummy"})
}
