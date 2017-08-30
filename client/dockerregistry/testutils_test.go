package dockerregistry

import (
	"errors"
	"io/ioutil"
	"log"
	"os"

	"code.uber.internal/infra/kraken/client/store"
	"github.com/uber-go/tally"
)

var errMockError = errors.New("MockTorrent")

type mockTorrentClient struct{}

func (mc *mockTorrentClient) DownloadTorrent(name string) error                   { return errMockError }
func (mc *mockTorrentClient) CreateTorrentFromFile(name, filepath string) error   { return errMockError }
func (mc *mockTorrentClient) GetManifest(repo, tag string) (string, error)        { return "", errMockError }
func (mc *mockTorrentClient) PostManifest(repo, tag, manifestDigest string) error { return nil }
func (mc *mockTorrentClient) Close() error                                        { return nil }

func genDockerTags() (*DockerTags, func()) {
	s, cleanupStore := store.LocalStoreWithRefcountFixture()
	tag, err := ioutil.TempDir("/tmp", "tag")
	if err != nil {
		cleanupStore()
		log.Panic(err)
	}
	c := &Config{}
	c.TagDir = tag
	c.TagDeletion.Enable = true

	cleanup := func() {
		cleanupStore()
		os.RemoveAll(c.TagDir)
	}

	tags, err := NewDockerTags(c, s, &mockTorrentClient{}, tally.NoopScope)
	if err != nil {
		cleanup()
		log.Fatal(err)
	}
	return tags.(*DockerTags), cleanup
}
