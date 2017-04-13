package dockerregistry

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"sync"

	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/infra/kraken/client/store"
	"code.uber.internal/infra/kraken/client/torrentclient"
	"code.uber.internal/infra/kraken/configuration"
	"github.com/docker/distribution/uuid"
	"github.com/uber-common/bark"
)

// Tags handles tag lookups
// a tag is a file with tag_path = <tag_dir>/<repo>/<tag>
// content of the file is sha1(<tag_path>), which is the name of a (torrent) file in cache_dir
// torrent file <cache_dir>/<sha1(<tag_path>)> is a link between tag and manifest
// the content of it is the manifest digest of the tag
type Tags struct {
	sync.RWMutex

	config *configuration.Config
	store  *store.LocalFileStore
	client *torrentclient.Client
}

// NewTags returns new Tags
func NewTags(c *configuration.Config, s *store.LocalFileStore, cl *torrentclient.Client) (*Tags, error) {
	err := os.MkdirAll(c.TagDir, 0755)
	if err != nil {
		return nil, err
	}
	return &Tags{
		config: c,
		store:  s,
		client: cl,
	}, nil
}

// getTaghash returns the hash of the tag reference given repo and tag
func (t *Tags) getTagHash(repo, tag string) []byte {
	tagFp := path.Join(repo, tag)
	rawtagSha := sha1.Sum([]byte(tagFp))
	return []byte(hex.EncodeToString(rawtagSha[:]))
}

// createTag creates a new tag file given repo and tag
// returns tag file sha1
func (t *Tags) createTag(repo, tag string) error {
	t.Lock()
	defer t.Unlock()
	// create tag file
	tagFp := path.Join(t.config.TagDir, repo, tag)
	err := os.MkdirAll(path.Dir(tagFp), 0755)
	if err != nil {
		return err
	}

	tagSha := t.getTagHash(repo, tag)
	err = ioutil.WriteFile(tagFp, tagSha, 0755)
	if err != nil {
		return err
	}
	return nil
}

// getOrDownloadTaglink gets a tag torrent reader or download one
func (t *Tags) getOrDownloadTaglink(repo, tag string) (io.ReadCloser, error) {
	tagSha := t.getTagHash(repo, tag)

	// try get file
	reader, err := t.store.GetCacheFileReader(string(tagSha[:]))
	// TODO (@evelynl): check for file not found error?
	if err == nil {
		return reader, nil
	}

	// download file and try again
	tor, err := t.client.AddTorrentByName(string(tagSha[:]))
	if err != nil {
		return nil, err
	}

	err = t.client.TimedDownload(tor)
	if err != nil {
		return nil, err
	}

	reader, err = t.store.GetCacheFileReader(string(tagSha[:]))
	if err != nil {
		return nil, err
	}

	// create tag file
	err = t.createTag(repo, tag)
	if err != nil {
		return nil, err
	}

	return reader, nil
}

// linkManifest creates a new tag given repo, tag and manifest and a new tag torrent for manifest referencing
// returns tag file sha1
func (t *Tags) linkManifest(repo, tag, manifest string) ([]byte, error) {
	// create tag torrent in upload directory
	tagSha := t.getTagHash(repo, tag)
	randFileName := string(tagSha[:]) + "." + uuid.Generate().String()
	_, err := t.store.CreateUploadFile(randFileName, int64(len(tagSha)))
	if err != nil {
		return nil, err
	}

	writer, err := t.store.GetUploadFileReadWriter(randFileName)
	if err != nil {
		return nil, err
	}

	// write manifest digest to tag torrent
	_, err = writer.Write([]byte(manifest))
	if err != nil {
		writer.Close()
		return nil, err
	}
	writer.Close()

	// move tag torrent to cache
	// TODO (@evelynl): file exist error
	err = t.store.MoveUploadFileToCache(randFileName, string(tagSha[:]))
	if err != nil {
		return nil, err
	}
	fp, err := t.store.GetCacheFilePath(string(tagSha[:]))
	if err != nil {
		return nil, err
	}

	err = t.client.CreateTorrentFromFile(string(tagSha[:]), fp)
	if err != nil {
		return nil, err
	}

	// create tag file
	err = t.createTag(repo, tag)
	if err != nil {
		return nil, err
	}

	log.WithFields(bark.Fields{
		"repo":     repo,
		"tag":      tag,
		"tagsha":   string(tagSha[:]),
		"manifest": manifest,
	}).Info("Successfully created tag")

	return tagSha[:], nil
}

// listTags lists tags under given repo
func (t *Tags) listTags(repo string) ([]string, error) {
	t.RLock()
	defer t.RUnlock()
	return nil, fmt.Errorf("Not implemented.")
}

// listRepos lists repos under tag directory
func (t *Tags) listRepos(repo string) ([]string, error) {
	t.RLock()
	defer t.RUnlock()
	return nil, fmt.Errorf("Not implemented.")
}

// deleteTag deletes a tag given repo and tag
func (t *Tags) deleteTag(repo, tag string) error {
	t.Lock()
	defer t.Unlock()
	return fmt.Errorf("Not implemented.")
}
