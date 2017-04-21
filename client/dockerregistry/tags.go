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

	"strings"

	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/infra/kraken/client/store"
	"code.uber.internal/infra/kraken/client/torrentclient"
	"code.uber.internal/infra/kraken/configuration"
	"github.com/docker/distribution"
	"github.com/docker/distribution/manifest/schema2"
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
	// Create tag file
	tagFp := t.getTagPath(repo, tag)
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

	// Try get file
	reader, err := t.store.GetCacheFileReader(string(tagSha[:]))
	// TODO (@evelynl): check for file not found error?
	if err == nil {
		return reader, nil
	}

	err = t.client.DownloadByName(string(tagSha[:]))
	if err != nil {
		return nil, err
	}

	reader, err = t.store.GetCacheFileReader(string(tagSha[:]))
	if err != nil {
		return nil, err
	}

	// Create tag file
	err = t.createTag(repo, tag)
	if err != nil {
		return nil, err
	}

	return reader, nil
}

// getAllLayers returns all layers referenced by the manifest, including the manifest itself.
func (t *Tags) getAllLayers(manifestDigest string) ([]string, error) {
	reader, err := t.store.GetCacheFileReader(manifestDigest)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	body, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	manifest, _, err := distribution.UnmarshalManifest(schema2.MediaTypeManifest, body)
	if err != nil {
		return nil, err
	}

	layers := []string{manifestDigest}

	switch manifest.(type) {
	case *schema2.DeserializedManifest:
		// Inc ref count for config and data layers.
		descriptors := manifest.References()
		for _, descriptor := range descriptors {
			layers = append(layers, descriptor.Digest.Hex())
		}
	default:
		return nil, fmt.Errorf("Unsupported manifest format")
	}
	return layers, nil
}

// linkManifest creates a new tag given repo, tag and manifest and a new tag torrent for manifest referencing
// returns tag file sha1
func (t *Tags) linkManifest(repo, tag, manifest string) ([]byte, error) {
	// Create tag torrent in upload directory
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

	// Write manifest digest to tag torrent
	_, err = writer.Write([]byte(manifest))
	if err != nil {
		writer.Close()
		return nil, err
	}
	writer.Close()

	// Inc ref for all layers and the manifest
	layers, err := t.getAllLayers(manifest)
	if err != nil {
		return nil, err
	}
	for _, layer := range layers {
		_, err := t.store.IncrementCacheFileRefCount(layer)
		if err != nil {
			return nil, err
		}
	}

	// Move tag torrent to cache
	err = t.store.MoveUploadFileToCache(randFileName, string(tagSha[:]))
	if err == nil {
		// Create torrent
		fp, err := t.store.GetCacheFilePath(string(tagSha[:]))
		if err != nil {
			return nil, err
		}

		err = t.client.CreateTorrentFromFile(string(tagSha[:]), fp)
		if err != nil {
			return nil, err
		}
	} else if os.IsExist(err) {
		// Someone is pushing an existing tag, which is not allowed.
		// TODO: cleanup upload file
		return nil, err
	} else {
		return nil, err
	}

	// Create tag file
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

	tagInfos, err := ioutil.ReadDir(t.getRepoPath(repo))
	if err != nil {
		return nil, err
	}

	var tags []string
	for _, tagInfo := range tagInfos {
		tags = append(tags, tagInfo.Name())
	}
	return tags, nil
}

// listRepos lists repos under tag directory
// this function can be expensive if there are too many repos
func (t *Tags) listRepos() ([]string, error) {
	t.RLock()
	defer t.RUnlock()

	return t.listReposFromRoot(t.getRepositoriesPath())
}

func (t *Tags) listReposFromRoot(root string) ([]string, error) {
	rootStat, err := os.Stat(root)
	if err != nil {
		return nil, err
	}
	if !rootStat.IsDir() {
		return nil, fmt.Errorf("Failed to list repos. %s is a directory", root)
	}

	repoInfos, err := ioutil.ReadDir(root)
	if err != nil {
		return nil, err
	}

	var repos []string
	foundRepo := false
	for _, repoInfo := range repoInfos {
		if repoInfo.IsDir() {
			foundRepo = true
			var subrepos []string
			var err error
			subrepos, err = t.listReposFromRoot(path.Join(root, repoInfo.Name()))
			if err != nil {
				continue
			}
			repos = append(repos, subrepos...)
		}
	}
	if foundRepo {
		return repos, nil
	}

	// all files in root are tags, return itself
	rootRepo := strings.TrimPrefix(root, t.getRepositoriesPath())
	rootRepo = strings.TrimLeft(rootRepo, "/")
	return []string{rootRepo}, nil
}

// deleteTag deletes a tag given repo and tag
func (t *Tags) deleteTag(repo, tag string) error {
	t.Lock()
	defer t.Unlock()
	return fmt.Errorf("Not implemented")
}

func (t *Tags) getRepoPath(repo string) string {
	return path.Join(t.config.TagDir, repo)
}

func (t *Tags) getTagPath(repo string, tag string) string {
	return path.Join(t.config.TagDir, repo, tag)
}

func (t *Tags) getRepositoriesPath() string {
	return t.config.TagDir
}
