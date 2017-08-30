package dockerregistry

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/infra/kraken/client/store"
	"code.uber.internal/infra/kraken/client/torrent"
	"code.uber.internal/infra/kraken/utils"

	"github.com/uber-common/bark"
	"github.com/uber-go/tally"
)

const (
	pullTimer            = "dockertag.time.pull"
	createSuccessCounter = "dockertag.success.create"
	createFailureCounter = "dockertag.failure.create"
	getSuccessCounter    = "dockertag.success.get"
	getFailureCounter    = "dockertag.failure.get"
)

// Tags is an interface
type Tags interface {
	ListTags(repo string) ([]string, error)
	ListRepos() ([]string, error)
	DeleteTag(repo, tag string) error
	GetTag(repo, tag string) (string, error)
	CreateTag(repo, tag, manifest string) error
	DeleteExpiredTags(n int, expireTime time.Time) error
}

// DockerTags handles tag lookups
// a tag is a file with tag_path = <tag_dir>/<repo>/<tag>
// content of the file is sha1(<tag_path>), which is the name of a (torrent) file in cache_dir
// torrent file <cache_dir>/<sha1(<tag_path>)> is a link between tag and manifest
// the content of it is the manifest digest of the tag
type DockerTags struct {
	sync.RWMutex

	config  *Config
	store   *store.LocalStore
	client  torrent.Client
	metrics tally.Scope
}

// Tag stores information about one tag.
type Tag struct {
	repo    string
	tagName string
	modTime time.Time
}

// TagSlice is used for sorting tags
type TagSlice []Tag

func (s TagSlice) Less(i, j int) bool { return s[i].modTime.Before(s[j].modTime) }
func (s TagSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s TagSlice) Len() int           { return len(s) }

// NewDockerTags returns new DockerTags
func NewDockerTags(c *Config, s *store.LocalStore, cl torrent.Client, metrics tally.Scope) (Tags, error) {
	err := os.MkdirAll(c.TagDir, 0755)
	if err != nil {
		return nil, err
	}
	return &DockerTags{
		config:  c,
		store:   s,
		client:  cl,
		metrics: metrics,
	}, nil
}

// ListTags lists tags under given repo
func (t *DockerTags) ListTags(repo string) ([]string, error) {
	t.RLock()
	defer t.RUnlock()

	return t.listTags(repo)
}

// ListRepos lists repos under tag directory
// this function can be expensive if there are too many repos
func (t *DockerTags) ListRepos() ([]string, error) {
	t.RLock()
	defer t.RUnlock()

	return t.listReposFromRoot(t.getRepositoriesPath())
}

// DeleteTag deletes a tag given repo and tag
func (t *DockerTags) DeleteTag(repo, tag string) error {
	if !t.config.TagDeletion.Enable {
		return fmt.Errorf("Tag Deletion not enabled")
	}

	t.Lock()
	defer t.Unlock()

	c := make(chan byte, 1)
	var tags []string
	var listErr error
	// list tags nonblocking
	go func() {
		tags, listErr = t.listTags(repo)
		c <- 'c'
	}()

	manifestDigest, err := t.getOrDownloadManifest(repo, tag)
	if err != nil {
		return err
	}

	layers, err := t.getAllLayers(manifestDigest)
	if err != nil {
		return err
	}

	<-c
	if listErr != nil {
		log.Errorf("Error listing tags in repo %s:%s. Err: %s", repo, tag, err.Error())
	} else {
		// remove repo along with the tag
		// if this is the last tag in the repo
		if len(tags) == 1 && tags[0] == tag {
			err = os.RemoveAll(t.getRepoPath(repo))
		} else {
			// delete tag file
			err = os.Remove(t.getTagPath(repo, tag))
		}
		if err != nil {
			return err
		}
	}

	for _, layer := range layers {
		_, err := t.store.DerefCacheFile(layer)
		if err != nil {
			// TODO (@evelynl): if decrement ref count fails, we might have some garbage layers that are never deleted
			// one possilbe solution is that we can add a reconciliation routine to re-calcalate ref count for all layers
			// another is that we use sqlite
			log.Errorf("Error decrement ref count for layer %s from %s:%s. Err: %s", layer, repo, tag, err.Error())
		}
	}
	return nil
}

// DeleteExpiredTags deletes tags that are older than expireTime and not in the n latest.
func (t *DockerTags) DeleteExpiredTags(n int, expireTime time.Time) error {
	repos, err := t.ListRepos()
	if err != nil {
		return err
	}
	for _, repo := range repos {
		tags, err := t.listExpiredTags(repo, n, expireTime)
		if err != nil {
			return err
		}
		for _, tag := range tags {
			log.Infof("Deleting tag %s", tag)
			t.DeleteTag(repo, tag)
		}
	}

	return nil
}

// GetTag returns a reader of tag content
// the manifest and layers referenced by the tag may or may not exist
func (t *DockerTags) GetTag(repo, tag string) (manifestDigest string, err error) {
	manifestDigest, err = t.getOrDownloadTag(repo, tag)
	if err != nil {
		t.metrics.Counter(getFailureCounter).Inc(1)
	} else {
		t.metrics.Counter(getSuccessCounter).Inc(1)
	}
	return
}

// CreateTag creates a new tag given repo, tag and manifest and a new tag torrent for manifest referencing and returns tag file sha1
// it expects the manifest and all layers referenced by the tag exists
func (t *DockerTags) CreateTag(repo, tag, manifest string) error {
	// Inc ref for all layers and the manifest
	layers, err := t.getAllLayers(manifest)
	if err != nil {
		log.Errorf("CreateTag: cannot get all layers for %s:%s, error: %s", repo, tag, err)
		t.metrics.Counter(createFailureCounter).Inc(1)
		return err
	}

	// Create tag file and increment ref count
	err = t.createTag(repo, tag, manifest, layers)
	if err != nil {
		log.Errorf("CreateTag: cannot create a tag for %s:%s, error: %s", repo, tag, err)
		t.metrics.Counter(createFailureCounter).Inc(1)
		return err
	}

	// Save manifest in tracker
	err = t.client.PostManifest(repo, tag, manifest)
	if err != nil {
		log.Errorf("CreateTag: cannot post manifest for %s:%s, error: %s", repo, tag, err)
		t.metrics.Counter(createFailureCounter).Inc(1)
		return err
	}

	log.WithFields(bark.Fields{
		"repo":     repo,
		"tag":      tag,
		"manifest": manifest,
	}).Info("Successfully created tag")
	t.metrics.Counter(createSuccessCounter).Inc(1)

	return nil
}

// createTag creates a new tag file given repo and tag
// returns tag file sha1
func (t *DockerTags) createTag(repo, tag, manifestDigest string, layers []string) error {
	t.Lock()
	defer t.Unlock()

	tagFp := t.getTagPath(repo, tag)

	// If tag already exists, return file exists error
	_, err := os.Stat(tagFp)
	if err == nil {
		return os.ErrExist
	}
	if !os.IsNotExist(err) {
		return err
	}

	if t.config.TagDeletion.Enable {
		for _, layer := range layers {
			// TODO (@evelynl): if increment ref count fails and the caller retries, we might have
			// some garbage layers that are never deleted. One possilbe solution is that we can add
			// a reconciliation routine to re-calcalate ref count for all layers; Another is that we
			// use sqlite
			_, err := t.store.RefCacheFile(layer)
			if err != nil {
				log.Error(err)
				return err
			}
		}
	}

	// Create tag file
	err = os.MkdirAll(path.Dir(tagFp), 0755)
	if err != nil {
		return err
	}

	err = ioutil.WriteFile(tagFp, []byte(manifestDigest), 0755)
	if err != nil {
		return err
	}

	return nil
}

// getOrDownloadTaglink gets a tag torrent reader or download one
func (t *DockerTags) getOrDownloadTag(repo, tag string) (string, error) {
	manifestDigest, err := t.getOrDownloadManifest(repo, tag)
	if err != nil {
		return "", err
	}

	// start downloading layers in advance
	go t.getOrDownloadAllLayersAndCreateTag(repo, tag)

	return manifestDigest, nil
}

// getOrDownloadAllLayersAndCreateTag downloads all data for a tag
func (t *DockerTags) getOrDownloadAllLayersAndCreateTag(repo, tag string) error {
	sw := t.metrics.Timer(pullTimer).Start()
	defer sw.Stop()

	manifestDigest, err := t.getOrDownloadManifest(repo, tag)
	if err != nil {
		log.Errorf("Error getting manifest for %s:%s", repo, tag)
		return err
	}

	log.Infof("Successfully got manifest %s for %s:%s", manifestDigest, repo, tag)

	layers, err := t.getAllLayers(manifestDigest)
	if err != nil {
		log.Errorf("Error getting layers from manifest %s for %s:%s", manifestDigest, repo, tag)
		return err
	}

	log.Infof("Successfully parsed layers from %s: %v", manifestDigest, layers)

	numLayers := len(layers)
	wg := &sync.WaitGroup{}
	wg.Add(numLayers)
	// errors is a channel to collect errors
	errors := make(chan error, numLayers)

	// for every layer, download if it is already
	for _, layer := range layers {
		go func(l string) {
			defer wg.Done()
			var err error
			_, err = t.store.GetCacheFileStat(l)
			if err != nil && os.IsNotExist(err) {
				err = t.client.DownloadTorrent(l)
			}

			if err != nil {
				log.Errorf("Error getting layer %s from manifest %s for %s:%s", l, manifestDigest, repo, tag)
				errors <- err
			}
		}(layer)
	}

	wg.Wait()
	select {
	// if there is any error downloading layers, we return without incrementing ref count nor creating tag
	case err = <-errors:
		return err
	default:
	}

	return t.createTag(repo, tag, manifestDigest, layers)
}

// getAllLayers returns all layers referenced by the manifest, including the manifest itself.
// this function assumes manifest exists in cache already
func (t *DockerTags) getAllLayers(manifestDigest string) ([]string, error) {
	reader, err := t.store.GetCacheFileReader(manifestDigest)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	body, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	manifest, digest, err := utils.ParseManifestV2(body)
	if err != nil {
		return nil, err
	}

	return utils.GetManifestV2References(manifest, digest)
}

func (t *DockerTags) listTags(repo string) ([]string, error) {
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

func (t *DockerTags) listReposFromRoot(root string) ([]string, error) {
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

	// All files in root are tags, return itself
	rootRepo := strings.TrimPrefix(root, t.getRepositoriesPath())
	rootRepo = strings.TrimLeft(rootRepo, "/")
	return []string{rootRepo}, nil
}

func (t *DockerTags) getRepoPath(repo string) string {
	return path.Join(t.config.TagDir, repo)
}

func (t *DockerTags) getTagPath(repo string, tag string) string {
	return path.Join(t.config.TagDir, repo, tag)
}

func (t *DockerTags) getRepositoriesPath() string {
	return t.config.TagDir
}

func (t *DockerTags) getOrDownloadManifest(repo, tag string) (string, error) {
	tagFp := t.getTagPath(repo, tag)
	_, err := os.Stat(tagFp)
	if err != nil && !os.IsNotExist(err) {
		return "", err
	}

	if os.IsNotExist(err) {
		return t.client.GetManifest(repo, tag)
	}

	data, err := ioutil.ReadFile(tagFp)
	if err != nil {
		return "", err
	}
	return string(data[:]), nil
}

// listExpiredTags lists expired tags under given repo.
// They are not in the latest n tags and reached expireTime.
func (t *DockerTags) listExpiredTags(repo string, n int, expireTime time.Time) ([]string, error) {
	t.RLock()
	defer t.RUnlock()

	tagInfos, err := ioutil.ReadDir(t.getRepoPath(repo))
	if err != nil {
		return nil, err
	}

	// Sort tags by creation time
	s := make(TagSlice, 0)
	for _, tagInfo := range tagInfos {
		tag := Tag{
			repo:    repo,
			tagName: tagInfo.Name(),
			modTime: tagInfo.ModTime(),
		}
		s = append(s, tag)
	}
	sort.Sort(s)

	var tags []string
	for i := 0; i < len(s)-n; i++ {
		if s[i].modTime.After(expireTime) {
			break
		}
		tags = append(tags, s[i].tagName)
	}

	return tags, nil
}
