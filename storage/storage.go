package storage

import (
	"os"

	"sync"

	"io/ioutil"

	"regexp"

	"code.uber.internal/go-common.git/x/log"
	cache "code.uber.internal/infra/dockermover/storage"
	"code.uber.internal/infra/kraken/configuration"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/anacrolix/torrent/storage"
)

const perm = 0755

// Manager implements a data storage for torrent. It should be initiated only once at the start of the program
type Manager struct {
	config *configuration.Config
	lru    *cache.FileCacheMap
	opened map[string]*LayerStore
	mu     *sync.RWMutex
}

// NewManager returns a new Manager
func NewManager(config *configuration.Config) (*Manager, error) {
	// init download dir
	err := os.MkdirAll(config.DownloadDir, perm)
	if err != nil {
		return nil, err
	}

	// init cache dir
	err = os.MkdirAll(config.CacheDir, perm)
	if err != nil {
		return nil, err
	}

	// init new cache
	l, err := cache.NewFileCacheMap(config.CacheMapSize, config.CacheSize)
	if err != nil {
		return nil, err
	}

	m := Manager{
		config: config,
		lru:    l,
		opened: make(map[string]*LayerStore),
		mu:     &sync.RWMutex{},
	}

	m.LoadFromDisk()

	return &m, nil
}

func (m *Manager) loadOpenedTorrent() error {
	files, err := ioutil.ReadDir(m.config.DownloadDir)
	if err != nil {
		return err
	}

	re := regexp.MustCompile(statusSuffix + "$")

	for _, file := range files {
		// skip status file
		if re.MatchString(file.Name()) {
			continue
		} else {
			ls := NewLayerStore(m, file.Name())
			// load torrent from disk
			err := ls.LoadFromDisk()
			if err != nil {
				// if there is an error, remove torrent completely
				os.Remove(file.Name())
				os.Remove(file.Name() + statusSuffix)
				continue
			}
			// add torrent to opened map
			m.opened[file.Name()] = ls
		}
	}
	return nil
}

func (m *Manager) loadCache() error {
	files, err := ioutil.ReadDir(m.config.CacheDir)
	if err != nil {
		return err
	}

	for _, file := range files {
		_, ok, _ := m.lru.Add(GetLayerKey(file.Name()), m.config.CacheDir+file.Name(), nil)
		if !ok {
			os.Remove(file.Name())
		}
	}
	return nil
}

// LoadFromDisk is called at restart of the program to resume torrents
func (m *Manager) LoadFromDisk() {
	err := m.loadCache()
	if err != nil {
		log.Error(err.Error())
	}

	err = m.loadOpenedTorrent()
	if err != nil {
		log.Error(err.Error())
	}
}

// OpenTorrent returns torrent specified by the info
func (m *Manager) OpenTorrent(info *metainfo.Info, infoHash metainfo.Hash) (storage.TorrentImpl, error) {
	// Check if torrent is already opened
	m.mu.Lock()
	defer m.mu.Unlock()
	ls, ok := m.opened[info.Name]
	// torrent already opened
	if ok {
		return ls, nil
	}

	// new torrent, create new LayerStore
	ls = NewLayerStore(m, info.Name)
	// new torrent, create an empty data file in downloading directory
	err := ls.CreateEmptyLayerFile(info.Length, info.NumPieces())
	if err != nil {
		return nil, err
	}
	m.opened[info.Name] = ls
	return ls, nil
}

// Close closes the storage
func (m *Manager) Close() error {
	return nil
}
