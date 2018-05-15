package originstorage

import (
	"errors"
	"fmt"
	"os"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/blobrefresh"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/lib/torrent/storage"
	"code.uber.internal/infra/kraken/utils/log"

	"github.com/willf/bitset"
)

// TorrentArchive is a TorrentArchive for origin peers. It assumes that
// all files (including metainfo) are already downloaded and in the cache directory.
type TorrentArchive struct {
	fs            store.OriginFileStore
	blobRefresher *blobrefresh.Refresher
}

// NewTorrentArchive creates a new TorrentArchive.
func NewTorrentArchive(
	fs store.OriginFileStore, blobRefresher *blobrefresh.Refresher) *TorrentArchive {

	return &TorrentArchive{fs, blobRefresher}
}

func (a *TorrentArchive) getMetaInfo(namespace, name string) (*core.MetaInfo, error) {
	d, err := core.NewSHA256DigestFromHex(name)
	if err != nil {
		return nil, fmt.Errorf("new digest: %s", err)
	}
	raw, err := a.fs.GetCacheFileMetadata(name, store.NewTorrentMeta())
	if err != nil {
		if os.IsNotExist(err) {
			refreshErr := a.blobRefresher.Refresh(namespace, d)
			if refreshErr != nil {
				return nil, fmt.Errorf("blob refresh: %s", refreshErr)
			}
			log.With("name", name).Infof("Missing torrent triggered remote blob refresh")
			return nil, errors.New("refreshing blob")
		}
		return nil, err
	}
	mi, err := core.DeserializeMetaInfo(raw)
	if err != nil {
		return nil, fmt.Errorf("deserialize metainfo: %s", err)
	}
	return mi, nil
}

// Stat returns TorrentInfo for given file name. If the file does not exist,
// attempts to re-fetch the file from the storae backend configured for namespace
// in a background goroutine.
func (a *TorrentArchive) Stat(namespace, name string) (*storage.TorrentInfo, error) {
	mi, err := a.getMetaInfo(namespace, name)
	if err != nil {
		return nil, err
	}
	bitfield := bitset.New(uint(mi.Info.NumPieces())).Complement()
	return storage.NewTorrentInfo(mi, bitfield), nil
}

// CreateTorrent is not supported.
func (a *TorrentArchive) CreateTorrent(namespace, name string) (storage.Torrent, error) {
	return nil, errors.New("not supported for origin")
}

// GetTorrent returns a Torrent for an existing file on disk. If the file does
// not exist, attempts to re-fetch the file from the storae backend configured
// for namespace in a background goroutine, and returns os.ErrNotExist.
func (a *TorrentArchive) GetTorrent(namespace, name string) (storage.Torrent, error) {
	mi, err := a.getMetaInfo(namespace, name)
	if err != nil {
		return nil, err
	}
	t, err := NewTorrent(a.fs, mi)
	if err != nil {
		return nil, fmt.Errorf("initialize torrent: %s", err)
	}
	return t, nil
}

// DeleteTorrent moves a torrent to the trash.
func (a *TorrentArchive) DeleteTorrent(name string) error {
	if err := a.fs.DeleteCacheFile(name); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}
