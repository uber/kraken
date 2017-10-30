package storage

import (
	"fmt"
	"os"

	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/torlib"
)

var _ TorrentArchive = (*LocalTorrentArchive)(nil)

// LocalTorrentArchive implements TorrentArchive
type LocalTorrentArchive struct {
	store store.FileStore
}

// NewLocalTorrentArchive creates a new LocalTorrentArchive
func NewLocalTorrentArchive(store store.FileStore) *LocalTorrentArchive {
	return &LocalTorrentArchive{store}
}

// CreateTorrent creates a torrent file on disk, or returns os.ErrExist if the
// torrent already exists.
func (a *LocalTorrentArchive) CreateTorrent(mi *torlib.MetaInfo) (Torrent, error) {

	// We ignore existing download / metainfo file errors to allow thread
	// interleaving: if two threads try to create the same torrent at the same
	// time, said files will be created exactly once and both threads will succeed.

	if err := a.store.CreateDownloadFile(mi.Info.Name, mi.Info.Length); err != nil && !os.IsExist(err) {
		return nil, fmt.Errorf("create download file: %s", err)
	}
	// Save metainfo in store so we do not need to query tracker everytime
	miRaw, err := mi.Serialize()
	if err != nil {
		return nil, fmt.Errorf("serialize metainfo: %s", err)
	}
	if _, err := a.store.SetDownloadOrCacheFileMeta(mi.Info.Name, []byte(miRaw)); err != nil && !os.IsExist(err) {
		return nil, fmt.Errorf("write metainfo: %s", err)
	}
	return NewLocalTorrent(a.store, mi), nil
}

// GetTorrent implements TorrentArchive.GetTorrent
// All torrents are content addressable by name, so both name and ih identify a unique torrent
// Our storage supports search by content addressable file name, so torrent name is required
// Returns os.ErrNotExist if the torrent does not exist.
func (a *LocalTorrentArchive) GetTorrent(name string, ih torlib.InfoHash) (Torrent, error) {
	miRaw, err := a.store.GetDownloadOrCacheFileMeta(name)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, err
		}
		return nil, fmt.Errorf("get metainfo: %s", err)
	}

	mi, err := torlib.NewMetaInfoFromBytes(miRaw)
	if err != nil {
		return nil, fmt.Errorf("parse metainfo: %s", err)
	}

	// Verify infohash
	// InfoHash changes when metainfo.Info is changed. Overwriting metainfo is disallowed
	// because InfoHash does not match what is in tracker.
	// The correct way is to delete and re-create this torrent.
	if mi.InfoHash.HexString() != ih.HexString() {
		return nil, InfoHashMismatchError{ih, mi.InfoHash}
	}

	return NewLocalTorrent(a.store, mi), nil
}

// DeleteTorrent implements TorrentArchive.DeleteTorrent
func (a *LocalTorrentArchive) DeleteTorrent(name string, ih torlib.InfoHash) error {
	return a.store.MoveDownloadOrCacheFileToTrash(name)
}

// Close implements TorrentArchive.Close
func (a *LocalTorrentArchive) Close() error {
	return nil
}
