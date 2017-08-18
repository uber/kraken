package storage

import (
	"os"

	"code.uber.internal/infra/kraken/client/store"
	"code.uber.internal/infra/kraken/configuration"
	"code.uber.internal/infra/kraken/torlib"
)

var _ TorrentArchive = (*LocalTorrentArchive)(nil)

// LocalTorrentArchive implements TorrentArchive
type LocalTorrentArchive struct {
	config *configuration.Config
	store  *store.LocalStore
}

// NewLocalTorrentArchive creates a new LocalTorrentArchive
func NewLocalTorrentArchive(config *configuration.Config, store *store.LocalStore) TorrentArchive {
	return &LocalTorrentArchive{
		config: config,
		store:  store,
	}
}

// CreateTorrent implements TorrentArchive.CreateTorrent
func (a *LocalTorrentArchive) CreateTorrent(ih torlib.InfoHash, mi *torlib.MetaInfo) (Torrent, error) {
	if err := a.store.CreateDownloadFile(mi.Info.Name, mi.Info.Length); err != nil {
		// Duplicated files are not allowed, only one thread will create a download file
		if os.IsExist(err) {
			return a.GetTorrent(mi.Info.Name, ih)
		}
		return nil, err
	}

	// Save metainfo in store so we do not need to query tracker everytime
	miRaw, err := mi.Serialize()
	if err != nil {
		return nil, err
	}

	_, err = a.store.SetDownloadOrCacheFileMeta(mi.Info.Name, []byte(miRaw))
	if err != nil {
		return nil, err
	}

	// Init piece status for empty torrent
	numPieces := mi.Info.NumPieces()
	statues := make([]byte, numPieces)
	for i := 0; i < numPieces; i++ {
		statues[i] = store.PieceClean
	}

	// Save piece status in store
	_, err = a.store.WriteDownloadFilePieceStatus(mi.Info.Name, statues)
	if err != nil {
		return nil, err
	}

	return NewLocalTorrent(a.store, mi), nil
}

// GetTorrent implements TorrentArchive.GetTorrent
// All torrents are content addressable by name, so both name and ih identify a unique torrent
// Our storage supports search by content addressable file name, so torrent name is required
func (a *LocalTorrentArchive) GetTorrent(name string, ih torlib.InfoHash) (Torrent, error) {
	// Get metainfo from disk
	miRaw, err := a.store.GetDownloadOrCacheFileMeta(name)
	if err != nil {
		return nil, err
	}

	mi, err := torlib.NewMetaInfoFromBytes(miRaw)
	if err != nil {
		return nil, err
	}

	// Verify infohash
	// InfoHash changes when metainfo.Info is changed. Overwriting metainfo is disallowed
	// because InfoHash does not match what is in tracker.
	// The correct way is to delete and re-create this torrent.
	if mi.InfoHash.HexString() != ih.HexString() {
		return nil, InfoHashMissMatchError{ih, mi.InfoHash}
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
