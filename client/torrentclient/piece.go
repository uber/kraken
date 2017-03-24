package torrentclient

import (
	"fmt"

	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/infra/kraken/configuration"

	"os"
)

const (
	clean    = uint8(0)
	dirty    = uint8(1)
	done     = uint8(2)
	dontCare = uint8(3)
)

// PieceStore contains piece information of a layer
type PieceStore struct {
	status uint8
	index  int
	config *configuration.Config
	ls     *LayerStore
}

// NewPieceStore returns a new PieceStore
func NewPieceStore(ls *LayerStore, index int, status uint8) *PieceStore {
	return &PieceStore{
		ls:     ls,
		config: ls.config,
		index:  index,
		status: status,
	}
}

func (ps *PieceStore) compareAndSwapStatus(fp string, currStatus byte, newStatus byte) (bool, error) {
	ps.ls.m.mu.Lock()
	defer ps.ls.m.mu.Unlock()

	off := int64(ps.index)

	b := make([]byte, 1)
	f, err := os.OpenFile(fp, os.O_RDWR, perm)
	if err != nil {
		return false, err
	}
	defer f.Close()

	_, err = f.ReadAt(b, off)
	if err != nil {
		return false, err
	}

	if b[0] == currStatus || currStatus == dontCare {
		// same status, no need to write
		if b[0] == newStatus {
			return true, nil
		}
		b[0] = newStatus
		_, err = f.WriteAt(b, off)
		if err != nil {
			return false, err
		}
		ps.status = newStatus
		return true, nil
	}

	if b[0] == newStatus {
		return false, nil
	}

	return false, fmt.Errorf("Current status not matched. Expected: %v. Actual: %v", currStatus, b[0])
}

// WriteAt writes buffered piece data to layer
func (ps *PieceStore) WriteAt(p []byte, off int64) (n int, err error) {
	offset := int64(ps.config.Agent.PieceLength*ps.index) + off
	log.Debugf("readAt index: %d, status: %v offset: %d (%d)", ps.index, ps.status, off, offset)
	// check downloading status
	downloading, err := ps.ls.IsDownloading()
	if err != nil {
		log.Error(err.Error())
		return 0, err
	}

	// do not allow write if the file is no downloading
	if !downloading {
		return 0, fmt.Errorf("Missing data file for %s in downloading directory", ps.ls.name)
	}

	f, err := os.OpenFile(ps.ls.downloadPath(), os.O_RDWR, perm)
	if err != nil {
		log.Error(err.Error())
		return 0, err
	}
	defer f.Close()

	// update status
	ok, err := ps.compareAndSwapStatus(ps.ls.pieceStatusPath(), clean, dirty)
	if !ok && err != nil {
		log.Error(err.Error())
		return 0, err
	}

	// only one thread will meet this condition
	if ok {
		defer ps.compareAndSwapStatus(ps.ls.pieceStatusPath(), dirty, clean)
		// write
		n, err = f.WriteAt(p, offset)
		if err != nil {
			log.Error(err.Error())
			return 0, err
		}
		return
	}

	return 0, fmt.Errorf("Another thread is writing to the same piece %d", ps.index)
}

func (ps *PieceStore) readFromCache(p []byte, off int64) (ok bool, n int, err error) {
	_, ok = ps.ls.m.lru.Get(GetLayerKey(ps.ls.name), func(fp string) error {
		f, ferr := os.Open(fp)
		if ferr != nil {
			return ferr
		}
		defer f.Close()
		n, err = f.ReadAt(p, off)
		return err
	})

	// cache hit, this means the layer is downloaded completely
	if ok {
		return
	}

	// cache miss
	log.Errorf("ReadFromCache miss %s", p)
	return false, 0, nil
}

func (ps *PieceStore) readAt(p []byte, off int64) (n int, err error) {
	// assume file is being downloaded, try read from downloading directory
	var f *os.File
	var ok bool
	f, err = os.Open(ps.ls.downloadPath())
	if err != nil {
		// if cannot find file in download directory, try read from cache
		_, isPathErr := err.(*os.PathError)
		if isPathErr {
			ok, n, err = ps.readFromCache(p, off)
			if err != nil {
				log.Error(err.Error())
				return n, err
			}

			// cache hit and read ok
			if ok {
				return n, nil
			}

			log.Errorf("File %s does not exist in either download directory %s or cache", f.Name(), ps.ls.downloadPath())
			// file not exist anywhere
			return 0, fmt.Errorf("File %s does not exists in either download directory %s or cache", f.Name(), ps.ls.downloadPath())
		}

		// error opening file
		return 0, err
	}

	// possible race condition when the file get renamed and cached
	// the caller will need to retry
	log.Debug(ps.ls.downloadPath())
	n, err = f.ReadAt(p, off)
	f.Close()
	return
}

// ReadAt reads piece data to buffer. ReadAt can happen either while the torrent is downloading or it is downloaded.
func (ps *PieceStore) ReadAt(p []byte, off int64) (n int, err error) {
	offset := int64(ps.config.Agent.PieceLength*ps.index) + off
	log.Debugf("readAt index: %d, status: %v offset: %d (%d)", ps.index, ps.status, off, offset)
	n, err = ps.readAt(p, offset)
	if err != nil {
		return ps.readAt(p, offset)
	}

	return
}

// MarkComplete marks piece as complete
func (ps *PieceStore) MarkComplete() error {
	_, err := ps.compareAndSwapStatus(ps.ls.pieceStatusPath(), dontCare, done)
	if err != nil {
		return err
	}

	// try cache layer
	ps.ls.TryCacheLayer()

	return nil
}

// MarkNotComplete marks piece as incomplete
func (ps *PieceStore) MarkNotComplete() error {
	_, ok := ps.ls.m.lru.Get(GetLayerKey(ps.ls.name), nil)
	if ok {
		// read error, could mean the data is corrupted, remove from cache
		ps.ls.m.lru.Remove(GetLayerKey(ps.ls.name))
		return nil
	}
	_, err := ps.compareAndSwapStatus(ps.ls.pieceStatusPath(), dontCare, clean)
	return err
}

// GetIsComplete returns completion status of the piece
func (ps *PieceStore) GetIsComplete() bool {
	_, ok := ps.ls.m.lru.Get(GetLayerKey(ps.ls.name), nil)
	if ok {
		return true
	}
	_, err := ps.compareAndSwapStatus(ps.ls.pieceStatusPath(), done, done)
	if err == nil {
		return true
	}
	return false
}
