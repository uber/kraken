package torrentclient

import (
	"fmt"

	"time"

	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/infra/kraken/client/store"
	"code.uber.internal/infra/kraken/configuration"
	"code.uber.internal/infra/kraken/utils"
)

const (
	readAtRetries    = 3
	readAtRetrySleep = 1 //sec
)

// Piece contains piece information of a layer
type Piece struct {
	config    *configuration.Config
	store     *store.LocalFileStore
	name      string
	index     int
	numPieces int
}

func (p *Piece) getOffset(off int64) int64 {
	return int64(p.config.Agent.PieceLength*p.index) + off
}

// WriteAt writes buffered piece data to layer
func (p *Piece) WriteAt(data []byte, off int64) (n int, err error) {
	// set metadata
	updated, err := p.store.WriteDownloadFilePieceStatusAt(p.name, []byte{store.PieceDirty}, p.index)
	if err != nil {
		return 0, err
	}
	if !updated {
		return 0, fmt.Errorf("Another thread is writing to the same piece %s: %d", p.name, p.index)
	}

	// reset metadata
	defer func() {
		updated, err := p.store.WriteDownloadFilePieceStatusAt(p.name, []byte{store.PieceClean}, p.index)
		if err != nil {
			log.Error(err)
			return
		}

		if !updated {
			log.Errorf("Another thread is marking the same piece as clean. This should not happend. %s: %d", p.name, p.index)
		}
		return
	}()

	offset := p.getOffset(off)
	writer, err := p.store.GetDownloadFileReadWriter(p.name)
	if err != nil {
		log.Errorf("Cannt get file writer for %s: %d", p.name, p.index)
		return 0, err
	}
	defer writer.Close()

	return writer.WriteAt(data, offset)
}

func (p *Piece) readAt(data []byte, off int64) (retry bool, n int, err error) {
	reader, err := p.store.GetDownloadOrCacheFileReader(p.name)
	if err != nil {
		return true, 0, err
	}
	defer reader.Close()

	offset := int64(p.config.Agent.PieceLength*p.index) + off
	n, err = reader.ReadAt(data, offset)
	return false, n, err
}

// ReadAt reads piece data to buffer. ReadAt can happen either while the torrent is downloading or it is downloaded.
func (p *Piece) ReadAt(data []byte, off int64) (n int, err error) {
	// when a torrent compeletes the download
	// it is moved to the cache directory after all current reads finish and block further reads
	// we don't want to directly fail the read, instead we retry a number of times
	var retry bool
	for i := 0; i < readAtRetries; i++ {
		retry, n, err = p.readAt(data, off)
		if !retry {
			return
		}
		// sleep and retry
		time.Sleep(readAtRetrySleep * time.Second)
	}
	return
}

// MarkComplete marks piece as complete
func (p *Piece) MarkComplete() error {
	_, err := p.store.WriteDownloadFilePieceStatusAt(p.name, []byte{store.PieceDone}, p.index)
	if err != nil {
		return err
	}

	status, err := p.store.GetFilePieceStatus(p.name, 0, p.numPieces)
	if err != nil {
		return err
	}

	expected := make([]byte, p.numPieces)
	for i := 0; i < p.numPieces; i++ {
		expected[i] = store.PieceDone
	}

	if utils.CompareByteArray(expected, status) {
		err = p.store.MoveDownloadFileToCache(p.name)
		if err != nil {
			log.Errorf("Download completed but failed to move file to cache directory: %s", err.Error())
		} else {
			log.Infof("Download completed and moved %s to cache directory", p.name)
		}
	}
	return nil
}

// MarkNotComplete marks piece as incomplete
func (p *Piece) MarkNotComplete() error {
	_, err := p.store.WriteDownloadFilePieceStatusAt(p.name, []byte{store.PieceClean}, p.index)
	return err
}

// GetIsComplete returns completion status of the piece
func (p *Piece) GetIsComplete() bool {
	status, err := p.store.GetFilePieceStatus(p.name, p.index, 1)
	if err != nil {
		log.Error(err)
		return false
	}

	if status != nil && status[0] == store.PieceDone {
		return true
	}

	return false
}
