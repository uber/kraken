package dockerregistry

import (
	"fmt"
	"os"

	"code.uber.internal/go-common.git/x/log"
)

// ChanWriteCloser is a writecloser with a channel
type ChanWriteCloser struct {
	Chan      chan byte
	tempPath  string
	F         *os.File
	closed    bool
	canceled  bool
	committed bool
	append    bool
}

// NewChanWriteCloser creates new ChanWriteCloser given dest string and tempPrefix string
func NewChanWriteCloser(path string, append bool) (*ChanWriteCloser, error) {
	log.Debugf("Writecloser %s", path)
	return &ChanWriteCloser{
		Chan:      make(chan byte, 1),
		tempPath:  path,
		closed:    false,
		canceled:  false,
		committed: false,
		append:    append,
	}, nil
}

// Write writes p to file
func (wc *ChanWriteCloser) Write(p []byte) (int, error) {
	log.Debugf("Write %s", wc.tempPath)
	var f *os.File
	var err error
	if wc.append {
		f, err = os.OpenFile(wc.tempPath, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
	} else {
		f, err = os.OpenFile(wc.tempPath, os.O_WRONLY, 0755)
	}
	if err != nil {
		return 0, err
	}
	defer f.Close()
	n, err := f.Write(p)
	return n, err
}

// Close closes file and set closed to true
func (wc *ChanWriteCloser) Close() error {
	log.Debugf("Close %s", wc.tempPath)
	wc.closed = true
	return nil
}

// Size returns the size of the file
func (wc *ChanWriteCloser) Size() (size int64) {
	log.Debugf("Size %s", wc.tempPath)
	f, err := os.Open(wc.tempPath)
	if err != nil {
		return -1
	}
	defer f.Close()
	fs, err := f.Stat()
	if err != nil {
		return -1
	}
	size = fs.Size()
	return
}

// Cancel cancel the write and remove the tempfile
func (wc *ChanWriteCloser) Cancel() error {
	log.Debugf("Cancel %s", wc.tempPath)
	// close file
	if !wc.closed {
		err := wc.Close()
		if err != nil {
			return err
		}
	}

	// signal cancel
	wc.Chan <- uint8(0)
	wc.canceled = true

	// remove file
	return os.Remove(wc.tempPath)
}

// Commit close and send signal to Channel
func (wc *ChanWriteCloser) Commit() error {
	log.Debugf("Commit %s", wc.tempPath)
	// check if committed
	if wc.committed {
		return fmt.Errorf("File %s already committed.", wc.tempPath)
	}

	// check if canceled
	if wc.canceled {
		return fmt.Errorf("File %s write canceled.", wc.tempPath)
	}

	// close if it is not
	if !wc.closed {
		err := wc.Close()
		if err != nil {
			return err
		}
	}

	// signal
	wc.Chan <- uint8(1)
	wc.committed = true

	return nil
}
