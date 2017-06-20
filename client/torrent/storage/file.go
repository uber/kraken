package storage

import (
	"io"
	"os"
	"path/filepath"

	"code.uber.internal/go-common.git/x/log"

	"code.uber.internal/infra/kraken/client/torrent/meta"
)

// FileTorrentStorage defines a file-based storage for torrents, that isn't yet bound to a particular
// torrent.
type FileTorrentStorage struct {
	baseDir   string
	pathMaker func(baseDir string, info *meta.Info, infoHash meta.Hash) string
}

// FileTorrent is an actual torrent's file content interface
type FileTorrent struct {
	ts       *FileTorrentStorage
	dir      string
	info     *meta.Info
	infoHash meta.Hash
}

// PathMaker creates a file name and path for a torrent
type PathMaker func(baseDir string, info *meta.Info, infoHash meta.Hash) string

// The Default path maker just returns the current path
func defaultPathMaker(baseDir string, info *meta.Info, infoHash meta.Hash) string {
	return baseDir
}

func infoHashPathMaker(baseDir string, info *meta.Info, infoHash meta.Hash) string {
	return filepath.Join(baseDir, infoHash.HexString())
}

// NewFileStorage creates a new file based storage for a torrent
func NewFileStorage(baseDir string) TorrentStorage {
	return NewFileWithCustomPathMaker(baseDir, nil)
}

// NewFileWithCustomPathMaker allows passing a function to determine the path for storing torrent data
func NewFileWithCustomPathMaker(baseDir string, pathMaker PathMaker) TorrentStorage {
	if pathMaker == nil {
		pathMaker = defaultPathMaker
	}
	fs := &FileTorrentStorage{
		baseDir:   baseDir,
		pathMaker: pathMaker,
	}

	return fs
}

// Close closes a file storage
func (fs *FileTorrentStorage) Close() error {
	return nil
}

// OpenTorrent opens a new torrent and returns read/write interface to it
func (fs *FileTorrentStorage) OpenTorrent(info *meta.Info, infoHash meta.Hash) (Torrent, error) {
	dir := fs.pathMaker(fs.baseDir, info, infoHash)
	err := CreateNativeZeroLengthFiles(info, dir)

	if err != nil {
		return nil, err
	}

	return &FileTorrent{
		ts:       fs,
		dir:      dir,
		info:     info,
		infoHash: infoHash,
	}, nil
}

// CreateNativeZeroLengthFiles creates natives files for any zero-length file entries in the info. This is
// a helper for file-based storages, which don't address or write to zero-
// length files because they have no corresponding pieces.
func CreateNativeZeroLengthFiles(info *meta.Info, dir string) (err error) {
	for _, fi := range info.UpvertedFiles() {
		if fi.Length != 0 {
			continue
		}
		name := filepath.Join(append([]string{dir, info.Name}, fi.Path...)...)
		os.MkdirAll(filepath.Dir(name), 0750)

		f, err := os.Create(name)
		defer f.Close()

		if err != nil {
			log.Errorf("cannot create a file %s: %s", name, err)
			break
		}
		err = f.Truncate(info.Length)
		if err != nil {
			log.Errorf("cannot truncate a file %s: %s", name, err)
			break
		}
	}
	return
}

// Returns EOF on short or missing file.
func (ft *FileTorrent) readFileAt(fi meta.FileInfo, b []byte, off int64) (n int, err error) {
	f, err := os.Open(ft.fileInfoName(fi))
	if os.IsNotExist(err) {
		// File missing is treated the same as a short file.
		err = io.EOF
		return
	}
	if err != nil {
		return
	}
	defer f.Close()
	// Limit the read to within the expected bounds of this file.
	if int64(len(b)) > fi.Length-off {
		b = b[:fi.Length-off]
	}
	for off < fi.Length && len(b) != 0 {
		n1, err1 := f.ReadAt(b, off)
		b = b[n1:]
		n += n1
		off += int64(n1)
		if n1 == 0 {
			err = err1
			break
		}
	}
	return
}

// ReadAt read bytes at a offset, it only returns EOF at the end of the torrent. Premature EOF is ErrUnexpectedEOF.
func (ft *FileTorrent) ReadAt(b []byte, off int64) (n int, err error) {
	for _, fi := range ft.info.UpvertedFiles() {
		for off < fi.Length {
			n1, err1 := ft.readFileAt(fi, b, off)
			n += n1
			off += int64(n1)
			b = b[n1:]
			if len(b) == 0 {
				// Got what we need.
				return
			}
			if n1 != 0 {
				// Made progress.
				continue
			}
			err = err1
			if err == io.EOF {
				// Lies.
				err = io.ErrUnexpectedEOF
			}
			return
		}
		off -= fi.Length
	}
	err = io.EOF
	return
}

// WriteAt writes bytes to a torrent file at a offset
func (ft *FileTorrent) WriteAt(p []byte, off int64) (n int, err error) {
	for _, fi := range ft.info.UpvertedFiles() {
		if off >= fi.Length {
			off -= fi.Length
			continue
		}
		n1 := len(p)
		if int64(n1) > fi.Length-off {
			n1 = int(fi.Length - off)
		}
		name := ft.fileInfoName(fi)
		os.MkdirAll(filepath.Dir(name), 0770)
		var f *os.File
		f, err = os.OpenFile(name, os.O_WRONLY|os.O_CREATE, 0660)
		if err != nil {
			return
		}
		n1, err = f.WriteAt(p[:n1], off)
		f.Close()
		if err != nil {
			log.Errorf("file write error %d", off)
			return
		}
		log.Infof("file write at %d, value: %s", off, string(p[:n1]))
		n += n1
		off = 0
		p = p[n1:]
		if len(p) == 0 {
			break
		}
	}
	return
}

func (ft *FileTorrent) fileInfoName(fi meta.FileInfo) string {
	return filepath.Join(append([]string{ft.dir, ft.info.Name}, fi.Path...)...)
}
