package storage

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"

	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/infra/kraken/client/torrent/bencode"
	"code.uber.internal/infra/kraken/torlib"
)

const infoFileSuffix = "_info"
const perm = 0755

// FileTorrentStorage defines a file-based storage for torrents, that isn't yet bound to a particular
// torrent.
type FileTorrentStorage struct {
	baseDir   string
	pathMaker func(baseDir string, info *torlib.Info, infoHash torlib.InfoHash) string
}

// FileTorrent is an actual torrent's file content interface
type FileTorrent struct {
	ts       *FileTorrentStorage
	dir      string
	info     *torlib.Info
	infoHash torlib.InfoHash
}

// PathMaker creates a file name and path for a torrent
type PathMaker func(baseDir string, info *torlib.Info, infoHash torlib.InfoHash) string

// The Default path maker just returns the current path
func defaultPathMaker(baseDir string, info *torlib.Info, infoHash torlib.InfoHash) string {
	return baseDir
}

func infoHashPathMaker(baseDir string, info *torlib.Info, infoHash torlib.InfoHash) string {
	return filepath.Join(baseDir, infoHash.HexString())
}

// NewFileStorage creates a new file based storage for a torrent
func NewFileStorage(baseDir string) TorrentManager {
	return NewFileWithCustomPathMaker(baseDir, infoHashPathMaker)
}

// NewFileWithCustomPathMaker allows passing a function to determine the path for storing torrent data
func NewFileWithCustomPathMaker(baseDir string, pathMaker PathMaker) TorrentManager {
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

// CreateTorrent opens a new torrent and returns read/write interface to it
func (fs *FileTorrentStorage) CreateTorrent(infoHash torlib.InfoHash, infoBytes []byte) (Torrent, error) {
	h := torlib.NewInfoHashFromBytes(infoBytes)
	if h != infoHash {
		return nil, fmt.Errorf("Invalid info hash")
	}

	info := new(torlib.Info)
	if err := bencode.Unmarshal(infoBytes, info); err != nil {
		return nil, err
	}

	dir := fs.pathMaker(fs.baseDir, info, infoHash)
	err := os.MkdirAll(dir, perm)
	if err != nil {
		return nil, err
	}

	err = CreateNativeZeroLengthFiles(info, dir)
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

// OpenTorrent opens an existing torrent and returns read/write interface to it
func (fs *FileTorrentStorage) OpenTorrent(infoHash torlib.InfoHash) (Torrent, []byte, error) {
	dir := fs.pathMaker(fs.baseDir, nil, infoHash)
	info := new(torlib.Info)
	infoBytes, err := ioutil.ReadFile(getInfoFilePath(dir))
	if err != nil {
		return nil, nil, err
	}
	if err := bencode.Unmarshal(infoBytes, info); err != nil {
		return nil, nil, err
	}

	return &FileTorrent{
		ts:       fs,
		dir:      dir,
		info:     info,
		infoHash: infoHash,
	}, infoBytes, nil
}

// CreateNativeZeroLengthFiles creates natives files for any zero-length file entries in the info. This is
// a helper for file-based storages, which don't address or write to zero-
// length files because they have no corresponding pieces.
func CreateNativeZeroLengthFiles(info *torlib.Info, dir string) (err error) {

	name := filepath.Join(dir, info.Name)
	os.MkdirAll(filepath.Dir(name), 0750)

	f, err := os.Create(name)
	if err != nil {
		log.Errorf("cannot create a file %s: %s", name, err)
		return
	}
	defer f.Close()

	err = f.Truncate(info.Length)
	if err != nil {
		log.Errorf("cannot truncate a file %s: %s", name, err)
		return
	}

	// Save info along with torrent
	// so peers do not need to query the tracker for complete info
	infoFile := getInfoFilePath(dir)
	f, err = os.Create(infoFile)
	if err != nil {
		log.Errorf("cannot create file %s: %s", infoFile, err)
		return
	}
	defer f.Close()
	bencodedInfoBytes, err := bencode.Marshal(info)
	if err != nil {
		log.Errorf("cannot write file %s: %s", infoFile, err)
		return
	}
	_, err = f.Write(bencodedInfoBytes)
	if err != nil {
		log.Errorf("cannot write file %s: %s", infoFile, err)
		return
	}
	return
}

func getInfoFilePath(torrentPath string) string {
	return fmt.Sprintf("%s%s", torrentPath, infoFileSuffix)
}

// ReadAt read bytes at a offset, it only returns EOF at the end of the torrent. Premature EOF is ErrUnexpectedEOF.
func (ft *FileTorrent) ReadAt(b []byte, off int64) (n int, err error) {
	f, err := os.Open(ft.getFilePath())
	if err != nil {
		return 0, err
	}
	defer f.Close()

	return f.ReadAt(b, off)
}

// WriteAt writes bytes to a torrent file at a offset
func (ft *FileTorrent) WriteAt(p []byte, off int64) (n int, err error) {
	f, err := os.OpenFile(ft.getFilePath(), os.O_CREATE|os.O_RDWR, 0755)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	return f.WriteAt(p, off)
}

func (ft *FileTorrent) getFilePath() string {
	return path.Join(ft.dir, ft.info.Name)
}
