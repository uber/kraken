package meta

import (
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"code.uber.internal/go-common.git/x/log"

	"github.com/anacrolix/missinggo/slices"
)

// Info is a torrent info dictionary.
type Info struct {
	PieceLength int64  `bencode:"piece length"`
	Pieces      []byte `bencode:"pieces"`
	Name        string `bencode:"name"`
	Length      int64  `bencode:"length,omitempty"`
	Private     *bool  `bencode:"private,omitempty"`
	// TODO: Document this field.
	Source string     `bencode:"source,omitempty"`
	Files  []FileInfo `bencode:"files,omitempty"`
}

// FileInfo is specific to a single file inside the MetaInfo structure.
type FileInfo struct {
	Length int64    `bencode:"length"`
	Path   []string `bencode:"path"`
}

// DisplayPath returns a file name or info name for a torrent's meta info
func (fi *FileInfo) DisplayPath(info *Info) string {
	if info.IsDir() {
		return strings.Join(fi.Path, "/")
	}
	return info.Name
}

// BuildFromFilePath is a helper that sets Files and Pieces from a root path and its
// children.
func (info *Info) BuildFromFilePath(root string) (err error) {
	info.Name = filepath.Base(root)
	info.Files = nil
	err = filepath.Walk(root, func(path string, fi os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if fi.IsDir() {
			// Directories are implicit in torrent files.
			return nil
		} else if path == root {
			// The root is a file.
			info.Length = fi.Size()
			return nil
		}
		relPath, err := filepath.Rel(root, path)
		if err != nil {
			return fmt.Errorf("error getting relative path: %s", err)
		}
		info.Files = append(info.Files, FileInfo{
			Path:   strings.Split(relPath, string(filepath.Separator)),
			Length: fi.Size(),
		})
		return nil
	})
	if err != nil {
		return
	}
	slices.Sort(info.Files, func(l, r FileInfo) bool {
		return strings.Join(l.Path, "/") < strings.Join(r.Path, "/")
	})
	err = info.GeneratePieces(func(fi FileInfo) (io.ReadCloser, error) {
		return os.Open(filepath.Join(root, strings.Join(fi.Path, string(filepath.Separator))))
	})
	if err != nil {
		err = fmt.Errorf("error generating pieces: %s", err)
	}
	return
}

func (info *Info) writeFiles(w io.Writer, open func(fi FileInfo) (io.ReadCloser, error)) error {
	for _, fi := range info.UpvertedFiles() {
		r, err := open(fi)
		if err != nil {
			return fmt.Errorf("error opening %v: %s", fi, err)
		}
		wn, err := io.CopyN(w, r, fi.Length)
		r.Close()
		if wn != fi.Length || err != nil {
			return fmt.Errorf("error hashing %v: %s", fi, err)
		}
	}
	return nil
}

// GeneratePieces sets info.Pieces by hashing info.Files.
func (info *Info) GeneratePieces(open func(fi FileInfo) (io.ReadCloser, error)) error {
	if info.PieceLength == 0 {
		return errors.New("piece length must be non-zero")
	}
	pr, pw := io.Pipe()
	go func() {
		err := info.writeFiles(pw, open)
		pw.CloseWithError(err)
	}()
	defer pr.Close()
	var pieces []byte
	for {
		hasher := sha1.New()
		wn, err := io.CopyN(hasher, pr, info.PieceLength)
		if err == io.EOF {
			err = nil
		}
		if err != nil {
			return err
		}
		if wn == 0 {
			break
		}
		pieces = hasher.Sum(pieces)
		if wn < info.PieceLength {
			break
		}
	}
	info.Pieces = pieces
	log.Infof("generated pieces hash: %s", hex.EncodeToString(pieces))
	return nil
}

// TotalLength returns a total length of all torrent files
func (info *Info) TotalLength() (ret int64) {
	if info.IsDir() {
		for _, fi := range info.Files {
			ret += fi.Length
		}
	} else {
		ret = info.Length
	}
	return
}

// NumPieces return number of pieces in a torrent
func (info *Info) NumPieces() int {
	if len(info.Pieces)%20 != 0 {
		panic(len(info.Pieces))
	}
	return len(info.Pieces) / 20
}

// IsDir returns if torrent file is a dir
func (info *Info) IsDir() bool {
	return len(info.Files) != 0
}

// UpvertedFiles is the files field, converted up from the old single-file in the parent info
// dict if necessary. This is a helper to avoid having to conditionally handle
// single and multi-file torrent infos.
func (info *Info) UpvertedFiles() []FileInfo {
	if len(info.Files) == 0 {
		return []FileInfo{{
			Length: info.Length,
			// Callers should determine that Info.Name is the basename, and
			// thus a regular file.
			Path: nil,
		}}
	}
	return info.Files
}
