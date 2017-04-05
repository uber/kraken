package store

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"

	"code.uber.internal/infra/kraken/utils"
)

// const enum representing the status of a torrent's piece
const (
	PieceClean    = uint8(0)
	PieceDirty    = uint8(1)
	PieceDone     = uint8(2)
	PieceDontCare = uint8(3)
)

// MetadataType is an interface that controls operations on metadata files
type MetadataType interface {
	Set(file FileEntry, content []byte) (bool, error)
	Get(file FileEntry) ([]byte, error)
	Delete(file FileEntry) error
}

type pieceStatus struct {
	// index should be -1 (when the caller wants to set the entire chunk of statuses), or
	// 0 <= index < numPieces for individual pieces
	index     int
	numPieces int
}

func getPieceStatus(index int, numPieces int) MetadataType {
	return &pieceStatus{
		index:     index,
		numPieces: numPieces,
	}
}

func (p *pieceStatus) path(file FileEntry) string {
	return file.GetPath() + "_status"
}

// Set updates pieceStatus and returns true only if the file is updated correctly
// returns false if error or file is already updated with desired content
func (p *pieceStatus) Set(file FileEntry, content []byte) (bool, error) {
	if file.GetState() != stateDownload {
		return false, fmt.Errorf("Cannot change piece status for %s: %d. File not in download directory.", file.GetPath(), p.index)
	}

	if p.index == -1 {
		return p.setAll(file, content)
	}

	return p.set(file, content)
}

// Get returns pieceStatus content as a byte array.
func (p *pieceStatus) Get(file FileEntry) ([]byte, error) {
	if p.index == -1 {
		return p.getAll(file)
	}

	return p.get(file)
}

// setAll sets pieceStatue of all pieces
func (p *pieceStatus) setAll(file FileEntry, content []byte) (bool, error) {
	fp := p.path(file)

	if len(content) != p.numPieces {
		return false, fmt.Errorf("Failed to set piece status. Invalid content: expecting length %d but got %d.", p.numPieces, len(content))
	}

	_, err := os.Stat(fp)

	if err != nil {
		if os.IsNotExist(err) {
			return true, ioutil.WriteFile(fp, content, 0755)
		}
		return false, err
	}

	data, err := ioutil.ReadFile(fp)
	if err != nil {
		return false, err
	}

	if utils.CompareByteArray(data, content) {
		return false, nil
	}

	return true, ioutil.WriteFile(fp, content, 0755)
}

// getAll gets pieceStatue of all pieces
func (p *pieceStatus) getAll(file FileEntry) ([]byte, error) {
	if file.GetState() == stateDownload {
		fp := p.path(file)
		if _, err := os.Stat(fp); err != nil {
			return nil, err
		}

		return ioutil.ReadFile(fp)
	}

	if file.GetState() == stateCache {
		meta := make([]byte, p.numPieces)
		for i := 0; i < p.numPieces; i++ {
			meta[i] = PieceDone
		}
		return meta, nil
	}

	return nil, fmt.Errorf("Failed to get piece status for %s: %d cannot find file in download nor cache directory.", file.GetPath(), p.index)
}

// setAll sets pieceStatue of all pieces
func (p *pieceStatus) set(file FileEntry, content []byte) (bool, error) {
	fp := p.path(file)

	if len(content) != 1 {
		return false, fmt.Errorf("Invalid content: %v", content)
	}

	data, err := ioutil.ReadFile(fp)
	if err != nil {
		return false, err
	}

	if p.index < 0 || p.index >= len(data) {
		return false, fmt.Errorf("Index out of range for %s: %d", fp, p.index)
	}

	if data[p.index] == content[0] {
		return false, nil
	}

	f, err := os.OpenFile(fp, os.O_RDWR, 0755)
	if err != nil {
		return false, err
	}
	defer f.Close()

	_, err = f.WriteAt(content, int64(p.index))
	if err != nil {
		return false, err
	}
	return true, nil
}

// getAll gets pieceStatue of all pieces
func (p *pieceStatus) get(file FileEntry) ([]byte, error) {
	if file.GetState() == stateDownload {
		fp := p.path(file)

		if p.index < 0 {
			return nil, fmt.Errorf("Index out of range for %s: %d", fp, p.index)
		}

		// check existence
		if _, err := os.Stat(fp); err != nil {
			return nil, err
		}

		// read to data
		f, err := os.Open(fp)
		if err != nil {
			return nil, err
		}
		defer f.Close()

		content := make([]byte, 1)

		_, err = f.ReadAt(content, int64(p.index))
		if err != nil {
			return nil, err
		}

		return content, nil
	}

	if file.GetState() == stateCache {
		return []byte{PieceDone}, nil
	}

	return nil, fmt.Errorf("Failed to get piece status for %s: %d cannot find file in download nor cache directory.", file.GetPath(), p.index)
}

// Delete deletes pieceStatus of the filepath, i.e. deletes all statuses.
func (p *pieceStatus) Delete(file FileEntry) error {
	fp := p.path(file)

	err := os.RemoveAll(fp)
	if err != nil {
		return err
	}
	return nil
}

type startedAt struct {
}

func getStartedAt() MetadataType {
	return &startedAt{}
}

func (s *startedAt) path(file FileEntry) string {
	return file.GetPath() + "_startedat"
}

// Set updates startedAt and returns true only if the file is updated correctly
// returns false if error or file is already updated with desired content
func (s *startedAt) Set(file FileEntry, content []byte) (bool, error) {
	fp := s.path(file)

	var f *os.File
	// check existence
	_, err := os.Stat(fp)
	if err != nil && !os.IsNotExist(err) {
		return false, err
	}

	if os.IsNotExist(err) {
		err = ioutil.WriteFile(fp, content, 0755)
		if err != nil {
			return false, err
		}
		return true, nil
	}

	f, err = os.OpenFile(fp, os.O_RDWR, 0755)
	if err != nil {
		return false, err
	}
	defer f.Close()

	fs, err := f.Stat()
	if err != nil {
		return false, err
	}

	data := make([]byte, int(fs.Size()))

	_, err = f.Read(data)

	if utils.CompareByteArray(data, content) {
		return false, nil
	}

	if len(data) != len(content) {
		err = f.Truncate(int64(len(content)))
		if err != nil {
			return false, err
		}
	}

	_, err = f.Write(content)
	if err != nil {
		return false, err
	}
	return true, nil
}

// Get returns startedAt content as a byte array.
func (s *startedAt) Get(file FileEntry) ([]byte, error) {
	fp := s.path(file)

	// check existence
	if _, err := os.Stat(fp); err != nil {
		return nil, err
	}

	return ioutil.ReadFile(fp)
}

// Delete deletes startedAt of the filepath.
func (s *startedAt) Delete(file FileEntry) error {
	fp := s.path(file)

	err := os.RemoveAll(fp)
	if err != nil {
		return err
	}
	return nil
}

type hashState struct {
	alg  string
	code string
}

func getHashState(alg, code string) MetadataType {
	return &hashState{
		alg:  alg,
		code: code,
	}
}

func (h *hashState) path(file FileEntry) string {
	dir := file.GetPath() + "_hashstates/"
	return fmt.Sprintf("%s%s_%s", dir, h.alg, h.code)
}

// Set updates hashState and returns true only if the file is updated correctly
// returns false if error or file is already updated with desired content
func (h *hashState) Set(file FileEntry, content []byte) (bool, error) {
	fp := h.path(file)

	var f *os.File
	// check existence
	_, err := os.Stat(fp)
	if err != nil && !os.IsNotExist(err) {
		return false, err
	}

	if os.IsNotExist(err) {
		err = os.MkdirAll(path.Dir(fp), 0755)
		if err != nil {
			return false, err
		}

		err = ioutil.WriteFile(fp, content, 0755)
		if err != nil {
			return false, err
		}
		return true, nil
	}

	f, err = os.OpenFile(fp, os.O_RDWR, 0755)
	if err != nil {
		return false, err
	}
	defer f.Close()

	fs, err := f.Stat()
	if err != nil {
		return false, err
	}

	data := make([]byte, int(fs.Size()))

	_, err = f.Read(data)

	if utils.CompareByteArray(data, content) {
		return false, nil
	}

	if len(data) != len(content) {
		err = f.Truncate(int64(len(content)))
		if err != nil {
			return false, err
		}
	}

	_, err = f.Write(content)
	if err != nil {
		return false, err
	}
	return true, nil
}

// Get returns hashState content as a byte array.
func (h *hashState) Get(file FileEntry) ([]byte, error) {
	fp := h.path(file)

	// check existence
	if _, err := os.Stat(fp); err != nil {
		return nil, err
	}

	return ioutil.ReadFile(fp)
}

// Delete deletes hashState of the filepath.
func (h *hashState) Delete(file FileEntry) error {
	fp := h.path(file)

	err := os.RemoveAll(fp)
	if err != nil {
		return err
	}
	return nil
}
