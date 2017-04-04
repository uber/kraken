package store

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
)

const (
	pieceClean    = uint8(0)
	pieceDirty    = uint8(1)
	pieceDone     = uint8(2)
	pieceDontCare = uint8(3)
)

// MetadataType is an interface that controls operations on metadata files
type MetadataType interface {
	Set(filepath string, content []byte) (bool, error)
	Get(filepath string) ([]byte, error)
	Delete(filepath string) error
}

type pieceStatus struct {
	index     int
	numPieces int
}

func getPieceStatus(index int, numPieces int) MetadataType {
	return &pieceStatus{
		index:     index,
		numPieces: numPieces,
	}
}

// init initilizes pieceStatue of all pieces as clean
func (p *pieceStatus) init(filepath string) error {
	fp := p.path(filepath)
	if _, err := os.Stat(fp); !os.IsNotExist(err) {
		return nil
	}

	data := make([]byte, p.numPieces)
	for i := 0; i < p.numPieces; i++ {
		data[i] = pieceClean
	}

	return ioutil.WriteFile(fp, data, 0755)
}

func (p *pieceStatus) path(filepath string) string {
	return filepath + "_status"
}

// Set updates pieceStatus and returns true only if the file is updated correctly
// returns false if error or file is already updated with desired content
func (p *pieceStatus) Set(filepath string, content []byte) (bool, error) {
	fp := p.path(filepath)
	if err := p.init(filepath); err != nil {
		return false, err
	}

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

// Get returns pieceStatus content as a byte array.
func (p *pieceStatus) Get(filepath string) ([]byte, error) {
	fp := p.path(filepath)

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

// Delete deletes pieceStatus of the filepath, i.e. deletes all statuses.
func (p *pieceStatus) Delete(filepath string) error {
	fp := p.path(filepath)

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

func (s *startedAt) path(filepath string) string {
	return filepath + "_startedat"
}

// Set updates startedAt and returns true only if the file is updated correctly
// returns false if error or file is already updated with desired content
func (s *startedAt) Set(filepath string, content []byte) (bool, error) {
	fp := s.path(filepath)

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

	if compareMetadata(data, content) {
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
func (s *startedAt) Get(filepath string) ([]byte, error) {
	fp := s.path(filepath)

	// check existence
	if _, err := os.Stat(fp); err != nil {
		return nil, err
	}

	return ioutil.ReadFile(fp)
}

// Delete deletes startedAt of the filepath.
func (s *startedAt) Delete(filepath string) error {
	fp := s.path(filepath)

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

func (h *hashState) path(filepath string) string {
	dir := filepath + "_hashstates/"
	return fmt.Sprintf("%s%s_%s", dir, h.alg, h.code)
}

// Set updates hashState and returns true only if the file is updated correctly
// returns false if error or file is already updated with desired content
func (h *hashState) Set(filepath string, content []byte) (bool, error) {
	fp := h.path(filepath)

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

	if compareMetadata(data, content) {
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
func (h *hashState) Get(filepath string) ([]byte, error) {
	fp := h.path(filepath)

	// check existence
	if _, err := os.Stat(fp); err != nil {
		return nil, err
	}

	return ioutil.ReadFile(fp)
}

// Delete deletes hashState of the filepath.
func (h *hashState) Delete(filepath string) error {
	fp := h.path(filepath)

	err := os.RemoveAll(fp)
	if err != nil {
		return err
	}
	return nil
}

func compareMetadata(d1 []byte, d2 []byte) bool {
	if len(d1) != len(d2) {
		return false
	}

	n := len(d1)
	for i := 0; i < n; i++ {
		if d1[i] != d2[i] {
			return false
		}
	}

	return true
}
