package storagedriver

import (
	"os"
	"path/filepath"
	"strings"

	"fmt"
	"io/ioutil"
)

// HashStates ..
type HashStates struct{}

// NewHashStates creates a new Hashstates
func NewHashStates() *HashStates {
	return &HashStates{}
}

func (h *HashStates) putHashState(dir, uuid, alg, code string, content []byte) (int, error) {
	hashdir := fmt.Sprintf("%s%s_hashstates/", dir, uuid)
	err := os.MkdirAll(hashdir, 0755)
	if err != nil {
		return -1, err
	}

	fp := fmt.Sprintf("%s%s_%s", hashdir, alg, code)
	f, err := os.Create(fp)
	if err != nil {
		return -1, err
	}
	defer f.Close()

	n, err := f.Write(content)
	return n, err
}

func (h *HashStates) getHashState(dir, uuid, alg, code string) ([]byte, error) {
	hashdir := fmt.Sprintf("%s%s_hashstates/", dir, uuid)
	fp := fmt.Sprintf("%s%s_%s", hashdir, alg, code)
	return ioutil.ReadFile(fp)
}

func (h *HashStates) getAlgAndCodeFromStateFile(base string) (string, string, error) {
	st := strings.Split(base, "_")
	if len(st) < 2 {
		return "", "", fmt.Errorf("Error getting algorithm and code from state file: %s", base)
	}
	return st[0], st[1], nil
}

func (h *HashStates) listHashStates(dir, uuid, path string) ([]string, error) {
	hashdir := fmt.Sprintf("%s%s_hashstates/", dir, uuid)
	states, err := ioutil.ReadDir(hashdir)
	if err != nil {
		return nil, err
	}

	st := strings.Split(path, uuid)
	if len(st) < 2 {
		return nil, fmt.Errorf("Error getting hash states. Invalid path: %s", path)
	}
	pathprefix := st[0]
	var ret []string
	for _, s := range states {
		alg, code, err := h.getAlgAndCodeFromStateFile(filepath.Base(s.Name()))
		if err != nil {
			return nil, err
		}
		state := pathprefix + uuid + "/hashstates/" + alg + "/" + code
		ret = append(ret, state)
	}
	return ret, nil
}
