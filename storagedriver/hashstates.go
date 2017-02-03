package storagedriver

import (
	"bufio"
	"io"
	"os"
	"strings"

	"fmt"
)

// HashStates ..
type HashStates struct{}

// NewHashStates creates a new Hashstates
func NewHashStates() *HashStates {
	return &HashStates{}
}

func (h *HashStates) putHashStates(dir, uuid, path string, content []byte) (int, error) {
	f, err := os.OpenFile(dir+uuid+"_startedat", os.O_APPEND|os.O_WRONLY, os.ModeAppend)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	_, err = f.WriteString(path + " ")
	if err != nil {
		return 0, err
	}
	n, err := f.Write(append(content, '\n'))
	return n, err
}

func (h *HashStates) getHashStates(dir, uuid string) (map[string]string, error) {
	fp := dir + uuid + "_startedat"
	f, err := os.Open(fp)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	reader := bufio.NewReader(f)
	// skip timestamp
	_, _, err = reader.ReadLine()
	if err != nil {
		return nil, err
	}

	m := make(map[string]string)

	for {
		state, _, err := reader.ReadLine()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		hashstate := strings.SplitN(string(state), " ", 2)
		if len(hashstate) < 2 {
			return nil, fmt.Errorf("Erroring getting hashstate %s", state)
		}
		m[hashstate[0]] = hashstate[1]
	}
	return m, nil
}

func (h *HashStates) listHashStates(dir, uuid string) ([]string, error) {
	m, err := h.getHashStates(dir, uuid)
	if err != nil {
		return nil, err
	}

	var s []string
	for key := range m {
		s = append(s, key)
	}
	return s, nil
}
