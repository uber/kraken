package store

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
)

// metadataType is an int that indicates the type of metadata
type metadataType int

const (
	// pieceStatus is the piece status of a downloading torrent
	pieceStatus metadataType = iota
	// startedAt is the timestamp that an upload is initiated
	startedAt
	// hashStates are the hash states of an updated upload
	// TODO (@evelynl): dont use diretory for storing hashstates
	// additional arguments to create hashStates for docker registry are
	// hash algorithem and a code indicating the state
	hashStates
)

// path returs the path of the metadata file
func (mt metadataType) path(filepath string, args ...interface{}) (string, error) {
	switch mt {
	case pieceStatus:
		return filepath + "-status", nil
	case startedAt:
		return filepath + "_startedat", nil
	case hashStates:
		if len(args) < 2 {
			return "", fmt.Errorf("Missing arguments for getting hashstates path")
		}
		dir := filepath + "_hashstates/"
		return fmt.Sprintf("%s%s_%s", dir, args[0].(string), args[1].(string)), nil
	default:
	}
	return "", fmt.Errorf("Unsupported metadata file type: %d", mt)
}

// set creates and writes content into the metadatafile, if file exists, overwrites
// this is not thread safe
func (mt metadataType) set(filepath string, content []byte, args ...interface{}) error {
	p, err := mt.path(filepath, args...)
	if err != nil {
		return err
	}

	if mt == hashStates {
		err = os.Mkdir(path.Dir(p), 0755)
		if err != nil {
			return err
		}
	}

	err = ioutil.WriteFile(p, content, 0755)
	if err != nil {
		return err
	}
	return nil
}

// get returns metadata content
func (mt metadataType) get(filepath string, content []byte, args ...interface{}) error {
	p, err := mt.path(filepath, args...)
	if err != nil {
		return err
	}

	// check existence
	if _, err = os.Stat(p); err != nil {
		return err
	}

	// read to data
	f, err := os.Open(p)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = f.Read(content)
	if err != nil {
		return err
	}

	return nil
}

// delete deletes metadata file
// this is not thread safe
func (mt metadataType) delete(filepath string, args ...interface{}) error {
	p, err := mt.path(filepath, args...)
	if err != nil {
		return err
	}

	err = os.RemoveAll(p)
	if err != nil {
		return err
	}
	return nil
}
