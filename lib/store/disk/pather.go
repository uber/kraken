package disk

import (
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"

	"go.uber.org/zap"
)

const (
	_incompleteSubDir = "incomplete"
	_completeSubDir   = "complete"
	_blobFileName     = "data"
)

type pather struct {
	dir         string
	shardLength int
	log         *zap.SugaredLogger
}

// shardLength is the number of bytes of blob key to be used for shard ID.
// For every byte (2 HEX char), one more level of directories will be created.
// A shardLength of 0 denotes no sharding.
func newPather(rootDir string, shardLength int, log *zap.SugaredLogger) *pather {
	return &pather{dir: rootDir, shardLength: shardLength, log: log}
}

func (p *pather) blobPath(key string, complete bool) string {
	dirName := p.dirPath(key, complete)
	return filepath.Join(dirName, _blobFileName)
}

func (p *pather) dirPath(key string, complete bool) string {
	subDirName := _incompleteSubDir
	if complete {
		subDirName = _completeSubDir
	}
	dirPath := filepath.Join(p.dir, subDirName)
	for i := 0; i < int(p.shardLength) && i < len(key)/2; i++ {
		// (1 byte = 2 char of file name assuming file name is in HEX)
		dirName := key[i*2 : i*2+2]
		dirPath = filepath.Join(dirPath, dirName)
	}

	return filepath.Join(dirPath, key)
}

func (p *pather) sidecarFilePath(key string, complete bool, sidecarFileName string) string {
	dirPath := p.dirPath(key, complete)
	return filepath.Join(dirPath, sidecarFileName)
}

func (p *pather) rebootKeys(complete bool) ([]string, error) {
	subDirName := _incompleteSubDir
	if complete {
		subDirName = _completeSubDir
	}
	dir := filepath.Join(p.dir, subDirName)
	keys := make([]string, 0)
	ok, err := exists(dir)
	if err != nil {
		return nil, fmt.Errorf("exists: %w", err)
	}
	if !ok {
		return []string{}, nil
	}
	numShards := p.shardLength
	err = filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || entry.Name() != _blobFileName {
			return nil
		}
		// We can't just take the parent dir's name as the key, since some keys
		// might have "/" as a part of their names, causing their names to be split over several directories.
		relPath, err := filepath.Rel(dir, filepath.Dir(path))
		if err != nil {
			return err
		}
		nameParts := strings.Split(relPath, string(filepath.Separator))
		if len(nameParts) < numShards {
			p.log.With("path", path).Error(
				"invariant violation - cannot reboot blob, as its path is shallower than the configured shard length - failing open by not rebooting it")
			return nil
		}
		key := strings.Join(nameParts[numShards:], string(filepath.Separator))
		keys = append(keys, key)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walk through dir '%v' to reboot blob keys: %w", dir, err)
	}
	return keys, nil
}
