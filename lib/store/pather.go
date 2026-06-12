package store

import "path/filepath"

const (
	// _defaultShardIDLength is the number of bytes of file digest to be used for shard ID.
	// For every byte (2 HEX char), one more level of directories will be created.
	_defaultShardIDLength = 2
	_incompleteSubDir     = "incomplete"
	_completeSubDir       = "complete"
	_blobFileName         = "data"
)

type pather struct {
	dir string
}

func newPather(rootDir string) *pather {
	return &pather{dir: rootDir}
}

func (p *pather) blobPath(key string, complete bool) string {
	dirName := p.dirPath(key, complete)
	return filepath.Join(dirName, _blobFileName)
}

func (p *pather) dirPath(key string, complete bool) string {
	// TODO - allow config to specify whether to shard or not. Allow no sharding, so we can replace [SimpleStore].
	subDirName := _incompleteSubDir
	if complete {
		subDirName = _completeSubDir
	}
	dirPath := filepath.Join(p.dir, subDirName)
	for i := 0; i < int(_defaultShardIDLength) && i < len(key)/2; i++ {
		// (1 byte = 2 char of file name assumming file name is in HEX)
		dirName := key[i*2 : i*2+2]
		dirPath = filepath.Join(dirPath, dirName)
	}

	return filepath.Join(dirPath, key)
}

func (p *pather) sidecarFilePath(key string, complete bool, sidecarFilePath string) string {
	dirPath := p.dirPath(key, complete)
	return filepath.Join(dirPath, sidecarFilePath)
}
