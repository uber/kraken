package store

import "code.uber.internal/infra/kraken/client/store/base"

// agentFileState implements FileState for managing kraken agent files on local disk.
// It's used by both peer and origin.
type agentFileState int

const (
	// StateUpload indicates File is being uploaded through docker registry API.
	// Upload files are temp files with unique names.
	// It's possible to have multiple uploads going on for the same file, and consolidation happens
	// when the file is moved to cache.
	StateUpload agentFileState = iota
	// StateDownload indicates File is being downloaded through torrent.
	StateDownload
	// StateCache indicates File has been downloaded through torrent or uploaded by docker push.
	StateCache
)

var _directoryLookup = make(map[base.FileState]string)

func registerFileState(s base.FileState, d string) {
	_directoryLookup[s] = d
}

// GetDirectory returns corresponding directory of a state.
func (state agentFileState) GetDirectory() string {
	return _directoryLookup[state]
}
