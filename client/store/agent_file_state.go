package store

import "code.uber.internal/infra/kraken/client/store/base"

var _directoryLookup = make(map[base.FileState]string)

func registerFileState(s base.FileState, d string) {
	_directoryLookup[s] = d
}

// agentFileState implements FileState for managing kraken agent files on local disk.
// It's used by both peer and origin.
type agentFileState string

// GetDirectory returns corresponding directory of a state.
func (state agentFileState) GetDirectory() string {
	return _directoryLookup[state]
}
