package store

// FileState decides what directory a file is in.
// A file can only be in one state at any given time.
type FileState interface {
	GetDirectory() string
}

// LocalFileState implements FileState for files on local disk.
type localFileState int

const (
	stateDownload localFileState = iota // File is being downloaded
	stateCache                          // File has been downloaded
	stateTrash                          // File ready to be removed
)

func (state localFileState) GetDirectory() string { return _localFileStateLookup.getDirectory(state) }

var _localFileStateLookup = localFileStateLookup{}

// LocalFileStateLookup provides utility functions to lookup mapping between states and directories.
type localFileStateLookup struct {
	stateLookup     map[string]FileState
	directoryLookup map[FileState]string
}

func (lookup localFileStateLookup) register(state FileState, directory string) {
	lookup.stateLookup[directory] = state
	lookup.directoryLookup[state] = directory
}

func (lookup localFileStateLookup) getDirectory(state FileState) string {
	return lookup.directoryLookup[state]
}

func (lookup localFileStateLookup) getDirectories(state FileState) []string {
	directories := []string{}
	for d := range lookup.stateLookup {
		directories = append(directories, d)
	}
	return directories
}

func (lookup localFileStateLookup) getState(directory string) FileState {
	return lookup.stateLookup[directory]
}

func (lookup localFileStateLookup) getStates(directory string) []FileState {
	states := []FileState{}
	for s := range lookup.directoryLookup {
		states = append(states, s)
	}
	return states
}
