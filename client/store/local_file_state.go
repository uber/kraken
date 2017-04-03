package store

// FileState decides what directory a file is in.
// A file can only be in one state at any given time.
type FileState interface {
	GetDirectory() string
}

// localFileState implements FileState for files on local disk.
type localFileState int

const (
	stateUpload   localFileState = iota // File is being uploaded through docker registry API
	stateDownload                       // File is being downloaded through torrent
	stateCache                          // File has been downloaded through torrent
	stateTrash                          // File ready to be removed
)

var _stateLookup = make(map[string]FileState)
var _directoryLookup = make(map[FileState]string)

func registerFileState(s FileState, d string) {
	_stateLookup[d] = s
	_directoryLookup[s] = d
}

func (state localFileState) GetDirectory() string {
	return _directoryLookup[state]
}

// FileStateError represents errors related to file state.
type FileStateError struct {
	Op    string
	State FileState
	Name  string
	Msg   string
}

func (e *FileStateError) Error() string {
	return e.Op + " " + e.State.GetDirectory() + "/" + e.Name + ": " + e.Msg
}

// IsFileStateError returns true if the param is of FileStateError type.
func IsFileStateError(err error) bool {
	switch err.(type) {
	default:
		return false
	case *FileStateError:
		return true
	}
}
