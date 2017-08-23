package store

// agentFileState implements FileState for managing files on local disk.
type agentFileState struct {
	directory string
}

// GetDirectory returns corresponding directory of a state.
func (state agentFileState) GetDirectory() string {
	return state.directory
}
