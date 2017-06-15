package store

// localFileState implements FileState for files on local disk.
type localFileState int

const (
	// File is being uploaded through docker registry API
	// upload files serve like temperary files when
	// we can have multiple threads write to multiple upload files,
	// then move to cache with the same name.
	// this supports concurrency better than downloads.
	stateUpload localFileState = iota
	// File is being downloaded through torrent
	// TODO (@evelynl): currently download files are only used for torrent
	// there should only be one download file per content (layer)
	// because remote peers read the same download file so it is pointless to create multiple tmp files
	// however we do not know when it is moved / or about to be moved to cache
	// one solution is to have a goal state for download files
	stateDownload
	// File has been downloaded through torrent
	stateCache
)

var _directoryLookup = make(map[FileState]string)

func registerFileState(s FileState, d string) {
	_directoryLookup[s] = d
}

func (state localFileState) GetDirectory() string {
	return _directoryLookup[state]
}
