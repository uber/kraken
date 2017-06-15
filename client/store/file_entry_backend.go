package store

import "os"

// FileStoreBackend manages all files.
type FileStoreBackend interface {
	CreateFile(fileName string, states []FileState, createState FileState, len int64) (bool, error)
	CreateLinkFromFile(fileName string, states []FileState, createState FileState, sourcePath string) (bool, error)
	LinkToFile(fileName string, states []FileState, targetPath string) error
	MoveFile(fileName string, states []FileState, goalState FileState) error
	DeleteFile(fileName string, states []FileState) error

	GetFilePath(fileName string, states []FileState) (string, error)
	GetFileStat(fileName string, states []FileState) (os.FileInfo, error)

	ReadFileMetadata(fileName string, states []FileState, mt MetadataType) ([]byte, error)
	WriteFileMetadata(fileName string, states []FileState, mt MetadataType, data []byte) (bool, error)
	ReadFileMetadataAt(fileName string, states []FileState, mt MetadataType, b []byte, off int64) (int, error)
	WriteFileMetadataAt(fileName string, states []FileState, mt MetadataType, b []byte, off int64) (int, error)
	DeleteFileMetadata(fileName string, states []FileState, mt MetadataType) error
	ListFileMetadata(fileName string, states []FileState) ([]MetadataType, error)

	GetFileReader(fileName string, states []FileState) (FileReader, error)
	GetFileReadWriter(fileName string, states []FileState) (FileReadWriter, error)

	IncrementFileRefCount(fileName string, states []FileState) (int64, error)
	DecrementFileRefCount(fileName string, states []FileState) (int64, error)
}

// BackendCallback defines callback function to be passed into FileEntry.
type BackendCallback func() (FileEntry, error)
