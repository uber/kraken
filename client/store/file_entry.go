package store

import (
	"io"
	"os"
)

// FileState decides what directory a file is in.
// A file can only be in one state at any given time.
type FileState interface {
	GetDirectory() string
}

// FileReader provides read operation on a file.
type FileReader interface {
	io.Reader
	io.ReaderAt
	io.Seeker
	io.Closer
}

// FileReadWriter provides read/write operation on a file.
type FileReadWriter interface {
	FileReader
	io.Writer
	io.WriterAt

	Size() int64   // required by docker registry.
	Cancel() error // required by docker registry.
	Commit() error // required by docker registry.
}

// MetadataType is an interface that controls operations on metadata files.
// Structs that implements MetadataType must make itself comparable, and have lookup logic defined
// in getMetadataType()
type MetadataType interface {
	Suffix() string
	IsValidState(state FileState) bool
}

// FileEntryBase manages one file and its metadata.
type FileEntryBase interface {
	GetName(states []FileState) (string, error)
	GetPath(states []FileState) (string, error)
	GetState(states []FileState) (FileState, error)
	SetState(states []FileState, state FileState) error
	IsOpen(states []FileState) (bool, error)
	Stat(states []FileState) (os.FileInfo, error)

	Create(states []FileState, targetState FileState, len int64, callback BackendCallback) (bool, error)
	CreateLinkFrom(states []FileState, targetState FileState, sourcePath string, callback BackendCallback) (bool, error)
	Delete(states []FileState, callback BackendCallback) error
	LinkTo(states []FileState, targetPath string) error
	Move(states []FileState, targetState FileState) error

	GetReader(states []FileState) (FileReader, error)
	GetReadWriter(states []FileState) (FileReadWriter, error)

	AddMetadata(states []FileState, mt MetadataType) error
	ReadMetadata(states []FileState, mt MetadataType) ([]byte, error)
	WriteMetadata(states []FileState, mt MetadataType, data []byte) (bool, error)
	ReadMetadataAt(states []FileState, mt MetadataType, b []byte, off int64) (int, error)
	WriteMetadataAt(states []FileState, mt MetadataType, b []byte, off int64) (int, error)
	DeleteMetadata(states []FileState, mt MetadataType) error
	ListMetadata(states []FileState) ([]MetadataType, error)
}

// FileEntry provides basic read/write operation to a file and its metadata, and keeps track of its ref count.
type FileEntry interface {
	FileEntryBase

	GetRefCount(states []FileState) (int64, error)
	IncrementRefCount(states []FileState) (int64, error)
	DecrementRefCount(states []FileState) (int64, error)
}
