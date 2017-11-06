package base

import (
	"bytes"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
)

var _ FileEntryInternalFactory = (*LocalFileEntryInternalFactory)(nil)
var _ FileEntryInternalFactory = (*CASFileEntryInternalFactory)(nil)

// LocalFileEntryInternalFactory initializes LocalFileEntryInternal obj.
type LocalFileEntryInternalFactory struct{}

// Create initializes and returns a FileEntryInternal object.
func (f *LocalFileEntryInternalFactory) Create(dir, name string) FileEntryInternal {
	return &LocalFileEntryInternal{
		dir:          dir,
		name:         name,
		relativePath: f.GetRelativePath(name),
		metadataSet:  make(map[MetadataType]struct{}),
	}
}

// GetRelativePath returns name because file entries are stored flat under state directory.
func (f *LocalFileEntryInternalFactory) GetRelativePath(name string) string {
	return path.Join(name, DefaultDataFileName)
}

// CASFileEntryInternalFactory initializes LocalFileEntryInternal obj.
// It uses the first few bytes of file digest (which is also used as file name) as shard ID.
// For every byte, one more level of directories will be created.
type CASFileEntryInternalFactory struct{}

// Create initializes and returns a FileEntryInternal object.
func (f *CASFileEntryInternalFactory) Create(dir, name string) FileEntryInternal {
	return &LocalFileEntryInternal{
		dir:          dir,
		name:         name,
		relativePath: f.GetRelativePath(name),
		metadataSet:  make(map[MetadataType]struct{}),
	}
}

// GetRelativePath returns content-addressable file path under state directory.
// Example:
// name = 07123e1f482356c415f684407a3b8723e10b2cbbc0b8fcd6282c49d37c9c1abc
// shardIDLength = 2
// relative path = 07/12/07123e1f482356c415f684407a3b8723e10b2cbbc0b8fcd6282c49d37c9c1abc
func (f *CASFileEntryInternalFactory) GetRelativePath(name string) string {
	filePath := ""
	for i := 0; i < int(DefaultShardIDLength) && i < len(name)/2; i++ {
		// (1 byte = 2 char of file name assumming file name is in HEX)
		dirName := name[i*2 : i*2+2]
		filePath = path.Join(filePath, dirName)
	}

	return path.Join(filePath, name, DefaultDataFileName)
}

// LocalFileEntryInternal implements FileEntryInternal interface, handles IO operations for one file on local disk.
type LocalFileEntryInternal struct {
	dir          string
	name         string
	relativePath string
	metadataSet  map[MetadataType]struct{}
}

// GetName returns name of the file.
func (fi *LocalFileEntryInternal) GetName() string {
	return fi.name
}

// GetPath returns current path of the file.
func (fi *LocalFileEntryInternal) GetPath() string {
	return path.Join(fi.dir, fi.relativePath)
}

// Stat returns a FileInfo describing the named file.
func (fi *LocalFileEntryInternal) Stat() (os.FileInfo, error) {
	return os.Stat(fi.GetPath())
}

// Create creates a file on disk.
func (fi *LocalFileEntryInternal) Create(len int64) error {
	targetPath := fi.GetPath()

	// Create dir
	os.MkdirAll(filepath.Dir(targetPath), DefaultDirPermission)

	// Create file
	f, err := os.Create(targetPath)
	if err != nil {
		return err
	}
	defer f.Close()

	// Change size
	err = f.Truncate(len)
	if err != nil {
		// Try to delete file
		os.Remove(targetPath)
		return err
	}
	return nil
}

// CreateLinkFrom creates a hardlink from another file.
func (fi *LocalFileEntryInternal) CreateLinkFrom(sourcePath string) error {
	targetPath := fi.GetPath()

	// Create dir
	os.MkdirAll(filepath.Dir(targetPath), DefaultDirPermission)

	// Create hardlink
	return os.Link(sourcePath, targetPath)
}

// LinkTo creates a hardlink to an unmanaged file.
func (fi *LocalFileEntryInternal) LinkTo(targetPath string) error {
	sourcePath := fi.GetPath()

	// Create dir
	os.MkdirAll(filepath.Dir(targetPath), DefaultDirPermission)

	// Create hardlink
	err := os.Link(sourcePath, targetPath)
	if err != nil {
		return err
	}
	return nil
}

// Move moves file to target dir under the same name, removes all metadata, and updates dir.
func (fi *LocalFileEntryInternal) Move(targetDir string) error {
	sourcePath := path.Dir(fi.GetPath())
	targetPath := path.Join(targetDir, path.Dir(fi.relativePath))
	os.MkdirAll(filepath.Dir(targetPath), DefaultDirPermission)

	if err := os.Rename(sourcePath, targetPath); err != nil {
		return err
	}

	// Update dir in memory.
	fi.dir = targetDir

	// Delete metadata we don't want to keep. Ignore error.
	cleanup := func(mt MetadataType) error {
		if !mt.Movable() {
			return fi.DeleteMetadata(mt)
		}
		return nil
	}
	fi.RangeMetadata(cleanup)

	return nil
}

// Delete removes file and all of its metedata files from disk.
func (fi *LocalFileEntryInternal) Delete() error {
	os.RemoveAll(path.Dir(fi.GetPath()))
	return nil
}

// AddMetadata adds a new metadata type to metadataSet. This is primirily used during reload.
func (fi *LocalFileEntryInternal) AddMetadata(mt MetadataType) error {
	filePath := path.Join(path.Dir(fi.GetPath()), mt.GetSuffix())

	// Check existence.
	if _, err := os.Stat(filePath); err != nil {
		return err
	}
	fi.metadataSet[mt] = struct{}{}
	return nil
}

// ReadMetadata returns metadata content as a byte array.
func (fi *LocalFileEntryInternal) ReadMetadata(mt MetadataType) ([]byte, error) {
	filePath := path.Join(path.Dir(fi.GetPath()), mt.GetSuffix())

	// Check existence.
	if _, err := os.Stat(filePath); err != nil {
		return nil, err
	}

	return ioutil.ReadFile(filePath)
}

// WriteMetadata updates metadata and returns true only if the file is updated correctly;
// It returns false if error happened or file already contains desired content.
func (fi *LocalFileEntryInternal) WriteMetadata(mt MetadataType, b []byte) (bool, error) {
	filePath := path.Join(path.Dir(fi.GetPath()), mt.GetSuffix())

	updated, err := CompareAndWriteFile(filePath, b)
	if err == nil {
		fi.metadataSet[mt] = struct{}{}
	}
	return updated, err
}

// ReadMetadataAt reads metadata at specified offset.
func (fi *LocalFileEntryInternal) ReadMetadataAt(mt MetadataType, b []byte, off int64) (int, error) {
	filePath := path.Join(path.Dir(fi.GetPath()), mt.GetSuffix())

	// Check existence.
	if _, err := os.Stat(filePath); err != nil {
		return 0, err
	}

	// Read to data.
	f, err := os.Open(filePath)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	return f.ReadAt(b, off)
}

// WriteMetadataAt writes metadata at specified offset.
func (fi *LocalFileEntryInternal) WriteMetadataAt(mt MetadataType, b []byte, off int64) (int, error) {
	filePath := path.Join(path.Dir(fi.GetPath()), mt.GetSuffix())

	// Check existence.
	if _, err := os.Stat(filePath); err != nil {
		return 0, err
	}

	// Compare with existing data, overwrite if different.
	f, err := os.OpenFile(filePath, os.O_RDWR, 0755)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	buf := make([]byte, len(b))
	_, err = f.ReadAt(buf, off)
	if err != nil {
		return 0, err
	}
	if bytes.Compare(buf, b) == 0 {
		return 0, nil
	}

	return f.WriteAt(b, off)
}

// DeleteMetadata deletes metadata of the specified type.
func (fi *LocalFileEntryInternal) DeleteMetadata(mt MetadataType) error {
	filePath := path.Join(path.Dir(fi.GetPath()), mt.GetSuffix())

	// Remove from map no matter if the actual metadata file is removed from disk.
	defer delete(fi.metadataSet, mt)

	return os.RemoveAll(filePath)
}

// RangeMetadata lofis through all metadata and applies function f, until an error happens.
func (fi *LocalFileEntryInternal) RangeMetadata(f func(mt MetadataType) error) error {
	for mt := range fi.metadataSet {
		if err := f(mt); err != nil {
			return err
		}
	}
	return nil
}
