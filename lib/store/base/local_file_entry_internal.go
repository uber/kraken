package base

import (
	"bytes"
	"io/ioutil"
	"os"
	"path"
)

var _ FileEntryInternalFactory = (*LocalFileEntryInternalFactory)(nil)
var _ FileEntryInternalFactory = (*CASFileEntryInternalFactory)(nil)

// LocalFileEntryInternalFactory initializes LocalFileEntryInternal obj.
type LocalFileEntryInternalFactory struct{}

// Create initializes and returns a FileEntryInternal object.
func (f *LocalFileEntryInternalFactory) Create(parentDir, name string) FileEntryInternal {
	return &LocalFileEntryInternal{
		parentDir:    parentDir,
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
func (f *CASFileEntryInternalFactory) Create(parentDir, name string) FileEntryInternal {
	return &LocalFileEntryInternal{
		parentDir:    parentDir,
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
	parentDir    string
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
	return path.Join(fi.parentDir, fi.relativePath)
}

// Stat returns a FileInfo describing the named file.
func (fi *LocalFileEntryInternal) Stat() (os.FileInfo, error) {
	return os.Stat(fi.GetPath())
}

// Create creates a file on disk.
func (fi *LocalFileEntryInternal) Create(len int64) error {
	targetPath := fi.GetPath()

	// Create dir
	os.MkdirAll(path.Dir(targetPath), DefaultDirPermission)

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
		os.RemoveAll(path.Dir(targetPath))
		return err
	}
	return f.Close()
}

// MoveFrom moves an unmanaged file in.
func (fi *LocalFileEntryInternal) MoveFrom(sourcePath string) error {
	targetPath := fi.GetPath()

	// Create dir.
	os.MkdirAll(path.Dir(targetPath), DefaultDirPermission)

	// Move data.
	return os.Rename(sourcePath, targetPath)
}

// Move moves file to target dir under the same name, moves all metadata that's `movable`, and updates dir.
func (fi *LocalFileEntryInternal) Move(targetDir string) error {
	sourcePath := fi.GetPath()
	targetPath := path.Join(targetDir, fi.relativePath)
	if err := os.MkdirAll(path.Dir(targetPath), DefaultDirPermission); err != nil {
		return err
	}

	// Copy metadata first.
	performCopy := func(mt MetadataType) error {
		if mt.Movable() {
			sourceMetadataPath := fi.getMetadataPath(mt)
			targetMetadataPath := path.Join(targetDir, path.Dir(fi.relativePath), mt.GetSuffix())
			bytes, err := ioutil.ReadFile(sourceMetadataPath)
			if err != nil {
				return err
			}
			if _, err := CompareAndWriteFile(targetMetadataPath, bytes); err != nil {
				return err
			}
		}
		return nil
	}
	if err := fi.RangeMetadata(performCopy); err != nil {
		return err
	}

	// Move data. This could be a slow operation if source and target are not on the same FS.
	if err := os.Rename(sourcePath, targetPath); err != nil {
		return err
	}

	// Update parent dir in memory.
	fi.parentDir = targetDir

	// Delete source dir. Ignore error.
	os.RemoveAll(path.Dir(sourcePath))

	return nil
}

// MoveTo moves data file out to an unmanaged location.
func (fi *LocalFileEntryInternal) MoveTo(targetPath string) error {
	sourcePath := fi.GetPath()

	// Create dir.
	os.MkdirAll(path.Dir(targetPath), DefaultDirPermission)

	// Move data.
	if err := os.Rename(sourcePath, targetPath); err != nil {
		return err
	}

	// Delete source dir. Ignore error.
	os.RemoveAll(path.Dir(sourcePath))
	return nil
}

// Delete removes file and all of its metedata files from disk.
func (fi *LocalFileEntryInternal) Delete() error {
	os.RemoveAll(path.Dir(fi.GetPath()))
	return nil
}

func (fi *LocalFileEntryInternal) getMetadataPath(mt MetadataType) string {
	return path.Join(path.Dir(fi.GetPath()), mt.GetSuffix())
}

// AddMetadata adds a new metadata type to metadataSet. This is primirily used during reload.
func (fi *LocalFileEntryInternal) AddMetadata(mt MetadataType) error {
	filePath := fi.getMetadataPath(mt)

	// Check existence.
	if _, err := os.Stat(filePath); err != nil {
		return err
	}
	fi.metadataSet[mt] = struct{}{}
	return nil
}

// ReadMetadata returns metadata content as a byte array.
func (fi *LocalFileEntryInternal) ReadMetadata(mt MetadataType) ([]byte, error) {
	filePath := fi.getMetadataPath(mt)

	// Check existence.
	if _, err := os.Stat(filePath); err != nil {
		return nil, err
	}

	return ioutil.ReadFile(filePath)
}

// WriteMetadata updates metadata and returns true only if the file is updated correctly;
// It returns false if error happened or file already contains desired content.
func (fi *LocalFileEntryInternal) WriteMetadata(mt MetadataType, b []byte) (bool, error) {
	filePath := fi.getMetadataPath(mt)

	updated, err := CompareAndWriteFile(filePath, b)
	if err == nil {
		fi.metadataSet[mt] = struct{}{}
	}
	return updated, err
}

// ReadMetadataAt reads metadata at specified offset.
func (fi *LocalFileEntryInternal) ReadMetadataAt(mt MetadataType, b []byte, off int64) (int, error) {
	filePath := fi.getMetadataPath(mt)

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
	filePath := fi.getMetadataPath(mt)

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
	filePath := fi.getMetadataPath(mt)

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

// GetOrSetMetadata writes b under metadata mt if mt has not been initialized yet.
// Always returns the final content of the metadata, whether it be the existing
// content or the content just written.
func (fi *LocalFileEntryInternal) GetOrSetMetadata(mt MetadataType, b []byte) ([]byte, error) {
	if _, ok := fi.metadataSet[mt]; ok {
		return fi.ReadMetadata(mt)
	}
	filePath := path.Join(path.Dir(fi.GetPath()), mt.GetSuffix())
	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0755)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	if _, err := f.Write(b); err != nil {
		return nil, err
	}
	fi.metadataSet[mt] = struct{}{}

	c := make([]byte, len(b))
	copy(c, b)
	return c, nil
}
