package refcountable

import (
	"encoding/binary"
	"fmt"
	"os"

	"code.uber.internal/infra/kraken/lib/store/base"
)

var _ base.FileEntryInternalFactory = (*LocalRCFileEntryInternalFactory)(nil)

// LocalRCFileEntryInternalFactory initializes LocalFileEntryInternal obj.
type LocalRCFileEntryInternalFactory struct {
	baseF *base.ShardedFileEntryInternalFactory
}

// NewLocalRCFileEntryInternalFactory returns a new LocalRCFileEntryInternalFactory
func NewLocalRCFileEntryInternalFactory(shardIDLength uint) *LocalRCFileEntryInternalFactory {
	return &LocalRCFileEntryInternalFactory{base.NewShardedFileEntryInternalFactory(shardIDLength)}
}

// Create initializes and returns a FileEntryInternal object.
func (f *LocalRCFileEntryInternalFactory) Create(dir, name string) base.FileEntryInternal {
	return &LocalRCFileEntryInternal{
		FileEntryInternal: f.baseF.Create(dir, name),
	}
}

// GetRelativePath returns
func (f *LocalRCFileEntryInternalFactory) GetRelativePath(name string) string {
	return f.baseF.GetRelativePath(name)
}

// LocalRCFileEntryInternal extends LocalFileEntryInternal, adds functions to manage file ref count.
type LocalRCFileEntryInternal struct {
	base.FileEntryInternal
}

// Move checks ref count, then moves file to target dir under the same name, removes all metadata,
// and updates dir.
func (fi *LocalRCFileEntryInternal) Move(targetDir string) error {
	// Verify it's safe to delete data file and/or metadata.
	checkSafeToDelete := func(mt base.MetadataType) error {
		refCount, err := fi.GetRefCount()
		if err == nil && refCount == 0 {
			return nil
		}
		return &RefCountError{Op: "SafeToDelete", Name: fi.GetPath(), RefCount: refCount, Msg: fmt.Sprintf("File still referenced")}
	}
	if err := fi.RangeMetadata(checkSafeToDelete); err != nil {
		return err
	}

	return fi.FileEntryInternal.Move(targetDir)
}

// Delete checks ref count, then removes file and all of its metedata files from disk.
func (fi *LocalRCFileEntryInternal) Delete() error {
	// Verify it's safe to delete data file and/or metadata.
	checkSafeToDelete := func(mt base.MetadataType) error {
		refCount, err := fi.GetRefCount()
		if err == nil && refCount == 0 {
			return nil
		}
		return &RefCountError{Op: "SafeToDelete", Name: fi.GetPath(), RefCount: refCount, Msg: fmt.Sprintf("File still referenced")}
	}
	if err := fi.RangeMetadata(checkSafeToDelete); err != nil {
		return err
	}

	return fi.FileEntryInternal.Delete()
}

// GetRefCount returns current ref count. No ref count file means ref count is 0.
func (fi *LocalRCFileEntryInternal) GetRefCount() (int64, error) {
	// Read value.
	var refCount int64
	var n int
	b, err := fi.FileEntryInternal.ReadMetadata(NewRefCount())
	if err != nil && !os.IsNotExist(err) {
		return 0, err
	} else if err == nil {
		refCount, n = binary.Varint(b)
		if n <= 0 {
			return 0, fmt.Errorf("Failed to parse ref count: %v", b)
		}
	}
	return refCount, nil
}

func (fi *LocalRCFileEntryInternal) updateRefCount(increment bool) (int64, error) {
	refCount, err := fi.GetRefCount()
	if err != nil {
		return 0, err
	}

	if increment {
		refCount++
	} else if refCount > 0 {
		refCount--
	}
	buf := make([]byte, 8)
	n := binary.PutVarint(buf, refCount)
	if n <= 0 {
		return 0, fmt.Errorf("Failed to put ref count: %d", refCount)
	}

	_, err = fi.FileEntryInternal.WriteMetadata(NewRefCount(), buf)
	if err != nil {
		return 0, err
	}

	return refCount, nil
}

// IncrementRefCount increments ref count by 1.
func (fi *LocalRCFileEntryInternal) IncrementRefCount() (int64, error) {

	return fi.updateRefCount(true)
}

// DecrementRefCount decrements ref count by 1.
func (fi *LocalRCFileEntryInternal) DecrementRefCount() (int64, error) {

	return fi.updateRefCount(false)
}
