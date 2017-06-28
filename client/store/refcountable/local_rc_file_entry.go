package refcountable

import "code.uber.internal/infra/kraken/client/store/base"

// LocalRCFileEntryFactory is responsible for initializing LocalRCFileEntry objects.
type LocalRCFileEntryFactory struct {
}

// Create initializes and returns a FileEntry object.
func (f *LocalRCFileEntryFactory) Create(state base.FileState, fi base.FileEntryInternal) base.FileEntry {
	baseF := base.LocalFileEntryFactory{}
	return &LocalRCFileEntry{
		LocalFileEntry: baseF.Create(state, fi).(*base.LocalFileEntry),
	}
}

// LocalRCFileEntry manages one file and its metadata on local disk, and keeps file ref counts on local disk too.
type LocalRCFileEntry struct {
	fi RCFileEntryInternal

	*base.LocalFileEntry
}

// GetRefCount returns current ref count. No ref count file means ref count is 0.
func (entry *LocalRCFileEntry) GetRefCount(v base.Verify) (int64, error) {
	entry.RLock()
	defer entry.RUnlock()
	if err := v(entry); err != nil {
		return 0, err
	}

	return entry.GetInternal().(RCFileEntryInternal).GetRefCount()
}

// IncrementRefCount increments ref count by 1.
func (entry *LocalRCFileEntry) IncrementRefCount(v base.Verify) (int64, error) {
	entry.Lock()
	defer entry.Unlock()
	if err := v(entry); err != nil {
		return 0, err
	}

	return entry.GetInternal().(RCFileEntryInternal).IncrementRefCount()
}

// DecrementRefCount decrements ref count by 1.
func (entry *LocalRCFileEntry) DecrementRefCount(v base.Verify) (int64, error) {
	entry.Lock()
	defer entry.Unlock()
	if err := v(entry); err != nil {
		return 0, err
	}

	return entry.GetInternal().(RCFileEntryInternal).DecrementRefCount()
}
