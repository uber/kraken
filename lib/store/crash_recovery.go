package store

import (
	"container/list"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"go.uber.org/zap"
)

type blobState struct {
	key       string
	size      uint64
	mTime     time.Time
	evictable bool
}

func rebootPersistedStateAfterCrash(capacityBytes uint64, rootDir string, log *zap.SugaredLogger) (*DiskStore, error) {
	completeDirPath, incompleteDirPath := filepath.Join(rootDir, _completeSubDir), filepath.Join(rootDir, _incompleteSubDir)

	// We remove incomplete entries, as we expect the processes/clients who were writing to
	// these entries to also have crashed, which would leak the files on disk.
	err := os.RemoveAll(incompleteDirPath)
	if err != nil {
		return nil, fmt.Errorf("remove incomplete blobs left from a previous service run: %w", err)
	}
	keys, err := rebootKeys(completeDirPath)
	if err != nil {
		return nil, err
	}
	pather := newPather(rootDir)

	completeEntries := make([]*blobState, 0)
	unevictableEntries := make([]*blobState, 0)
	for _, key := range keys {
		bState, ok, err := rebootBlobState(key, pather)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		if bState.evictable {
			completeEntries = append(completeEntries, bState)
		} else {
			unevictableEntries = append(unevictableEntries, bState)
		}
	}

	storeSize := uint64(0)
	unevictableBlobs := make(map[string]uint64, len(unevictableEntries))
	for _, e := range unevictableEntries {
		unevictableBlobs[e.key] = e.size
		storeSize += e.size
	}

	slices.SortFunc(completeEntries, func(left, right *blobState) int {
		// left-most is oldest, i.e. next-to-evict.
		return left.mTime.Compare(right.mTime)
	})
	blobs := make(map[string]*list.Element, len(completeEntries))
	evictQueue := list.New()
	for _, e := range completeEntries {
		node := evictQueue.PushBack(el{key: e.key, size: e.size})
		blobs[e.key] = node
		storeSize += e.size
	}

	store := &DiskStore{
		blobs:            blobs,
		evictQueue:       evictQueue,
		incompleteBlobs:  make(map[string]uint64),
		unevictableBlobs: unevictableBlobs,
		capacity:         capacityBytes,
		pather:           pather,
		size:             storeSize,
		log:              log,
	}

	if store.size > store.capacity {
		prevSize := store.size
		// evicts entries until size <= capacity.
		err = store.reserveSpace(0)
		if err != nil {
			log.With("error", err).Error("DiskStore size exceeds its capacity after service reboot. Evicting blobs from disk did not work to reduce size within capacity.")
			return nil, fmt.Errorf("remove blobs to reduce store size within configured capacity")
		}
		evictedBytes := prevSize - store.size
		log.With("evicted_bytes", evictedBytes).Warn("DiskStore size exceeds its capacity after service reboot. Successfully evicted blobs to reduce size within capacity.")
	}
	return store, nil
}

func rebootBlobState(key string, pather *pather) (res *blobState, ok bool, err error) {
	complete := true
	blobPath := pather.blobPath(key, complete)
	fInfo, err := os.Stat(blobPath)
	if errors.Is(err, os.ErrNotExist) {
		// For some reason, the directory for the blob exists but not the blob itself.
		return nil, true, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("stat blob entry: %w", err)
	}
	flagBlobPath := pather.sidecarFilePath(key, complete, _evictionBannedFileName)
	isUnevictable, err := exists(flagBlobPath)
	if err != nil {
		return nil, false, err
	}
	size := fInfo.Size()
	mTime := fInfo.ModTime()
	return &blobState{
		key:       key,
		size:      uint64(size),
		mTime:     mTime,
		evictable: !isUnevictable,
	}, true, nil
}

func rebootKeys(completeDirPath string) ([]string, error) {
	keys := make([]string, 0)
	err := filepath.WalkDir(completeDirPath, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if !entry.IsDir() {
			return nil
		}
		relPath, err := filepath.Rel(completeDirPath, path)
		if err != nil {
			return err
		}
		nameParts := strings.Split(relPath, string(filepath.Separator))
		isBlobDir := len(nameParts) == _numShards+1
		if !isBlobDir {
			return nil
		}
		key := nameParts[len(nameParts)-1]
		keys = append(keys, key)
		return fs.SkipDir
	})
	if err != nil {
		return nil, fmt.Errorf("walk through complete dir to collect keys of blobs: %w", err)
	}
	return keys, nil
}

func existsPersistedState(rootDir string) (ok bool, err error) {
	completeDir, incompleteDir := filepath.Join(rootDir, _completeSubDir), filepath.Join(rootDir, _incompleteSubDir)
	completeExists, err := exists(completeDir)
	if err != nil {
		return false, fmt.Errorf("check if store has persisted state left on disk from previous service runs: %w", err)
	}
	incompleteExists, err := exists(incompleteDir)
	if err != nil {
		return false, fmt.Errorf("check if store has persisted state left on disk from previous service runs: %w", err)
	}

	return completeExists || incompleteExists, nil
}
