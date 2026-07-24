package store

import (
	"container/list"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"time"

	"github.com/uber-go/tally"
	"github.com/uber/kraken/utils/closers"
	"go.uber.org/zap"
)

type blobState struct {
	key       string
	size      uint64
	mTime     time.Time
	evictable bool
	complete  bool
}

func rebootPersistedStore(capacityBytes uint64, rootDir string, rebootIncompleteBlobs bool, shardLength int, log *zap.SugaredLogger, metrics tally.Scope) (*diskStore, error) {
	completeDirPath, incompleteDirPath := filepath.Join(rootDir, _completeSubDir), filepath.Join(rootDir, _incompleteSubDir)
	if !rebootIncompleteBlobs {
		err := os.RemoveAll(incompleteDirPath)
		if err != nil {
			return nil, fmt.Errorf("remove incomplete blobs left from a previous service run: %w", err)
		}
	}

	pather := newPather(rootDir, shardLength)
	keys, err := pather.rebootKeys(completeDirPath)
	if err != nil {
		return nil, err
	}
	numCompleteBlobs := len(keys)
	if rebootIncompleteBlobs {
		incompleteKeys, err := pather.rebootKeys(incompleteDirPath)
		if err != nil {
			return nil, err
		}
		keys = append(keys, incompleteKeys...)
	}

	completeEvictableEntries := make([]*blobState, 0)
	otherEntries := make([]*blobState, 0)
	for i, key := range keys {
		complete := i < numCompleteBlobs
		bState, ok, err := rebootBlobState(key, complete, pather)
		if err != nil {
			return nil, err
		}
		if !ok {
			log.Warn("Could not reboot blob from disk - its parent directory is there but the blob is missing")
			continue
		}
		if bState.complete && bState.evictable {
			completeEvictableEntries = append(completeEvictableEntries, bState)
		} else {
			otherEntries = append(otherEntries, bState)
		}
	}

	storeSize := uint64(0)
	blobs := make(map[string]*blob, 0)
	for _, bState := range otherEntries {
		blobs[bState.key] = &blob{
			size:           bState.size,
			complete:       bState.complete,
			evictionBanned: !bState.evictable,
			node:           nil,
		}
		storeSize += bState.size
	}

	slices.SortFunc(completeEvictableEntries, func(left, right *blobState) int {
		// left-most is oldest, i.e. next-to-evict.
		return left.mTime.Compare(right.mTime)
	})
	evictQueue := list.New()
	for _, bState := range completeEvictableEntries {
		node := evictQueue.PushBack(bState.key)
		blobs[bState.key] = &blob{
			size:           bState.size,
			complete:       true,
			node:           node,
			evictionBanned: false,
		}
		storeSize += bState.size
	}

	store := &diskStore{
		blobs:                 blobs,
		evictQueue:            evictQueue,
		capacity:              capacityBytes,
		pather:                pather,
		size:                  storeSize,
		rebootIncompleteBlobs: rebootIncompleteBlobs,
		log:                   log,
		metrics:               metrics,
	}

	if store.size > store.capacity {
		prevSize := store.size
		// evicts entries until size <= capacity.
		err = store.reserveSpace(0)
		if err != nil {
			log.With("error", err).Error("DiskStore size exceeds its capacity after service reboot. Evicting blobs from disk did not work to reduce size within capacity.")
			return nil, fmt.Errorf("remove blobs to reduce store size within configured capacity: %w", err)
		}
		evictedBytes := prevSize - store.size
		log.With("evicted_bytes", evictedBytes).Warn("DiskStore size exceeded its capacity after service reboot. Successfully evicted blobs to reduce size within capacity.")
	}
	return store, nil
}

func rebootBlobState(key string, complete bool, pather *pather) (res *blobState, ok bool, err error) {
	blobPath := pather.blobPath(key, complete)
	fInfo, err := os.Stat(blobPath)
	if errors.Is(err, os.ErrNotExist) {
		// The directory for the blob exists but not the blob itself.
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("stat blob entry: %w", err)
	}

	flagBlobPath := pather.sidecarFilePath(key, complete, _evictionBannedFileName)
	isUnevictable, err := exists(flagBlobPath)
	if err != nil {
		return nil, false, err
	}
	var size uint64
	if complete {
		size = uint64(fInfo.Size())
	} else {
		size, ok, err = rebootIncompleteBlobSize(key, pather)
		if err != nil {
			return nil, false, fmt.Errorf("get incomplete blob size from sidecar file: %w", err)
		}
		if !ok {
			return nil, false, nil
		}
	}
	mTime := fInfo.ModTime()
	return &blobState{
		key:       key,
		size:      size,
		mTime:     mTime,
		evictable: !isUnevictable,
		complete:  complete,
	}, true, nil
}

func rebootIncompleteBlobSize(key string, pather *pather) (size uint64, ok bool, err error) {
	blobSizeFilePath := pather.sidecarFilePath(key, _incompleteBlob, _blobSizeFileName)
	blobSizeF, err := os.OpenFile(blobSizeFilePath, os.O_RDONLY, _defaultFilePerm)
	if errors.Is(err, os.ErrNotExist) {
		// The size metadata file is not present, we fail-open by evicting the blob.
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("open blob size sidecar file: %w", err)
	}
	blobSizeData, err := io.ReadAll(blobSizeF)
	if err != nil {
		return 0, false, fmt.Errorf("read blob size sidecar file: %w", err)
	}
	blobSize, err := strconv.Atoi(string(blobSizeData))
	if err != nil {
		return 0, false, fmt.Errorf("blob size sidecar file is in unexpected format: %w", err)
	}
	closers.Close(blobSizeF)
	return uint64(blobSize), true, nil
}

func existsPersistedStore(rootDir string) (ok bool, err error) {
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
