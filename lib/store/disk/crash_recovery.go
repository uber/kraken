package disk

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

type rebootedBlob struct {
	key       string
	size      uint64
	mTime     time.Time
	evictable bool
	complete  bool
}

func rebootPersistedStore(config *Config, log *zap.SugaredLogger, stats tally.Scope) (*store, error) {
	incompleteDirPath := filepath.Join(config.RootDir, _incompleteSubDir)
	if !config.RebootIncompleteBlobs {
		err := os.RemoveAll(incompleteDirPath)
		if err != nil {
			return nil, fmt.Errorf("remove incomplete blobs left from a previous service run: %w", err)
		}
	}

	pather := newPather(config.RootDir, config.ShardLength)
	keys, err := pather.rebootKeys(_completeBlob)
	if err != nil {
		return nil, err
	}
	numCompleteBlobs := len(keys)
	if config.RebootIncompleteBlobs {
		incompleteKeys, err := pather.rebootKeys(_incompleteBlob)
		if err != nil {
			return nil, err
		}
		keys = append(keys, incompleteKeys...)
	}

	completeEvictableBlobs := make([]*rebootedBlob, 0)
	otherBlobs := make([]*rebootedBlob, 0)
	for i, key := range keys {
		complete := i < numCompleteBlobs
		b, ok, err := rebootBlob(key, complete, pather)
		if err != nil {
			return nil, err
		}
		if !ok {
			log.With("key", key).Warn("Could not reboot blob from disk - its parent directory is there but the blob is missing")
			continue
		}
		if b.complete && b.evictable {
			completeEvictableBlobs = append(completeEvictableBlobs, b)
		} else {
			otherBlobs = append(otherBlobs, b)
		}
	}

	storeSize := uint64(0)
	blobs := make(map[string]*blob, 0)
	for _, b := range otherBlobs {
		blobs[b.key] = &blob{
			size:           b.size,
			complete:       b.complete,
			evictionBanned: !b.evictable,
			node:           nil,
		}
		storeSize += b.size
	}

	slices.SortFunc(completeEvictableBlobs, func(left, right *rebootedBlob) int {
		// left-most is oldest, i.e. next-to-evict.
		return left.mTime.Compare(right.mTime)
	})
	evictQueue := list.New()
	for _, b := range completeEvictableBlobs {
		node := evictQueue.PushBack(b.key)
		blobs[b.key] = &blob{
			size:           b.size,
			complete:       true,
			node:           node,
			evictionBanned: false,
		}
		storeSize += b.size
	}

	store := &store{
		blobs:      blobs,
		evictQueue: evictQueue,
		capacity:   config.CapacityBytes,
		size:       storeSize,
		pather:     pather,
		config:     config,
		log:        log,
		stats:      stats,
	}

	if store.size > store.capacity {
		prevSize := store.size
		// evicts blobs until size <= capacity.
		err = store.ensureFreeSpace(0)
		if err != nil {
			log.With("error", err).Error("Store size exceeds its capacity after service reboot. Evicting blobs from disk did not work to reduce size within capacity.")
			return nil, fmt.Errorf("remove blobs to reduce store size within configured capacity: %w", err)
		}
		evictedBytes := prevSize - store.size
		log.With("evicted_bytes", evictedBytes).Warn("Store size exceeded its capacity after service reboot. Successfully evicted blobs to reduce size within capacity.")
	}
	return store, nil
}

func rebootBlob(key string, complete bool, pather *pather) (res *rebootedBlob, ok bool, err error) {
	blobPath := pather.blobPath(key, complete)
	fInfo, err := os.Stat(blobPath)
	if errors.Is(err, os.ErrNotExist) {
		// The directory for the blob exists but not the blob itself.
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("stat blob file: %w", err)
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
	return &rebootedBlob{
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
	defer closers.Close(blobSizeF)
	blobSizeData, err := io.ReadAll(blobSizeF)
	if err != nil {
		return 0, false, fmt.Errorf("read blob size sidecar file: %w", err)
	}
	blobSize, err := strconv.Atoi(string(blobSizeData))
	if err != nil {
		return 0, false, fmt.Errorf("blob size sidecar file is in unexpected format: %w", err)
	}
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
