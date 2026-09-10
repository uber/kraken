package tiered

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/uber/kraken/lib/store/disk"
	"github.com/uber/kraken/lib/store/memory"
	"github.com/uber/kraken/lib/store/metadata"
	"github.com/uber/kraken/utils/closers"
	"go.uber.org/zap"
)

// indirection needed for testing
var (
	memOpen = func(mem *memory.Store, key string) (*memory.File, error) {
		return mem.Open(key)
	}
	ioCopy = io.Copy
)

type flusher struct {
	blobs     map[string]*blob
	queue     []string
	mu        sync.Mutex
	notify    chan struct{}
	stop      chan struct{}
	wg        sync.WaitGroup
	closeOnce sync.Once

	mem  *memory.Store
	disk *disk.Store
	log  *zap.SugaredLogger
}

type blob struct {
	key       string
	dataDirty bool
	dataSize  uint64
	dirtyMD   map[string]struct{}
	mu        sync.Mutex
}

func newFlusher(mem *memory.Store, disk *disk.Store, log *zap.SugaredLogger, numWorkers int) *flusher {
	f := &flusher{
		blobs:  make(map[string]*blob, 0),
		queue:  make([]string, 0),
		notify: make(chan struct{}, numWorkers),
		stop:   make(chan struct{}),
		mem:    mem,
		disk:   disk,
		log:    log,
	}

	for range numWorkers {
		f.wg.Add(1)
		go f.worker()
	}
	return f
}

// Enqueues a blob and its metadata for flushing to disk. Should be called once per blob, upon MarkComplete.
// Once a blob has been flushed, UnbanEviction is called on the mem store.
func (f *flusher) markDirty(key string, sizeBytes uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()

	dirtyMD, err := f.mem.ListMetadata(key)
	if err != nil {
		f.log.With(
			"key", key,
			"error", fmt.Errorf("mem store list metadata: %w", err),
		).Error("Could not mark blob as dirty, trying to abort flushing")
		err = f.mem.UnbanEviction(key) // prevent leak
		if err != nil {
			f.log.With(
				"key", key,
				"error", fmt.Errorf("mem store unban eviction: %w", err),
			).Error("Leaked blob to mem store while trying to abort flushing")
		}
		return
	}
	dirtyMDMap := make(map[string]struct{})
	for _, dirtyMD := range dirtyMD {
		dirtyMDMap[dirtyMD.GetSuffix()] = struct{}{}
	}

	f.blobs[key] = &blob{
		key:       key,
		dataDirty: true,
		dataSize:  sizeBytes,
		dirtyMD:   dirtyMDMap,
	}
	f.queue = append(f.queue, key)

	select {
	case f.notify <- struct{}{}:
	default:
	}
}

// Must be called on each md mutation.
func (f *flusher) markMetadataDirty(key, mdSuffix string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	b, dirty := f.blobs[key]
	if dirty {
		b.mu.Lock()
		defer b.mu.Unlock()

		b.dirtyMD[mdSuffix] = struct{}{}
		return
	}

	_, onDisk := f.disk.Has(key)
	if !onDisk {
		return // Blob is not yet complete, we can't start flushing.
	}

	f.blobs[key] = &blob{
		key:       key,
		dataDirty: false,
		dirtyMD:   map[string]struct{}{mdSuffix: {}},
	}
	f.queue = append(f.queue, key)

	select {
	case f.notify <- struct{}{}:
	default:
	}
}

// Aborts the flushing of a blob, possibly midway through. May leave corrupt state on disk, which caller is responsible for cleaning.
func (f *flusher) abort(key string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	delete(f.blobs, key)
}

func (f *flusher) worker() {
	defer f.wg.Done()

	for {
		select {
		case <-f.stop:
			// Flush anything left before stopping.
			for {
				b, ok := f.nextToFlush()
				if !ok {
					return
				}
				f.flush(b)
			}
		case <-f.notify:
			for {
				b, ok := f.nextToFlush()
				if !ok {
					break
				}
				f.flush(b)
			}
		}
	}
}

// close blocks until all dirty items are flushed. Items marked as dirty after calling close may or may not be flushed.
func (f *flusher) close() {
	f.closeOnce.Do(func() { close(f.stop) })

	f.wg.Wait()
}

func (f *flusher) nextToFlush() (b *blob, ok bool) {
	f.mu.Lock()
	defer f.mu.Unlock()

	for len(f.queue) > 0 {
		key := f.queue[0]
		f.queue = f.queue[1:]
		b, ok := f.blobs[key]
		if !ok {
			continue
		}
		return b, true
	}
	return nil, false
}

func (f *flusher) flush(b *blob) {
	key := b.key
	defer func() {
		err := f.mem.UnbanEviction(key) // prevent leak
		if err != nil {
			f.log.With(
				"key", key,
				"error", fmt.Errorf("mem store unban eviction: %w", err),
			).Error("Leaked blob to mem store while trying to abort flushing")
		}
	}()

	if b.dataDirty {
		if err := f.flushData(b); err != nil {
			err = fmt.Errorf("flush data: %w", err)
			f.log.With(
				"key", key,
				"error", err,
			).Error("Could not flush data from mem to disk, abandoning flushing operation")
			f.handleFlushFailure(key)
			return
		}
	}

	if err := f.flushMetadatasAndUnmarkDirty(key, b); err != nil {
		err = fmt.Errorf("flush metadatas: %w", err)
		f.log.With(
			"key", key,
			"error", err,
		).Error("Could not flush metadata from mem to disk, abandoning flushing operation")
		f.handleFlushFailure(key)
	}
}

// Tries to prevent corrupt state in disk store upon failed flush. To do so, the flush to disk is aborted,
// simulating similar behavior to the file being flushed and subsequently evicted by the disk store's LRU policy.
// This will break any open [File] handles to the blob after eviction from memory, but it's the best we can do.
func (f *flusher) handleFlushFailure(key string) {
	if err := f.disk.Delete(key); err != nil && !errors.Is(err, os.ErrNotExist) {
		f.log.With(
			"key", key,
			"error", err).
			Error("Could not clean disk entry after flushing failed, blob is now leaked in disk store")
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.blobs, key)
}

func (f *flusher) flushMetadatasAndUnmarkDirty(key string, b *blob) error {
	b.mu.Lock()
	for {
		dirtyMDSnapshot := b.dirtyMD
		b.dirtyMD = make(map[string]struct{})
		b.mu.Unlock()

		for mdSuffix := range dirtyMDSnapshot {
			err := f.flushMetadata(key, mdSuffix)
			if err != nil {
				f.log.With(
					"key", key,
					"mdSuffix", mdSuffix,
					"error", err,
				).Error("Could not flush metadata from mem to disk")
				return fmt.Errorf("flush md: %w", err)
			}
		}

		f.mu.Lock()
		b.mu.Lock()
		if len(b.dirtyMD) == 0 {
			delete(f.blobs, key)
			b.mu.Unlock()
			f.mu.Unlock()
			return nil
		}

		f.mu.Unlock()
	}
}

func (f *flusher) flushMetadata(key, mdSuffix string) error {
	md := metadata.CreateFromSuffix(mdSuffix)
	ok, err := f.mem.GetMetadata(key, md)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("mem store get md: %w", err)
	}
	if !ok {
		err = f.disk.DeleteMetadata(key, md.GetSuffix())
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("disk store delete md: %w", err)
		}
		return nil
	}
	err = f.disk.SetMetadata(key, md)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("disk store set md: %w", err)
	}

	return nil
}

func (f *flusher) flushData(b *blob) error {
	key := b.key
	memF, err := memOpen(f.mem, key)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("mem store open: %w", err)
	}
	defer closers.Close(memF)
	diskF, err := f.disk.Create(key, b.dataSize)
	if err != nil {
		return fmt.Errorf("disk store create: %w", err)
	}
	defer closers.Close(diskF)
	f.mu.Lock()
	_, ok := f.blobs[b.key]
	if !ok {
		// abort was called before we created the file, we need to cleanup.
		err := f.disk.Delete(key)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			f.log.With(
				"key", key,
				"error", err).
				Error("Could not clean disk entry after flushing failed, blob is now leaked in disk store")
		}
		f.mu.Unlock()
		return nil
	}
	f.mu.Unlock()
	_, err = ioCopy(diskF, memF)
	if errors.Is(err, memory.ErrEvicted) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("io copy from mem file to disk file: %w", err)
	}
	err = f.disk.MarkComplete(key)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("disk mark complete: %w", err)
	}
	return nil
}
