package disk

import (
	"errors"
	"fmt"
	"os"
	"time"

	storelib "github.com/uber/kraken/lib/store"
)

func (s *store) startLeakCleaner() {
	ticker := s.clk.Tick(s.config.LeakCleanerInterval)

	go func() {
		for {
			select {
			case <-s.stopCh:
				close(s.doneCh)
				return
			case <-ticker:
				s.cleanLeakedFiles()
			}
		}
	}()
}

func (s *store) close() {
	close(s.stopCh)
	<-s.doneCh
}

type leakedBlob struct {
	key     string
	size    int64
	modTime time.Time
}

func (b leakedBlob) String() string {
	return fmt.Sprintf("%s(size=%d,mod_time=%s)",
		b.key, b.size, b.modTime.UTC().Format(time.RFC3339Nano))
}

// MarshalText makes a leakedBlob log as its String form. Without it the logger
// reflects over the struct, finds no exported fields and emits an empty object.
func (b leakedBlob) MarshalText() ([]byte, error) {
	return []byte(b.String()), nil
}

func (s *store) cleanLeakedFiles() {
	s.log.Info("Starting a leak garbage collection run...")
	scanStart := s.clk.Now()

	incompleteKeys := s.List(storelib.BlobScopeIncomplete)
	leakedBlobs := make([]leakedBlob, 0)
	for _, key := range incompleteKeys {
		blobPath := s.blobPath(key, _incompleteBlob)
		info, err := os.Stat(blobPath)
		if errors.Is(err, os.ErrNotExist) {
			// Either the blob just got renamed/marked as complete OR
			// somehow the store thinks it has a blob in its memory that it doesn't actually have on disk (an invariant).
			s.log.With("key", key).Warn("incomplete blob returned by List not found on disk (possible invariant)")
			continue
		}
		if err != nil {
			s.log.With("key", key, "error", err).Error("could not Stat a blob during leak scanning")
			continue
		}

		age := s.clk.Now().Sub(info.ModTime())
		isLeaked := age > s.config.IncompleteBlobTTI
		s.log.With(
			"key", key,
			"blob_size", info.Size(),
			"mod_time", info.ModTime().UTC(),
			"age", age,
			"is_leaked", isLeaked).Info("Scanned an incomplete blob for leakage")
		if isLeaked {
			leakedBlobs = append(leakedBlobs, leakedBlob{
				key:     key,
				size:    info.Size(),
				modTime: info.ModTime(),
			})
		}
	}
	scanTime := s.clk.Now().Sub(scanStart)
	if len(leakedBlobs) == 0 {
		s.log.With(
			"num_incomplete_blobs", len(incompleteKeys),
			"scan_time", scanTime).Info("No leaked files were found on disk")
		return
	}

	leakedKeys := make([]string, 0, len(leakedBlobs))
	for _, b := range leakedBlobs {
		leakedKeys = append(leakedKeys, b.key)
	}
	s.log.With(
		"num_leaked_files", len(leakedBlobs),
		"leaked_keys", leakedKeys,
		"leaked_blobs", leakedBlobs,
		"num_incomplete_blobs", len(incompleteKeys),
		"scan_time", scanTime,
		"incomplete_blob_tti", s.config.IncompleteBlobTTI).
		Warn("Leaked incomplete blobs were found on disk (not touched for more than incomplete_blob_tti). Clients of disk.Store are probably misusing the store. Proceeding with deletion")

	lockStart := s.clk.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.log.With(
		"lock_wait", s.clk.Now().Sub(lockStart),
		"num_leaked_files", len(leakedBlobs)).
		Info("Leak cleaner acquired the store lock, deleting the leaked blobs")

	for _, b := range leakedBlobs {
		err := s.deleteNoLock(b.key, storelib.BlobScopeIncomplete)
		if err != nil {
			s.log.With("key", b.key, "error", err).
				Error("Could not delete leaked, incomplete blob from disk")
		}
	}
}
