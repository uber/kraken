package disk

import (
	"errors"
	"os"

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

func (s *store) cleanLeakedFiles() {
	s.log.Info("Starting a leak garbage collection run...")

	incompleteKeys := s.List(storelib.BlobScopeIncomplete)
	leakedKeys := make([]string, 0)
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

		isLeaked := s.clk.Now().Sub(info.ModTime()) > s.config.IncompleteBlobTTI
		if isLeaked {
			leakedKeys = append(leakedKeys, key)
		}
	}
	if len(leakedKeys) == 0 {
		s.log.Info("No leaked files were found on disk")
		return
	}

	s.log.With(
		"num_leaked_files", len(leakedKeys),
		"leaked_keys", leakedKeys,
		"incomplete_blob_tti", s.config.IncompleteBlobTTI).
		Warn("Leaked incomplete blobs were found on disk (not touched for more than incomplete_blob_tti). Clients of disk.Store are probably misusing the store. Proceeding with deletion")

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, key := range leakedKeys {
		err := s.deleteNoLock(key, storelib.BlobScopeIncomplete)
		if err != nil {
			s.log.With("key", key, "error", err).Error("Could not delete leaked, incomplete blob from disk")
		}
	}
}
