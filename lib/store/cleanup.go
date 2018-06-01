package store

import (
	"fmt"
	"os"
	"sync"
	"time"

	"code.uber.internal/infra/kraken/lib/store/base"
	"code.uber.internal/infra/kraken/lib/store/metadata"
	"code.uber.internal/infra/kraken/utils/log"

	"github.com/andres-erbsen/clock"
	"github.com/uber-go/tally"
)

// CleanupConfig defines configuration for periodically cleaning up idle files.
type CleanupConfig struct {
	Disabled bool          `yaml:"disabled"`
	Interval time.Duration `yaml:"interval"` // How often cleanup runs.
	TTI      time.Duration `yaml:"tti"`      // Time to idle based on last access time.
	TTL      time.Duration `yaml:"ttl"`      // Time to live based on last update time.
}

func (c CleanupConfig) applyDefaults() CleanupConfig {
	if c.Interval == 0 {
		c.Interval = 30 * time.Minute
	}
	if c.TTI == 0 {
		c.TTI = 6 * time.Hour
	}
	if c.TTL == 0 {
		c.TTL = 24 * time.Hour
	}
	return c
}

type cleanupManager struct {
	clk      clock.Clock
	stats    tally.Scope
	stopOnce sync.Once
	stopc    chan struct{}
}

func newCleanupManager(clk clock.Clock, stats tally.Scope) *cleanupManager {
	return &cleanupManager{
		clk:   clk,
		stats: stats,
		stopc: make(chan struct{}),
	}
}

// addJob starts a background cleanup task which removes idle files from op based
// on the settings in config. op must set the desired states to clean before addJob
// is called.
func (m *cleanupManager) addJob(jobName string, config CleanupConfig, op base.FileOp) {
	config = config.applyDefaults()
	if config.Disabled {
		log.Warnf("Cleanup disabled for %s", op)
		return
	}
	ticker := m.clk.Ticker(config.Interval)

	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("Error getting hostname: %s", err)
	}
	stats := m.stats.Tagged(map[string]string{
		"job":  jobName,
		"host": hostname,
	})
	gauge := stats.Gauge("disk_size")

	go func() {
		for {
			select {
			case <-ticker.C:
				log.Infof("Deleting idle files from %s", op)
				if err := m.deleteIdleOrExpiredFiles(op, config.TTI, config.TTL, gauge); err != nil {
					log.Errorf("Error deleting idle files from %s: %s", op, err)
				}
			case <-m.stopc:
				ticker.Stop()
				return
			}
		}
	}()
}

func (m *cleanupManager) stop() {
	m.stopOnce.Do(func() { close(m.stopc) })
}

func (m *cleanupManager) deleteIdleOrExpiredFiles(
	op base.FileOp, tti time.Duration, ttl time.Duration, gauge tally.Gauge) error {

	var totalSize int64
	names, err := op.ListNames()
	if err != nil {
		return fmt.Errorf("list names: %s", err)
	}

	for _, name := range names {
		fi, err := op.GetFileStat(name)
		if err != nil {
			log.With("name", name).Errorf("Error getting file stat: %s", err)
			continue
		}
		// Add size before deletion, so the total number we got will include files to be deleted,
		// but exclude some new files added during this function call.
		totalSize += fi.Size()

		if ready, err := m.readyForDeletion(op, name, fi, tti, ttl); err != nil {
			log.With("name", name).Errorf("Error checking if file expired: %s", err)
		} else if ready {
			if err := op.DeleteFile(name); err != nil {
				log.With("name", name).Errorf("Error deleting expired file: %s", err)
			}
		}
	}

	gauge.Update(float64(totalSize))

	return nil
}

func (m *cleanupManager) readyForDeletion(
	op base.FileOp, name string, fi os.FileInfo, tti time.Duration, ttl time.Duration,
) (bool, error) {
	if m.clk.Now().Sub(fi.ModTime()) > ttl {
		return true, nil
	}

	var lat metadata.LastAccessTime
	if err := op.GetFileMetadata(name, &lat); os.IsNotExist(err) {
		return false, nil
	} else if err != nil {
		return false, fmt.Errorf("get file lat: %s", err)
	}

	return m.clk.Now().Sub(lat.Time) > tti, nil
}
