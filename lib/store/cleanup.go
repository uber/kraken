package store

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/andres-erbsen/clock"

	"code.uber.internal/infra/kraken/lib/store/base"
	"code.uber.internal/infra/kraken/lib/store/metadata"
	"code.uber.internal/infra/kraken/utils/log"
)

// CleanupConfig defines configuration for periodically cleaning up idle files.
type CleanupConfig struct {
	Disabled bool          `yaml:"disabled"`
	Interval time.Duration `yaml:"interval"` // How often cleanup runs.
	TTI      time.Duration `yaml:"tti"`      // Time to idle based on last access time.
}

func (c CleanupConfig) applyDefaults() CleanupConfig {
	if c.Interval == 0 {
		c.Interval = 30 * time.Minute
	}
	if c.TTI == 0 {
		c.TTI = 24 * time.Hour
	}
	return c
}

type cleanupManager struct {
	clk      clock.Clock
	stopOnce sync.Once
	stopc    chan struct{}
}

func newCleanupManager(clk clock.Clock) *cleanupManager {
	return &cleanupManager{
		clk:   clk,
		stopc: make(chan struct{}),
	}
}

func (m *cleanupManager) deleteIdleFiles(op base.FileOp, tti time.Duration) error {
	names, err := op.ListNames()
	if err != nil {
		return fmt.Errorf("list names: %s", err)
	}
	for _, name := range names {
		var lat metadata.LastAccessTime
		if err := op.GetFileMetadata(name, &lat); os.IsNotExist(err) {
			continue
		} else if err != nil {
			return fmt.Errorf("get last access time: %s", err)
		}
		if m.clk.Now().Sub(lat.Time) > tti {
			if err := op.DeleteFile(name); err != nil {
				log.With("name", name).Errorf("Error deleting idle file: %s", err)
			}
		}
	}
	return nil
}

// addJob starts a background cleanup task which removes idle files from op based
// on the settings in config. op must set the desired states to clean before addJob
// is called.
func (m *cleanupManager) addJob(config CleanupConfig, op base.FileOp) {
	config = config.applyDefaults()
	if config.Disabled {
		log.Warnf("Cleanup disabled for %s", op)
		return
	}
	ticker := m.clk.Ticker(config.Interval)
	go func() {
		for {
			select {
			case <-ticker.C:
				log.Infof("Deleting idle files from %s", op)
				if err := m.deleteIdleFiles(op, config.TTI); err != nil {
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
