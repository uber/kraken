// Copyright (c) 2016-2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package store

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/uber/kraken/lib/store/base"
	"github.com/uber/kraken/lib/store/metadata"
	"github.com/uber/kraken/utils/log"

	"github.com/andres-erbsen/clock"
	"github.com/uber-go/tally"
)

// CleanupConfig defines configuration for periodically cleaning up idle files.
type CleanupConfig struct {
	Disabled bool          `yaml:"disabled"`
	Interval time.Duration `yaml:"interval"` // How often cleanup runs.
	TTI      time.Duration `yaml:"tti"`      // Time to idle based on last access time.
	TTL      time.Duration `yaml:"ttl"`      // Time to live regardless of access. If 0, disables TTL.
}

func (c CleanupConfig) applyDefaults() CleanupConfig {
	if c.Interval == 0 {
		c.Interval = 30 * time.Minute
	}
	if c.TTI == 0 {
		c.TTI = 6 * time.Hour
	}
	return c
}

type cleanupManager struct {
	clk      clock.Clock
	stats    tally.Scope
	stopOnce sync.Once
	stopc    chan struct{}
}

func newCleanupManager(clk clock.Clock, stats tally.Scope) (*cleanupManager, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return nil, fmt.Errorf("look up hostname: %s", err)
	}
	stats = stats.Tagged(map[string]string{
		"module":   "storecleanup",
		"hostname": hostname,
	})
	return &cleanupManager{
		clk:   clk,
		stats: stats,
		stopc: make(chan struct{}),
	}, nil
}

// addJob starts a background cleanup task which removes idle files from op based
// on the settings in config. op must set the desired states to clean before addJob
// is called.
func (m *cleanupManager) addJob(tag string, config CleanupConfig, op base.FileOp) {
	config = config.applyDefaults()
	if config.Disabled {
		log.Warnf("Cleanup disabled for %s", op)
		return
	}
	if config.TTL == 0 {
		log.Warnf("TTL disabled for %s", op)
	}

	ticker := m.clk.Ticker(config.Interval)

	usageGauge := m.stats.Tagged(map[string]string{"job": tag}).Gauge("disk_usage")

	go func() {
		for {
			select {
			case <-ticker.C:
				log.Debugf("Performing cleanup of %s", op)
				usage, err := m.scan(op, config.TTI, config.TTL)
				if err != nil {
					log.Errorf("Error scanning %s: %s", op, err)
				}
				usageGauge.Update(float64(usage))
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

// scan scans the op for idle or expired files. Also returns the total disk usage
// of op.
func (m *cleanupManager) scan(
	op base.FileOp, tti time.Duration, ttl time.Duration) (usage int64, err error) {

	names, err := op.ListNames()
	if err != nil {
		return 0, fmt.Errorf("list names: %s", err)
	}
	for _, name := range names {
		info, err := op.GetFileStat(name)
		if err != nil {
			log.With("name", name).Errorf("Error getting file stat: %s", err)
			continue
		}
		if ready, err := m.readyForDeletion(op, name, info, tti, ttl); err != nil {
			log.With("name", name).Errorf("Error checking if file expired: %s", err)
		} else if ready {
			if err := op.DeleteFile(name); err != nil && err != base.ErrFilePersisted {
				log.With("name", name).Errorf("Error deleting expired file: %s", err)
			}
		}
		usage += info.Size()
	}
	return usage, nil
}

func (m *cleanupManager) readyForDeletion(
	op base.FileOp,
	name string,
	info os.FileInfo,
	tti time.Duration,
	ttl time.Duration) (bool, error) {

	if ttl > 0 && m.clk.Now().Sub(info.ModTime()) > ttl {
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
