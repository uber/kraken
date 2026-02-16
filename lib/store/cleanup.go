// Copyright (c) 2016-2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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
	"slices"
	"sync"
	"time"

	"github.com/uber/kraken/lib/store/base"
	"github.com/uber/kraken/lib/store/metadata"
	"github.com/uber/kraken/utils/diskspaceutil"
	"github.com/uber/kraken/utils/log"

	"github.com/andres-erbsen/clock"
	"github.com/uber-go/tally"
)

// CleanupConfig defines configuration for periodically cleaning up idle files.
type CleanupConfig struct {
	Disabled                 bool          `yaml:"disabled"`
	Interval                 time.Duration `yaml:"interval"`                   // How often cleanup runs.
	TTI                      time.Duration `yaml:"tti"`                        // Time to idle based on last access time.
	TTL                      time.Duration `yaml:"ttl"`                        // Time to live regardless of access. If 0, disables TTL.
	AggressiveThreshold      int           `yaml:"aggressive_threshold"`       // The disk util threshold to trigger aggressive cleanup. If 0, disables aggressive cleanup.
	AggressiveTTL            time.Duration `yaml:"aggressive_ttL"`             // Time to live regardless of access if aggressive cleanup is triggered.
	AggressiveLowerThreshold int           `yaml:"aggressive_lower_threshold"` // The lower disk util threshold in percent, below which aggressive cleanup will stop. If 0, no lower threshold.
}

type (
	// for mocking
	diskUsageFn func() (diskspaceutil.UsageInfo, error)
)

func (c CleanupConfig) applyDefaults() CleanupConfig {
	if c.Interval == 0 {
		c.Interval = 30 * time.Minute
	}
	if c.TTI == 0 {
		c.TTI = 6 * time.Hour
	}

	if c.AggressiveThreshold != 0 {
		if c.AggressiveTTL == 0 {
			c.AggressiveTTL = 1 * time.Hour
		}
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

	if config.AggressiveThreshold == 0 {
		log.Warnf("Aggressive cleanup disabled for %s", op)
	}

	ticker := m.clk.Ticker(config.Interval)

	usageGauge := m.stats.Tagged(map[string]string{"job": tag}).Gauge("disk_usage")

	go func() {
		for {
			select {
			case <-ticker.C:
				log.Debugf("Performing cleanup of %s", op)
				usage, err := m.cleanup(op, config, cachedInAgentPolicy)
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

type fInfo struct {
	name         string
	accessTime   time.Time
	downloadTime time.Time
	size         int64
}

// cachedInAgentPolicy is a custom cleanup policy that prioritizes discarding blobs already distributed to agents.
// cachedInAgentPolicy is passed to [slices.SortFunc]
func cachedInAgentPolicy(left, right fInfo) int {
	// For context, the order when downloading a file is:
	// 1. Upon downloading the last byte of a file, its download time is set.
	// 2. The application logic sets the access time.
	// 3. no-op - The file is moved from the upload dir to the upload cache, but this changes neither the access nor download times.
	// 4. [optional] File is downloaded by a consumer (proxy or agent) and the access time is updated.

	if isDownloadedByConsumer(left) && !isDownloadedByConsumer(right) {
		return -1
	}
	if !isDownloadedByConsumer(left) && isDownloadedByConsumer(right) {
		return 1
	}
	// At this point, both files must be downloaded by a consumer (proxy/agent).

	// The ifs below check if one file is for sure cached by an agent, while the other isn't.
	if forSureInAgent(left) && !forSureInAgent(right) {
		return -1
	}
	if !forSureInAgent(left) && forSureInAgent(right) {
		return 1
	}

	// If none of the heuristics above work out, we default to a basic LRU cache, keeping the most recently accessed files.
	return int(left.accessTime.Sub(right.accessTime))
}

// A file must be downloaded by a consumer if the diff is > 1s,
// as to trigger a download an 202 HTTP is made to the origin,
// after which the consumer backs off for at least 1s before trying to download again.
// During the 1s+ backoff, the access time and download time are almost the same.
func isDownloadedByConsumer(f fInfo) bool {
	return accessDownloadDiff(f) > 1*time.Second
}

func accessDownloadDiff(f fInfo) time.Duration {
	return f.downloadTime.Sub(f.accessTime).Abs()
}

// If a file is accessed long after it has been downloaded,
// it must have gotten prefetched by proxy earlier and now an agent consumed it.
func forSureInAgent(f fInfo) bool {
	return accessDownloadDiff(f) > 45*time.Minute
}

// cleanup cleans op from idle or expired files and returns its size BEFORE cleanup.
// It works in one of two possible modes:
//  1. tti + ttl based cleanup - the default.
//  2. aggressive cleanup - triggered on high disk usage. By default, it is ttl- and threshold-based.
//     However, it can also be custom policy- and threshold-based, when a `customPolicy` and a `config.AggressiveLowerThreshold` are provided.
//     Then the cache is cleaned until the lower threshold is reached, prioritizing blobs for deletion based on the `customPolicy`, which is a fn passed to [slices.SortFunc].
func (m *cleanupManager) cleanup(op base.FileOp, config CleanupConfig, customPolicy func(a, b fInfo) int) (usage int64, err error) {
	shouldAggro := m.shouldAggro(op, config, diskspaceutil.Usage)
	customPolicyBasedCleanup := shouldAggro && customPolicy != nil && config.AggressiveLowerThreshold != 0

	if customPolicyBasedCleanup {
		return m.customPolicyBasedCleanup(op, config, customPolicy, diskspaceutil.Usage)
	}

	ttl := config.TTL
	lowerThreshold := 0
	if shouldAggro {
		ttl = config.AggressiveTTL
		lowerThreshold = config.AggressiveLowerThreshold
	}

	return m.ttlBasedCleanup(op, config.TTI, ttl, lowerThreshold, diskspaceutil.Usage)
}

func (m *cleanupManager) customPolicyBasedCleanup(op base.FileOp, config CleanupConfig, customPolicy func(a, b fInfo) int, diskUsageFn diskUsageFn) (usage int64, err error) {
	names, err := op.ListNames()
	if err != nil {
		return 0, fmt.Errorf("list names: %s", err)
	}

	var fInfos []fInfo
	var totalUsage int64
	for _, name := range names {
		fStat, err := op.GetFileStat(name)
		if err != nil {
			log.With("name", name).Errorf("Error getting file stat: %s", err)
			continue
		}
		fInfo := fInfo{
			name:         name,
			downloadTime: fStat.ModTime(),
			size:         fStat.Size(),
		}
		totalUsage += fStat.Size()

		var accessTime metadata.LastAccessTime
		err = op.GetFileMetadata(name, &accessTime)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			log.With("name", name).Errorf("Error getting file metadata: %s", err)
			continue
		}
		fInfo.accessTime = accessTime.Time
		fInfos = append(fInfos, fInfo)
	}

	slices.SortFunc(fInfos, customPolicy)

	dInfo, err := diskUsageFn()
	if err != nil {
		return 0, fmt.Errorf("get disk usage info %s: %s", op, err)
	}

	minBytes := dInfo.TotalBytes * uint64(config.AggressiveLowerThreshold) / 100

	remainDeleteBytes := int64(dInfo.TotalBytes) - int64(minBytes)
	for _, file := range fInfos {
		if remainDeleteBytes <= 0 {
			break
		}
		err := op.DeleteFile(file.name)
		if err != nil && err != base.ErrFilePersisted {
			log.With("name", file.name).Errorf("Error deleting expired file: %s", err)
		}
		if err == nil {
			remainDeleteBytes -= file.size
		}
	}
	return totalUsage, nil
}

func (m *cleanupManager) ttlBasedCleanup(
	op base.FileOp, tti time.Duration, ttl time.Duration, aggroUtilLowerThreshold int, diskUsageFn diskUsageFn) (scannedBytes int64, err error) {

	var lowThresholdBytes uint64 = 0
	respectLowThreshold := false
	var dInfo diskspaceutil.UsageInfo
	if aggroUtilLowerThreshold != 0 {
		dInfo, err = diskUsageFn()
		if err != nil {
			log.Errorf("Error getting disk usage info %s: %s", op, err)
		} else {
			respectLowThreshold = true
			lowThresholdBytes = (dInfo.TotalBytes * uint64(aggroUtilLowerThreshold)) / 100
		}
	}

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
		ready, err := m.readyForDeletion(op, name, info, tti, ttl)
		if err != nil {
			log.With("name", name).Errorf("Error checking if file expired: %s", err)
		}

		lowThresholdBreached := respectLowThreshold && ((dInfo.UsedBytes - uint64(scannedBytes)) <= lowThresholdBytes)
		if ready && !lowThresholdBreached {
			if err := op.DeleteFile(name); err != nil && err != base.ErrFilePersisted {
				log.With("name", name).Errorf("Error deleting expired file: %s", err)
			}
		}
		scannedBytes += info.Size()
	}
	return scannedBytes, nil
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

func (m *cleanupManager) shouldAggro(op base.FileOp, config CleanupConfig, diskUsageFn diskUsageFn) bool {
	if config.AggressiveThreshold == 0 {
		return false
	}

	diskUsage, err := diskUsageFn()
	if err != nil {
		log.Errorf("Error getting disk usage info %s: %s", op, err)
		return false
	}
	if diskUsage.Util >= config.AggressiveThreshold {
		log.Warnf("Aggressive cleanup of %s triggers with disk space util %d", op, diskUsage.Util)
		m.stats.Counter("aggro_gc_runs").Inc(1)
		return true
	}
	return false
}
