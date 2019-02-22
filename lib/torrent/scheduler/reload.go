// Copyright (c) 2019 Uber Technologies, Inc.
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
package scheduler

import (
	"sync"

	"github.com/uber/kraken/lib/torrent/scheduler/announcequeue"
	"github.com/uber/kraken/utils/log"
)

// ReloadableScheduler is a Scheduler which supports reloadable configuration.
type ReloadableScheduler interface {
	Scheduler
	Reload(config Config)
}

type reloadableScheduler struct {
	*scheduler
	mu sync.Mutex // Protects reloading Scheduler.
	aq func() announcequeue.Queue
}

func makeReloadable(s *scheduler, aq func() announcequeue.Queue) *reloadableScheduler {
	return &reloadableScheduler{scheduler: s, aq: aq}
}

// Reload restarts the Scheduler with new configuration. Panics if the Scheduler
// fails to restart.
func (rs *reloadableScheduler) Reload(config Config) {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	s := rs.scheduler
	s.Stop()
	n, err := newScheduler(
		config, s.torrentArchive, s.stats, s.pctx, s.announceClient, rs.aq(), s.netevents)
	if err != nil {
		// Totally unrecoverable error -- rs.scheduler is now stopped and unusable,
		// so let process die and restart with original config.
		log.Fatalf("Failed to reload scheduler config: %s", err)
	}
	rs.scheduler = n
}
