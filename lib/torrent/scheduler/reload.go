package scheduler

import (
	"sync"

	"code.uber.internal/infra/kraken/lib/torrent/scheduler/announcequeue"
	"code.uber.internal/infra/kraken/utils/log"
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
