package scheduler

import (
	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/blobrefresh"
	"code.uber.internal/infra/kraken/lib/serverset"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/lib/torrent/networkevent"
	"code.uber.internal/infra/kraken/lib/torrent/scheduler/announcequeue"
	"code.uber.internal/infra/kraken/lib/torrent/storage/agentstorage"
	"code.uber.internal/infra/kraken/lib/torrent/storage/originstorage"
	"code.uber.internal/infra/kraken/tracker/announceclient"
	"code.uber.internal/infra/kraken/tracker/metainfoclient"

	"github.com/uber-go/tally"
)

// NewAgentScheduler creates and starts a ReloadableScheduler configured for an agent.
func NewAgentScheduler(
	config Config,
	stats tally.Scope,
	pctx core.PeerContext,
	fs store.FileStore,
	netevents networkevent.Producer,
	trackers serverset.Set) (ReloadableScheduler, error) {

	s, err := newScheduler(
		config,
		agentstorage.NewTorrentArchive(stats, fs, metainfoclient.Default(trackers)),
		stats,
		pctx,
		announceclient.New(pctx, trackers),
		announcequeue.New(),
		netevents)
	if err != nil {
		return nil, err
	}
	return makeReloadable(s), nil
}

// NewOriginScheduler creates and starts a ReloadableScheduler configured for an origin.
func NewOriginScheduler(
	config Config,
	stats tally.Scope,
	pctx core.PeerContext,
	fs store.OriginFileStore,
	netevents networkevent.Producer,
	blobRefresher *blobrefresh.Refresher) (ReloadableScheduler, error) {

	s, err := newScheduler(
		config,
		originstorage.NewTorrentArchive(fs, blobRefresher),
		stats,
		pctx,
		announceclient.Disabled(),
		announcequeue.Disabled(),
		netevents)
	if err != nil {
		return nil, err
	}
	return makeReloadable(s), nil
}
