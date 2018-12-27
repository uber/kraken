package scheduler

import (
	"crypto/tls"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/blobrefresh"
	"github.com/uber/kraken/lib/hashring"
	"github.com/uber/kraken/lib/store"
	"github.com/uber/kraken/lib/torrent/networkevent"
	"github.com/uber/kraken/lib/torrent/scheduler/announcequeue"
	"github.com/uber/kraken/lib/torrent/storage/agentstorage"
	"github.com/uber/kraken/lib/torrent/storage/originstorage"
	"github.com/uber/kraken/tracker/announceclient"
	"github.com/uber/kraken/tracker/metainfoclient"

	"github.com/uber-go/tally"
)

// NewAgentScheduler creates and starts a ReloadableScheduler configured for an agent.
func NewAgentScheduler(
	config Config,
	stats tally.Scope,
	pctx core.PeerContext,
	cads *store.CADownloadStore,
	netevents networkevent.Producer,
	trackers hashring.Ring,
	tls *tls.Config) (ReloadableScheduler, error) {

	s, err := newScheduler(
		config,
		agentstorage.NewTorrentArchive(stats, cads, metainfoclient.New(trackers, tls)),
		stats,
		pctx,
		announceclient.New(pctx, trackers, tls),
		announcequeue.New(),
		netevents)
	if err != nil {
		return nil, err
	}
	return makeReloadable(s, func() announcequeue.Queue { return announcequeue.New() }), nil
}

// NewOriginScheduler creates and starts a ReloadableScheduler configured for an origin.
func NewOriginScheduler(
	config Config,
	stats tally.Scope,
	pctx core.PeerContext,
	cas *store.CAStore,
	netevents networkevent.Producer,
	blobRefresher *blobrefresh.Refresher) (ReloadableScheduler, error) {

	s, err := newScheduler(
		config,
		originstorage.NewTorrentArchive(cas, blobRefresher),
		stats,
		pctx,
		announceclient.Disabled(),
		announcequeue.Disabled(),
		netevents)
	if err != nil {
		return nil, err
	}
	return makeReloadable(s, func() announcequeue.Queue { return announcequeue.Disabled() }), nil
}
