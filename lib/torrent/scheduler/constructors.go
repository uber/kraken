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
package scheduler

import (
	"crypto/tls"
	"fmt"

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
	trackers hashring.PassiveRing,
	tls *tls.Config) (ReloadableScheduler, error) {

	s, err := newScheduler(
		config,
		agentstorage.NewTorrentArchive(stats, cads, metainfoclient.New(trackers, tls)),
		stats,
		pctx,
		announceclient.New(pctx, trackers, tls),
		netevents)
	if err != nil {
		return nil, fmt.Errorf("new scheduler: %s", err)
	}

	aq := func() announcequeue.Queue { return announcequeue.New() }
	rs := makeReloadable(s, aq)
	if err := rs.start(aq()); err != nil {
		return nil, fmt.Errorf("start: %s", err)
	}

	return rs, nil
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
		netevents)
	if err != nil {
		return nil, err
	}

	aq := func() announcequeue.Queue { return announcequeue.Disabled() }
	rs := makeReloadable(s, aq)
	if err := rs.start(aq()); err != nil {
		return nil, fmt.Errorf("start: %s", err)
	}

	return rs, nil
}
