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
package originstore

import (
	"fmt"
	"time"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/hostlist"
	"github.com/uber/kraken/origin/blobclient"
	"github.com/uber/kraken/utils/dedup"
	"github.com/uber/kraken/utils/errutil"
	"github.com/uber/kraken/utils/log"

	"github.com/andres-erbsen/clock"
)

type allUnavailableError struct {
	error
}

// Store is a local cache in front of the origin cluster which is resilient to
// origin unavailability.
type Store interface {
	// GetOrigins returns all available origins seeding d. Returns error if all origins
	// are unavailable.
	GetOrigins(d core.Digest) ([]*core.PeerInfo, error)
}

type store struct {
	config       Config
	origins      hostlist.List
	provider     blobclient.Provider
	locations    *dedup.Limiter // Caches results for origin locations per digest.
	peerContexts *dedup.Limiter // Caches results for individual origin peer contexts.
}

// New creates a new Store.
func New(config Config, clk clock.Clock, origins hostlist.List, provider blobclient.Provider) Store {
	config.applyDefaults()
	s := &store{
		config:   config,
		origins:  origins,
		provider: provider,
	}
	s.locations = dedup.NewLimiter(clk, &locations{s})
	s.peerContexts = dedup.NewLimiter(clk, &peerContexts{s})
	return s
}

func (s *store) GetOrigins(d core.Digest) ([]*core.PeerInfo, error) {
	lr := s.locations.Run(d).(*locationsResult)
	if lr.err != nil {
		return nil, lr.err
	}

	var errs []error
	var origins []*core.PeerInfo
	for _, addr := range lr.addrs {
		pcr := s.peerContexts.Run(addr).(*peerContextResult)
		if pcr.err != nil {
			errs = append(errs, pcr.err)
		} else {
			origins = append(origins, core.PeerInfoFromContext(pcr.pctx, true))
		}
	}
	if len(origins) == 0 {
		return nil, allUnavailableError{fmt.Errorf("all origins unavailable: %s", errutil.Join(errs))}
	}
	return origins, nil
}

type locations struct {
	store *store
}

type locationsResult struct {
	addrs []string
	err   error
}

func (l *locations) Run(input interface{}) (interface{}, time.Duration) {
	d := input.(core.Digest)
	addrs, err := blobclient.Locations(l.store.provider, l.store.origins, d)
	ttl := l.store.config.LocationsTTL
	if err != nil {
		ttl = l.store.config.LocationsErrorTTL
	}
	return &locationsResult{addrs, err}, ttl
}

type peerContexts struct {
	store *store
}

type peerContextResult struct {
	pctx core.PeerContext
	err  error
}

func (p *peerContexts) Run(input interface{}) (interface{}, time.Duration) {
	addr := input.(string)
	pctx, err := p.store.provider.Provide(addr).GetPeerContext()
	ttl := p.store.config.OriginContextTTL
	if err != nil {
		log.With("origin", addr).Errorf("Origin unavailable: %s", err)
		ttl = p.store.config.OriginUnavailableTTL
	}
	return &peerContextResult{pctx, err}, ttl
}
