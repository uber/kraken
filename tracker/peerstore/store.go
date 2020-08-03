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
package peerstore

import (
	"fmt"

	"github.com/andres-erbsen/clock"
	"github.com/uber/kraken/core"
	"github.com/uber/kraken/utils/log"
)

// Store provides storage for announcing peers.
type Store interface {
	// Close cleans up any Store resources.
	Close()

	// GetPeers returns at most n random peers announcing for h.
	GetPeers(h core.InfoHash, n int) ([]*core.PeerInfo, error)

	// UpdatePeer updates peer fields.
	UpdatePeer(h core.InfoHash, peer *core.PeerInfo) error
}

// New creates a new Store implementation based on config.
func New(config Config) (Store, error) {
	if config.Redis.Enabled {
		log.Info("Redis peer store enabled")
		s, err := NewRedisStore(config.Redis, clock.New())
		if err != nil {
			return nil, fmt.Errorf("new redis store: %s", err)
		}
		return s, nil
	}
	log.Info("Defaulting to local peer store")
	return NewLocalStore(config.Local, clock.New()), nil
}
