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
package trackerserver

import (
	"time"

	"github.com/uber/kraken/utils/listener"
)

// Config defines configuration for the tracker service.
type Config struct {
	// Limits the number of unique metainfo requests to origin per namespace/digest.
	GetMetaInfoLimit time.Duration `yaml:"get_metainfo_limit"`

	// Limits the number of peers returned on each announce.
	PeerHandoutLimit int `yaml:"announce_limit"`

	AnnounceInterval time.Duration `yaml:"announce_interval"`

	Listener listener.Config `yaml:"listener"`
}

func (c Config) applyDefaults() Config {
	if c.GetMetaInfoLimit == 0 {
		c.GetMetaInfoLimit = time.Second
	}
	if c.PeerHandoutLimit == 0 {
		c.PeerHandoutLimit = 50
	}
	if c.AnnounceInterval == 0 {
		c.AnnounceInterval = 3 * time.Second
	}
	return c
}
