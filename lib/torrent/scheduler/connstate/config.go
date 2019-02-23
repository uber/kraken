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
package connstate

import "time"

// Config defines State configuration.
type Config struct {

	// MaxOpenConnectionsPerTorrent is the maximum number of connections which a
	// Scheduler will maintain at once for each torrent.
	MaxOpenConnectionsPerTorrent int `yaml:"max_open_conn"`

	// MaxMutualConnections is the maximum number of mutual connections a peer
	// can have and still connect with us.
	MaxMutualConnections int `yaml:"max_mutual_conn"`

	// DisableBlacklist disables the blacklisting of peers. Should only be used
	// for testing purposes.
	DisableBlacklist bool `yaml:"disable_blacklist"`

	// BlacklistDuration is the duration a connection will remain blacklisted.
	BlacklistDuration time.Duration `yaml:"blacklist_duration"`
}

func (c Config) applyDefaults() Config {
	if c.MaxOpenConnectionsPerTorrent == 0 {
		c.MaxOpenConnectionsPerTorrent = 10
	}
	// Defaults to no mutual connection limit.
	if c.MaxMutualConnections == 0 {
		c.MaxMutualConnections = c.MaxOpenConnectionsPerTorrent
	}
	if c.BlacklistDuration == 0 {
		c.BlacklistDuration = 30 * time.Second
	}
	return c
}
