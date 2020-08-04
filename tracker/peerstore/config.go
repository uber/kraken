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
	"time"
)

// Config defines Store configuration.
//
// NOTE: By default, the LocalStore implementation is used. Redis configuration
// is ignored unless RedisConfig.Enabled is true.
type Config struct {
	Local LocalConfig `yaml:"local"`
	Redis RedisConfig `yaml:"redis"`
}

// LocalConfig defines LocalStore configuration.
type LocalConfig struct {
	TTL time.Duration `yaml:"ttl"`
}

func (c *LocalConfig) applyDefaults() {
	if c.TTL == 0 {
		c.TTL = 5 * time.Hour
	}
}

// RedisConfig defines RedisStore configuration.
// TODO(evelynl94): rename
type RedisConfig struct {
	Enabled           bool          `yaml:"enabled"`
	Addr              string        `yaml:"addr"`
	DialTimeout       time.Duration `yaml:"dial_timeout"`
	ReadTimeout       time.Duration `yaml:"read_timeout"`
	WriteTimeout      time.Duration `yaml:"write_timeout"`
	PeerSetWindowSize time.Duration `yaml:"peer_set_window_size"`
	MaxPeerSetWindows int           `yaml:"max_peer_set_windows"`
	MaxIdleConns      int           `yaml:"max_idle_conns"`
	MaxActiveConns    int           `yaml:"max_active_conns"`
	IdleConnTimeout   time.Duration `yaml:"idle_conn_timeout"`
}

func (c *RedisConfig) applyDefaults() {
	if c.DialTimeout == 0 {
		c.DialTimeout = 5 * time.Second
	}
	if c.ReadTimeout == 0 {
		c.ReadTimeout = 30 * time.Second
	}
	if c.WriteTimeout == 0 {
		c.WriteTimeout = 30 * time.Second
	}
	if c.PeerSetWindowSize == 0 {
		c.PeerSetWindowSize = time.Hour
	}
	if c.MaxPeerSetWindows == 0 {
		c.MaxPeerSetWindows = 5
	}
	if c.MaxIdleConns == 0 {
		c.MaxIdleConns = 10
	}
	if c.MaxActiveConns == 0 {
		c.MaxActiveConns = 500
	}
	if c.IdleConnTimeout == 0 {
		c.IdleConnTimeout = 60 * time.Second
	}
}
