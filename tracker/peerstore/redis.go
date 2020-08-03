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
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/utils/log"
	"github.com/uber/kraken/utils/randutil"

	"github.com/andres-erbsen/clock"
	"github.com/garyburd/redigo/redis"
)

func peerSetKey(h core.InfoHash, window int64) string {
	return fmt.Sprintf("peerset:%s:%d", h.String(), window)
}

func serializePeer(p *core.PeerInfo) string {
	var completeBit int
	if p.Complete {
		completeBit = 1
	}
	return fmt.Sprintf("%s:%s:%d:%d", p.PeerID.String(), p.IP, p.Port, completeBit)
}

type peerIdentity struct {
	peerID core.PeerID
	ip     string
	port   int
}

func deserializePeer(s string) (id peerIdentity, complete bool, err error) {
	parts := strings.Split(s, ":")
	if len(parts) != 4 {
		return id, false, fmt.Errorf("invalid peer encoding: expected 'pid:ip:port:complete'")
	}
	peerID, err := core.NewPeerID(parts[0])
	if err != nil {
		return id, false, fmt.Errorf("parse peer id: %s", err)
	}
	ip := parts[1]
	port, err := strconv.Atoi(parts[2])
	if err != nil {
		return id, false, fmt.Errorf("parse port: %s", err)
	}
	id = peerIdentity{peerID, ip, port}
	complete = parts[3] == "1"
	return id, complete, nil
}

// RedisStore is a Store backed by Redis.
type RedisStore struct {
	config RedisConfig
	pool   *redis.Pool
	clk    clock.Clock
}

// NewRedisStore creates a new RedisStore.
func NewRedisStore(config RedisConfig, clk clock.Clock) (*RedisStore, error) {
	config.applyDefaults()

	if config.Addr == "" {
		return nil, errors.New("invalid config: missing addr")
	}

	s := &RedisStore{
		config: config,
		pool: &redis.Pool{
			Dial: func() (redis.Conn, error) {
				// TODO Add options
				return redis.Dial(
					"tcp",
					config.Addr,
					redis.DialConnectTimeout(config.DialTimeout),
					redis.DialReadTimeout(config.ReadTimeout),
					redis.DialWriteTimeout(config.WriteTimeout))
			},
			MaxIdle:     config.MaxIdleConns,
			MaxActive:   config.MaxActiveConns,
			IdleTimeout: config.IdleConnTimeout,
			Wait:        true,
		},
		clk: clk,
	}

	// Ensure we can connect to Redis.
	c, err := s.pool.Dial()
	if err != nil {
		return nil, fmt.Errorf("dial redis: %s", err)
	}
	c.Close()

	return s, nil
}

// Close implements Store.
func (s *RedisStore) Close() {}

func (s *RedisStore) curPeerSetWindow() int64 {
	t := s.clk.Now().Unix()
	return t - (t % int64(s.config.PeerSetWindowSize.Seconds()))
}

func (s *RedisStore) peerSetWindows() []int64 {
	cur := s.curPeerSetWindow()
	ws := make([]int64, s.config.MaxPeerSetWindows)
	for i := range ws {
		ws[i] = cur - int64(i)*int64(s.config.PeerSetWindowSize.Seconds())
	}
	return ws
}

// UpdatePeer writes p to Redis with a TTL.
func (s *RedisStore) UpdatePeer(h core.InfoHash, p *core.PeerInfo) error {
	c := s.pool.Get()
	defer c.Close()

	w := s.curPeerSetWindow()
	expireAt := w + int64(s.config.PeerSetWindowSize.Seconds())*int64(s.config.MaxPeerSetWindows)

	// Add p to the current window.
	k := peerSetKey(h, w)

	if err := c.Send("SADD", k, serializePeer(p)); err != nil {
		return fmt.Errorf("send SADD: %s", err)
	}
	if err := c.Send("EXPIREAT", k, expireAt); err != nil {
		return fmt.Errorf("send EXPIREAT: %s", err)
	}
	if err := c.Flush(); err != nil {
		return fmt.Errorf("flush: %s", err)
	}
	if _, err := c.Receive(); err != nil {
		return fmt.Errorf("SADD: %s", err)
	}
	if _, err := c.Receive(); err != nil {
		return fmt.Errorf("EXPIREAT: %s", err)
	}
	return nil
}

// GetPeers returns at most n PeerInfos associated with h.
func (s *RedisStore) GetPeers(h core.InfoHash, n int) ([]*core.PeerInfo, error) {
	c := s.pool.Get()
	defer c.Close()

	// Try to sample n peers from each window in randomized order until we have
	// collected n distinct peers. This achieves random sampling across multiple
	// windows.
	// TODO(codyg): One limitation of random window sampling is we're no longer
	// guaranteed to include the latest completion bits. A simple way to mitigate
	// this is to decrease the number of windows.
	windows := s.peerSetWindows()
	randutil.ShuffleInt64s(windows)

	// Eliminate duplicates from other windows and collapses complete bits.
	selected := make(map[peerIdentity]bool)

	for i := 0; len(selected) < n && i < len(windows); i++ {
		k := peerSetKey(h, windows[i])
		result, err := redis.Strings(c.Do("SRANDMEMBER", k, n-len(selected)))
		if err == redis.ErrNil {
			continue
		} else if err != nil {
			return nil, err
		}
		for _, s := range result {
			id, complete, err := deserializePeer(s)
			if err != nil {
				log.Errorf("Error deserializing peer %q: %s", s, err)
				continue
			}
			selected[id] = selected[id] || complete
		}
	}

	var peers []*core.PeerInfo
	for id, complete := range selected {
		p := core.NewPeerInfo(id.peerID, id.ip, id.port, false, complete)
		peers = append(peers, p)
	}
	return peers, nil
}
