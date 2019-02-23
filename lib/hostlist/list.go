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
package hostlist

import (
	"fmt"
	"net"
	"os"
	"strings"
	"sync"

	"github.com/uber/kraken/utils/dedup"
	"github.com/uber/kraken/utils/log"
	"github.com/uber/kraken/utils/stringset"

	"github.com/andres-erbsen/clock"
)

// List defines a list of addresses which is subject to change.
type List interface {
	Resolve() stringset.Set
}

type list struct {
	resolver resolver

	snapshotTrap *dedup.IntervalTrap

	mu       sync.RWMutex
	snapshot stringset.Set
}

// New creates a new List.
//
// An error is returned if a DNS record is supplied and resolves to an empty list
// of addresses.
//
// If List is backed by DNS, it will be periodically refreshed (defined by TTL
// in config). If, after construction, there is an error resolving DNS, the
// latest successful snapshot is used. As such, Resolve never returns an empty
// set.
func New(config Config) (List, error) {
	config.applyDefaults()

	resolver, err := config.getResolver()
	if err != nil {
		return nil, fmt.Errorf("invalid config: %s", err)
	}

	l := &list{resolver: resolver}
	l.snapshotTrap = dedup.NewIntervalTrap(config.TTL, clock.New(), &snapshotTask{l})

	if err := l.takeSnapshot(); err != nil {
		// Fail fast if a snapshot cannot be initialized.
		return nil, err
	}
	return l, nil
}

func (l *list) Resolve() stringset.Set {
	l.snapshotTrap.Trap()

	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.snapshot.Copy()
}

type snapshotTask struct {
	list *list
}

func (t *snapshotTask) Run() {
	if err := t.list.takeSnapshot(); err != nil {
		log.With("source", t.list.resolver).Errorf("Error taking hostlist snapshot: %s", err)
	}
}

func (l *list) takeSnapshot() error {
	snapshot, err := l.resolver.resolve()
	if err != nil {
		return err
	}
	l.mu.Lock()
	l.snapshot = snapshot
	l.mu.Unlock()
	return nil
}

type nonLocalList struct {
	list       List
	localAddrs stringset.Set
}

// StripLocal wraps a List and filters out the local machine, if present. The
// local machine is identified by both its hostname and ip address, concatenated
// with port.
//
// If the local machine is the only member of list, then Resolve returns an empty
// set.
func StripLocal(list List, port int) (List, error) {
	localNames, err := getLocalNames()
	if err != nil {
		return nil, fmt.Errorf("get local names: %s", err)
	}
	localAddrs, err := attachPortIfMissing(localNames, port)
	if err != nil {
		return nil, fmt.Errorf("attach port to local names: %s", err)
	}
	return &nonLocalList{list, localAddrs}, nil
}

func (l *nonLocalList) Resolve() stringset.Set {
	return l.list.Resolve().Sub(l.localAddrs)
}

func getLocalNames() (stringset.Set, error) {
	result := make(stringset.Set)

	// Add all local non-loopback ips.
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil, fmt.Errorf("interfaces: %s", err)
	}
	for _, i := range ifaces {
		addrs, err := i.Addrs()
		if err != nil {
			return nil, fmt.Errorf("addrs of %v: %s", i, err)
		}
		for _, addr := range addrs {
			ip := net.ParseIP(addr.String()).To4()
			if ip == nil {
				continue
			}
			result.Add(ip.String())
		}
	}

	// Add local hostname just to be safe.
	hostname, err := os.Hostname()
	if err != nil {
		return nil, fmt.Errorf("hostname: %s", err)
	}
	result.Add(hostname)

	return result, nil
}

func attachPortIfMissing(names stringset.Set, port int) (stringset.Set, error) {
	result := make(stringset.Set)
	for name := range names {
		parts := strings.Split(name, ":")
		switch len(parts) {
		case 1:
			// Name is in 'host' format -- attach port.
			name = fmt.Sprintf("%s:%d", parts[0], port)
		case 2:
			// No-op, name is already in "ip:port" format.
		default:
			return nil, fmt.Errorf("invalid name format: %s, expected 'host' or 'ip:port'", name)
		}
		result.Add(name)
	}
	return result, nil
}
