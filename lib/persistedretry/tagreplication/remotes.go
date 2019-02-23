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
package tagreplication

import (
	"fmt"
	"regexp"
)

// RemoteValidator validates remotes.
type RemoteValidator interface {
	Valid(tag, addr string) bool
}

// Remote represents a remote build-index.
type Remote struct {
	regexp *regexp.Regexp
	addr   string
}

// Remotes represents all namespaces and their configured remote build-indexes.
type Remotes []*Remote

// Match returns all matched remotes for a tag.
func (rs Remotes) Match(tag string) (addrs []string) {
	for _, r := range rs {
		if r.regexp.MatchString(tag) {
			addrs = append(addrs, r.addr)
		}
	}
	return addrs
}

// Valid returns true if tag matches to addr.
func (rs Remotes) Valid(tag, addr string) bool {
	for _, a := range rs.Match(tag) {
		if a == addr {
			return true
		}
	}
	return false
}

// RemotesConfig defines remote replication configuration which specifies which
// namespaces should be replicated to certain build-indexes.
//
// For example, given the configuration:
//
//   build-index-zone1:
//   - namespace_foo/.*
//
//   build-index-zone2:
//   - namespace_foo/.*
//
// Any builds matching the namespace_foo/.* namespace should be replicated to
// zone1 and zone2 build-indexes.
type RemotesConfig map[string][]string

// Build builds configuration into Remotes.
func (c RemotesConfig) Build() (Remotes, error) {
	var remotes Remotes
	for addr, namespaces := range c {
		for _, ns := range namespaces {
			re, err := regexp.Compile(ns)
			if err != nil {
				return nil, fmt.Errorf("regexp compile namespace %s: %s", ns, err)
			}
			remotes = append(remotes, &Remote{re, addr})
		}
	}
	return remotes, nil
}
