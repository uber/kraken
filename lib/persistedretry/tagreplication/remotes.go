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
