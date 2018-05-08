package remotes

import (
	"fmt"
	"regexp"
	"sync"

	"code.uber.internal/infra/kraken/build-index/tagclient"
	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/origin/blobclient"
	"code.uber.internal/infra/kraken/utils/errutil"
)

// Config defines remote replication configuration which specifies which
// build-indexes certain namespaces should be replicated to.
//
// For example, given the configuration:
//
//   uber-usi/.*:
//     - build-index-sjc1
//     - build-index-dca1
//
// Any builds matching the uber-usi/.* namespace should be replicated to sjc1 and
// dca1 build-indexes.
type Config map[string][]string

type remote struct {
	regexp *regexp.Regexp
	addr   string
	client tagclient.Client
}

// Replicator provides build replication to remote build-indexes.
type Replicator interface {
	Replicate(tag string, d core.Digest, dependencies []core.Digest) error
}

type replicator struct {
	originCluster blobclient.ClusterClient
	remotes       []*remote
}

// New creates a new Replicator.
func New(
	config Config,
	originCluster blobclient.ClusterClient,
	provider tagclient.Provider) (Replicator, error) {

	var remotes []*remote
	for ns, addrs := range config {
		re, err := regexp.Compile(ns)
		if err != nil {
			return nil, fmt.Errorf("regexp compile namespace %s: %s", ns, err)
		}
		for _, addr := range addrs {
			r := &remote{re, addr, provider.Provide(addr)}
			remotes = append(remotes, r)
		}
	}
	return &replicator{originCluster, remotes}, nil
}

// Replicate replicates tag / digest and its blob dependencies to all matching
// remotes. Tag is only pushed to the remote if all the blob dependencies are
// successfully replicated first. If any single replication to a remote fails,
// the entire operation fails.
func (r *replicator) Replicate(tag string, d core.Digest, dependencies []core.Digest) error {
	var mu sync.Mutex
	var errs []error
	var wg sync.WaitGroup
	for _, rem := range r.remotes {
		if rem.regexp.MatchString(tag) {
			wg.Add(1)
			go func(rem *remote) {
				defer wg.Done()
				if err := r.replicateToRemote(rem, tag, d, dependencies); err != nil {
					mu.Lock()
					errs = append(errs, fmt.Errorf("replicate to %s: %s", rem.addr, err))
					mu.Unlock()
				}
			}(rem)
		}
	}
	wg.Wait()
	return errutil.Join(errs)
}

func (r *replicator) replicateToRemote(
	rem *remote, tag string, d core.Digest, dependencies []core.Digest) error {

	remoteOrigin, err := rem.client.Origin()
	if err != nil {
		return fmt.Errorf("lookup remote origin cluster: %s", err)
	}
	for _, d := range dependencies {
		if err := r.originCluster.ReplicateToRemote(tag, d, remoteOrigin); err != nil {
			return fmt.Errorf("origin cluster replicate: %s", err)
		}
	}
	if err := rem.client.Put(tag, d); err != nil {
		return fmt.Errorf("put tag: %s", err)
	}
	return nil
}
