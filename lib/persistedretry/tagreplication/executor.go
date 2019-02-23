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
	"time"

	"github.com/uber/kraken/build-index/tagclient"
	"github.com/uber/kraken/lib/persistedretry"
	"github.com/uber/kraken/origin/blobclient"

	"github.com/uber-go/tally"
)

// Executor executes tag replication tasks.
type Executor struct {
	stats             tally.Scope
	originCluster     blobclient.ClusterClient
	tagClientProvider tagclient.Provider
}

// NewExecutor creates a new Executor.
func NewExecutor(
	stats tally.Scope,
	originCluster blobclient.ClusterClient,
	tagClientProvider tagclient.Provider) *Executor {

	stats = stats.Tagged(map[string]string{
		"module": "tagreplicationexecutor",
	})

	return &Executor{stats, originCluster, tagClientProvider}
}

// Name returns the executor name.
func (e *Executor) Name() string {
	return "tagreplication"
}

// Exec replicates a tag's blob dependencies to the task's remote origin
// cluster, then replicates the tag to the remote build-index.
func (e *Executor) Exec(r persistedretry.Task) error {
	t := r.(*Task)
	start := time.Now()
	remoteTagClient := e.tagClientProvider.Provide(t.Destination)

	if ok, err := remoteTagClient.Has(t.Tag); err == nil && ok {
		// Remote index already has the tag, therefore dependencies have already
		// been replicated, and the remote has also replicated the tag. No-op.
		return nil
	}

	remoteOrigin, err := remoteTagClient.Origin()
	if err != nil {
		return fmt.Errorf("lookup remote origin cluster: %s", err)
	}
	for _, d := range t.Dependencies {
		if err := e.originCluster.ReplicateToRemote(t.Tag, d, remoteOrigin); err != nil {
			return fmt.Errorf("origin cluster replicate: %s", err)
		}
	}

	// Put tag and triggers replication on the remote client.
	// Replication will call Exec n^2 times but some will return early
	// if remote has the tag already.
	if err := remoteTagClient.PutAndReplicate(t.Tag, t.Digest); err != nil {
		return fmt.Errorf("put and replicate tag: %s", err)
	}

	// We don't want to time noops nor errors.
	e.stats.Timer("replicate").Record(time.Since(start))
	e.stats.Timer("lifetime").Record(time.Since(t.CreatedAt))

	return nil
}
