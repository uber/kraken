package tagreplicate

import (
	"errors"
	"fmt"
	"regexp"

	"github.com/uber-go/tally"

	"code.uber.internal/infra/kraken/build-index/tagclient"
	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/origin/blobclient"
)

// ErrMatchingRemoteNotFound is returned when the tag should not be replicated to any remote.
var ErrMatchingRemoteNotFound = errors.New("matching remote not found")

// TaskGenerator defines a interface to generate task.
type TaskGenerator interface {
	Create(tag string, d core.Digest, deps ...core.Digest) ([]*Task, error)
	Load(*Task) error
	IsValid(task Task) bool
}

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
}

type taskgenerator struct {
	stats             tally.Scope
	originCluster     blobclient.ClusterClient
	removeTagProvider tagclient.Provider
	remotes           []*remote
}

// NewTaskGenerator creates a TaskGenerator.
func NewTaskGenerator(
	config Config,
	stats tally.Scope,
	originCluster blobclient.ClusterClient,
	remoteTagProvider tagclient.Provider) (TaskGenerator, error) {
	var remotes []*remote
	for ns, addrs := range config {
		re, err := regexp.Compile(ns)
		if err != nil {
			return nil, fmt.Errorf("regexp compile namespace %s: %s", ns, err)
		}
		for _, addr := range addrs {
			r := &remote{re, addr}
			remotes = append(remotes, r)
		}
	}
	return &taskgenerator{stats, originCluster, remoteTagProvider, remotes}, nil
}

func (g *taskgenerator) Create(tag string, d core.Digest, deps ...core.Digest) ([]*Task, error) {
	var tasks []*Task
	for _, rem := range g.remotes {
		if rem.regexp.MatchString(tag) {
			tasks = append(tasks, NewTask(g.originCluster, g.removeTagProvider, g.stats, tag, rem.addr, d, deps...))
		}
	}
	if len(tasks) == 0 {
		return nil, ErrMatchingRemoteNotFound
	}

	return tasks, nil
}

// Load mutates the task by injecting stats, clusterclient and tagprovider.
// This is used for loading task from store.
func (g *taskgenerator) Load(task *Task) error {
	for _, rem := range g.remotes {
		if rem.regexp.MatchString(task.Name) && rem.addr == task.Destination {
			task.stats = g.stats
			task.localOriginClient = g.originCluster
			task.remoteTagProvider = g.removeTagProvider
			return nil
		}
	}
	return ErrMatchingRemoteNotFound
}

func (g *taskgenerator) IsValid(task Task) bool {
	for _, rem := range g.remotes {
		if rem.regexp.MatchString(task.Name) && rem.addr == task.Destination {
			return true
		}
	}

	return false
}
