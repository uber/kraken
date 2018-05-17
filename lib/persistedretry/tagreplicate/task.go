package tagreplicate

import (
	"fmt"
	"time"

	"github.com/uber-go/tally"

	"code.uber.internal/infra/kraken/build-index/tagclient"
	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/persistedretry"
	"code.uber.internal/infra/kraken/origin/blobclient"
)

// State defines the state of a task
type State string

const (
	// Pending means the task is yet to be executed.
	Pending State = "pending"
	// Failed means that task has failed.
	Failed State = "failed"
)

var _ persistedretry.Task = (*Task)(nil)

// Task contains information to replicate a tag.
type Task struct {
	Name         string          `db:"name"`
	Destination  string          `db:"destination"`
	Digest       core.Digest     `db:"digest"`
	Dependencies core.DigestList `db:"dependencies"`
	CreatedAt    time.Time       `db:"created_at"`
	LastAttempt  time.Time       `db:"last_attempt"`
	State        State           `db:"state"`
	Failures     int             `db:"failures"`

	stats             tally.Scope
	localOriginClient blobclient.ClusterClient
	remoteTagProvider tagclient.Provider
}

// NewTask creates a new replication tag given name, destination and digest.
func NewTask(
	localOriginClient blobclient.ClusterClient,
	remoteTagProvider tagclient.Provider,
	stats tally.Scope,
	name, destination string,
	digest core.Digest,
	deps ...core.Digest) *Task {
	stats = stats.Tagged(map[string]string{
		"module": "tagreplicatetask",
	})

	return &Task{
		Name:              name,
		Destination:       destination,
		Digest:            digest,
		Dependencies:      deps,
		CreatedAt:         time.Now(),
		State:             Pending,
		LastAttempt:       time.Now(),
		stats:             stats,
		localOriginClient: localOriginClient,
		remoteTagProvider: remoteTagProvider,
	}
}

func (t *Task) String() string {
	return fmt.Sprintf("tagreplicate.Task(name=%s, destination=%s)", t.Name, t.Destination)
}

// Run replicates a tag to a remote cluster.
func (t *Task) Run() error {
	remoteTagClient := t.remoteTagProvider.Provide(t.Destination)
	remoteOrigin, err := remoteTagClient.Origin()
	if err != nil {
		t.stats.Counter("lookup_failure").Inc(1)
		return fmt.Errorf("lookup remote origin cluster: %s", err)
	}

	for _, d := range t.Dependencies {
		if err := t.localOriginClient.ReplicateToRemote(t.Name, d, remoteOrigin); err != nil {
			t.stats.Counter("blob_failure").Inc(1)
			return fmt.Errorf("origin cluster replicate: %s", err)
		}
	}

	if err := remoteTagClient.Put(t.Name, t.Digest); err != nil {
		t.stats.Counter("tag_failure").Inc(1)
		return fmt.Errorf("put tag: %s", err)
	}

	return nil
}
