package hostlist

import (
	"sync"

	"code.uber.internal/infra/kraken/utils/dedup"
	"code.uber.internal/infra/kraken/utils/stringset"
	"github.com/andres-erbsen/clock"
)

// List defines a list of hosts, which is subject to change.
type List struct {
	config Config
	port   int

	snapshotTrap *dedup.IntervalTrap

	mu       sync.RWMutex
	snapshot stringset.Set
	err      error
}

// New creates a new List.
//
// List can be resolved into a set of addresses in 'ip:port' format. List is very
// flexible in what host strings are accepted. Names missing a port suffix will
// have the provided port attached. Hosts with a port suffix will be untouched.
// Either ip addresses or host names are allowed.
//
// List also strips the local machine from the resolved address list, if present.
// The local machine is identified by both its hostname and ip address, concatenated
// with the provided port.
//
// An error is returned if a DNS record is supplied and resolves to an empty list
// of addresses.
//
// List caches resolved DNS records for the configured TTL.
func New(config Config, port int) (*List, error) {
	config.applyDefaults()

	l := &List{
		config: config,
		port:   port,
	}
	l.snapshotTrap = dedup.NewIntervalTrap(config.TTL, clock.New(), &snapshotTask{l})

	l.takeSnapshot()
	if l.err != nil {
		// Fail fast if a snapshot cannot be initialized.
		return nil, l.err
	}
	return l, nil
}

// Resolve returns a snapshot of l.
func (l *List) Resolve() (stringset.Set, error) {
	l.snapshotTrap.Trap()

	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.snapshot, l.err
}

type snapshotTask struct {
	list *List
}

func (t *snapshotTask) Run() {
	t.list.takeSnapshot()
}

func (l *List) takeSnapshot() {
	snapshot, err := l.config.snapshot(l.port)
	l.mu.Lock()
	l.snapshot = snapshot
	l.err = err
	l.mu.Unlock()
}
