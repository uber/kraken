package hostlist

import (
	"fmt"
	"net"
	"os"
	"strings"
	"sync"

	"code.uber.internal/infra/kraken/utils/dedup"
	"code.uber.internal/infra/kraken/utils/stringset"

	"github.com/andres-erbsen/clock"
)

// List defines a list of hosts, which is subject to change and cached with a TTL.
type List struct {
	config Config
	port   int

	localAddrs stringset.Set

	snapshotTrap *dedup.IntervalTrap

	mu       sync.RWMutex
	snapshot stringset.Set
	err      error
}

// New creates a new List.
//
// List can be resolved into a set of addresses in 'ip:port' format. List is very
// flexible in what host strings are accepted. Names missing a port suffix will
// have a provided port attached. Hosts with a port suffix will be untouched.
// Either ip addresses or host names are allowed.
//
// An error is returned if a DNS record is supplied and resolves to an empty list
// of addresses.
func New(config Config, port int) (*List, error) {
	config.applyDefaults()

	localNames, err := getLocalNames()
	if err != nil {
		return nil, fmt.Errorf("get local names: %s", err)
	}
	localAddrs, err := attachPortIfMissing(localNames, port)
	if err != nil {
		return nil, fmt.Errorf("attach port to local names: %s", err)
	}

	l := &List{
		config:     config,
		port:       port,
		localAddrs: localAddrs,
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

// ResolveNonLocal returns a snapshot of l with the local machine stripped from
// the snapshot, if present. The local machine is identified by both its hostname
// and ip address, concatenated l's port.
func (l *List) ResolveNonLocal() (stringset.Set, error) {
	snapshot, err := l.Resolve()
	if err != nil {
		return nil, err
	}
	return snapshot.Sub(l.localAddrs), nil
}

type snapshotTask struct {
	list *List
}

func (t *snapshotTask) Run() {
	t.list.takeSnapshot()
}

func (l *List) takeSnapshot() {
	snapshot, err := l.resolve()
	l.mu.Lock()
	l.snapshot = snapshot
	l.err = err
	l.mu.Unlock()
}

func (l *List) resolve() (stringset.Set, error) {
	names, err := l.config.resolve()
	if err != nil {
		return nil, fmt.Errorf("config: %s", err)
	}
	addrs, err := attachPortIfMissing(names, l.port)
	if err != nil {
		return nil, fmt.Errorf("attach port to resolved names: %s", err)
	}
	return addrs, nil
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
			if ip.IsLoopback() {
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
