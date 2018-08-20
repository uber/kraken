package hostlist

import (
	"fmt"
	"net"
	"os"
	"strings"
	"sync"

	"code.uber.internal/infra/kraken/utils/dedup"
	"code.uber.internal/infra/kraken/utils/log"
	"code.uber.internal/infra/kraken/utils/stringset"

	"github.com/andres-erbsen/clock"
)

// List defines a list of hosts which is subject to change.
type List interface {
	Resolve() stringset.Set
}

type list struct {
	config Config
	port   int

	snapshotTrap *dedup.IntervalTrap

	mu       sync.RWMutex
	snapshot stringset.Set
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
//
// If List is backed by DNS, it will be periodically refreshed (defined by TTL
// in config). If, after construction, there is an error resolving DNS, the
// latest successful snapshot is used. As such, Resolve never returns an empty
// set.
func New(config Config, port int) (List, error) {
	config.applyDefaults()

	l := &list{
		config: config,
		port:   port,
	}
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

	return l.snapshot
}

type snapshotTask struct {
	list *list
}

func (t *snapshotTask) Run() {
	if err := t.list.takeSnapshot(); err != nil {
		log.With("source", t.list.config).Errorf("Error taking hostlist snapshot: %s", err)
	}
}

func (l *list) takeSnapshot() error {
	snapshot, err := l.resolve()
	if err != nil {
		return err
	}
	l.mu.Lock()
	l.snapshot = snapshot
	l.mu.Unlock()
	return nil
}

func (l *list) resolve() (stringset.Set, error) {
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
