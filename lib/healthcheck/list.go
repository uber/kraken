package healthcheck

import (
	"github.com/uber/kraken/lib/hostlist"
	"github.com/uber/kraken/utils/stringset"
)

// List defines both a complete list of hosts (same as hostlist)
// and a list of healthy hosts. It can also be used in passive
// healthcheck where a host can be marked as unhealthy.
type List interface {
	Resolve() (healthy stringset.Set, all stringset.Set)
	Failed(addr string)
}

type noopList struct {
	l hostlist.List
}

func (n *noopList) Failed(addr string) {}

func (n *noopList) Resolve() (stringset.Set, stringset.Set) {
	all := n.l.Resolve()
	healthy := all.Copy()
	return healthy, all
}

// NoopList converts a hostlist.List to a List by making the Failed method
// a no-op and Resolve returning all hosts as healthy.
// Useful when healthcheck is disabled.
func NoopList(list hostlist.List) List {
	return &noopList{list}
}

type hostList struct {
	l List
}

func (h *hostList) Resolve() stringset.Set {
	healthy, _ := h.l.Resolve()
	return healthy
}

// HostList converts a List to hostlist.List by only returning healthy hosts
// when resolve is called.
func HostList(list List) hostlist.List {
	return &hostList{list}
}
