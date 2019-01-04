package healthcheck

import (
	"github.com/uber/kraken/lib/hostlist"
)

// List is a hostlist.List which can be passively health checked.
type List interface {
	hostlist.List
	Failed(addr string)
}

type noopFailed struct {
	hostlist.List
}

func (f *noopFailed) Failed(addr string) {}

// NoopFailed converts a hostlist.List to a List by making the Failed method
// a no-op. Useful for using a Monitor in place of a Passive.
func NoopFailed(list hostlist.List) List {
	return &noopFailed{list}
}
