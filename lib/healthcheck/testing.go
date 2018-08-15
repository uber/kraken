package healthcheck

import "code.uber.internal/infra/kraken/utils/stringset"

// IdentityFilter is a Filter which never filters out any addresses.
type IdentityFilter struct{}

// Run runs the filter.
func (f IdentityFilter) Run(addrs stringset.Set) stringset.Set {
	return addrs.Copy()
}

// ManualFilter is a Filter whose unhealthy hosts can be manually changed.
type ManualFilter struct {
	Unhealthy stringset.Set
}

// NewManualFilter returns a new ManualFilter.
func NewManualFilter() *ManualFilter {
	return &ManualFilter{stringset.New()}
}

// Run removes any unhealthy addrs.
func (f *ManualFilter) Run(addrs stringset.Set) stringset.Set {
	return addrs.Sub(f.Unhealthy)
}

// BinaryFilter is a filter which can be switched to all-healthy vs. all-unhealthy.
type BinaryFilter struct {
	Healthy bool
}

// NewBinaryFilter returns a new BinaryFilter that defaults to all-healthy.
func NewBinaryFilter() *BinaryFilter {
	return &BinaryFilter{true}
}

// Run runs the filter.
func (f BinaryFilter) Run(addrs stringset.Set) stringset.Set {
	if f.Healthy {
		return addrs.Copy()
	}
	return stringset.New()
}
