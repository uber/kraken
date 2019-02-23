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
package healthcheck

import "github.com/uber/kraken/utils/stringset"

// IdentityFilter is a Filter which never filters out any addresses.
type IdentityFilter struct{}

// Run runs the filter.
func (f IdentityFilter) Run(addrs stringset.Set) stringset.Set {
	return addrs.Copy()
}

// Failed is a no-op.
func (f IdentityFilter) Failed(addr string) {}

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
