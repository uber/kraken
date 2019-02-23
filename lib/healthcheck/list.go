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
