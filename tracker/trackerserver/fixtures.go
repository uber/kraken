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
package trackerserver

import (
	"time"

	"github.com/uber-go/tally"

	"github.com/uber/kraken/tracker/originstore"
	"github.com/uber/kraken/tracker/peerhandoutpolicy"
	"github.com/uber/kraken/tracker/peerstore"
)

// Fixture is a test utility which returns a tracker server with in-memory storage.
func Fixture() *Server {
	policy := peerhandoutpolicy.DefaultPriorityPolicyFixture()
	config := Config{
		AnnounceInterval: 250 * time.Millisecond,
	}
	return New(
		config, tally.NoopScope, policy,
		peerstore.NewTestStore(), originstore.NewNoopStore(), nil)
}
