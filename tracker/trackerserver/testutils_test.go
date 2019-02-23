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
	"net/http"
	"testing"

	"github.com/uber/kraken/mocks/origin/blobclient"
	"github.com/uber/kraken/mocks/tracker/originstore"
	"github.com/uber/kraken/mocks/tracker/peerstore"
	"github.com/uber/kraken/tracker/peerhandoutpolicy"

	"github.com/golang/mock/gomock"
	"github.com/uber-go/tally"
)

type serverMocks struct {
	config        Config
	policy        *peerhandoutpolicy.PriorityPolicy
	ctrl          *gomock.Controller
	peerStore     *mockpeerstore.MockStore
	originStore   *mockoriginstore.MockStore
	originCluster *mockblobclient.MockClusterClient
	stats         tally.Scope
}

func newServerMocks(t *testing.T, config Config) (*serverMocks, func()) {
	ctrl := gomock.NewController(t)
	return &serverMocks{
		config:        config,
		policy:        peerhandoutpolicy.DefaultPriorityPolicyFixture(),
		peerStore:     mockpeerstore.NewMockStore(ctrl),
		originStore:   mockoriginstore.NewMockStore(ctrl),
		originCluster: mockblobclient.NewMockClusterClient(ctrl),
		stats:         tally.NewTestScope("testing", nil),
	}, ctrl.Finish
}

func (m *serverMocks) handler() http.Handler {
	return New(
		m.config,
		m.stats,
		m.policy,
		m.peerStore,
		m.originStore,
		m.originCluster).Handler()
}
