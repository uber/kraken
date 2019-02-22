// Copyright (c) 2019 Uber Technologies, Inc.
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
package agentserver

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/uber-go/tally"

	"github.com/uber/kraken/lib/store"
	"github.com/uber/kraken/mocks/lib/torrent/scheduler"
	"github.com/uber/kraken/utils/testutil"
)

type serverMocks struct {
	cads    *store.CADownloadStore
	sched   *mockscheduler.MockReloadableScheduler
	cleanup *testutil.Cleanup
}

func newServerMocks(t *testing.T) (*serverMocks, func()) {
	var cleanup testutil.Cleanup

	cads, c := store.CADownloadStoreFixture()
	cleanup.Add(c)

	ctrl := gomock.NewController(t)
	cleanup.Add(ctrl.Finish)

	sched := mockscheduler.NewMockReloadableScheduler(ctrl)

	return &serverMocks{cads, sched, &cleanup}, cleanup.Run
}

func (m *serverMocks) startServer() string {
	s := New(Config{}, tally.NoopScope, m.cads, m.sched)
	addr, stop := testutil.StartServer(s.Handler())
	m.cleanup.Add(stop)
	return addr
}
