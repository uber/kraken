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
package proxyserver

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/uber-go/tally"

	mockblobclient "github.com/uber/kraken/mocks/origin/blobclient"
	"github.com/uber/kraken/utils/testutil"
)

type serverMocks struct {
	originClient *mockblobclient.MockClusterClient
	cleanup      *testutil.Cleanup
}

func newServerMocks(t *testing.T) (*serverMocks, func()) {
	var cleanup testutil.Cleanup
	defer cleanup.Recover()

	ctrl := gomock.NewController(t)
	cleanup.Add(ctrl.Finish)

	originClient := mockblobclient.NewMockClusterClient(ctrl)

	return &serverMocks{
		originClient: originClient,
		cleanup:      &cleanup,
	}, cleanup.Run
}

func (m *serverMocks) startServer() string {
	s := New(tally.NoopScope, m.originClient)
	addr, stop := testutil.StartServer(s.Handler())
	m.cleanup.Add(stop)
	return addr
}
