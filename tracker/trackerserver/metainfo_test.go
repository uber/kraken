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
	"testing"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/hashring"
	"github.com/uber/kraken/lib/hostlist"
	"github.com/uber/kraken/tracker/metainfoclient"
	"github.com/uber/kraken/utils/httputil"
	"github.com/uber/kraken/utils/testutil"

	"github.com/stretchr/testify/require"
)

func newMetaInfoClient(addr string) metainfoclient.Client {
	return metainfoclient.New(hashring.NoopPassiveRing(hostlist.Fixture(addr)), nil)
}

func TestGetMetaInfoHandlerFetchesFromOrigin(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newServerMocks(t, Config{})
	defer cleanup()

	addr, stop := testutil.StartServer(mocks.handler())
	defer stop()

	namespace := core.TagFixture()
	mi := core.MetaInfoFixture()

	mocks.originCluster.EXPECT().GetMetaInfo(namespace, mi.Digest()).Return(mi, nil)

	client := newMetaInfoClient(addr)

	result, err := client.Download(namespace, mi.Digest())
	require.NoError(err)
	require.Equal(mi, result)
}

func TestGetMetaInfoHandlerPropagatesOriginError(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newServerMocks(t, Config{})
	defer cleanup()

	addr, stop := testutil.StartServer(mocks.handler())
	defer stop()

	namespace := core.TagFixture()
	mi := core.MetaInfoFixture()

	mocks.originCluster.EXPECT().GetMetaInfo(
		namespace, mi.Digest()).Return(nil, httputil.StatusError{Status: 599}).MinTimes(1)

	client := newMetaInfoClient(addr)

	_, err := client.Download(namespace, mi.Digest())
	require.Error(err)
	require.True(httputil.IsStatus(err, 599))
}
