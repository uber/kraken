// Copyright (c) 2016-2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package metainfosidecar

import (
	"bytes"
	"testing"

	"github.com/uber-go/tally"
	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/backend/namepath"
	"github.com/uber/kraken/lib/backend/testfs"
	"github.com/uber/kraken/utils/closers"
	"github.com/uber/kraken/utils/testutil"

	"github.com/stretchr/testify/require"
)

func TestFetchRoundTrip(t *testing.T) {
	require := require.New(t)

	s := testfs.NewServer()
	defer s.Cleanup()

	addr, stop := testutil.StartServer(s.Handler())
	defer stop()

	c, err := testfs.NewClient(
		testfs.Config{Addr: addr, NamePath: namepath.Identity}, tally.NoopScope)
	require.NoError(err)
	defer closers.Close(c)

	blob := core.SizedBlobFixture(100, 7)
	ns := core.NamespaceFixture()

	b, err := blob.MetaInfo.Serialize()
	require.NoError(err)
	require.NoError(c.Upload(ns, Name(blob.Digest.Hex()), bytes.NewReader(b)))

	mi, err := Fetch(c, ns, blob.Digest)
	require.NoError(err)
	require.Equal(blob.MetaInfo.InfoHash(), mi.InfoHash())
	require.Equal(blob.MetaInfo.NumPieces(), mi.NumPieces())
	for i := 0; i < blob.MetaInfo.NumPieces(); i++ {
		require.Equal(blob.MetaInfo.GetPieceSum(i), mi.GetPieceSum(i))
	}
}

func TestFetchNotFound(t *testing.T) {
	require := require.New(t)

	s := testfs.NewServer()
	defer s.Cleanup()

	addr, stop := testutil.StartServer(s.Handler())
	defer stop()

	c, err := testfs.NewClient(
		testfs.Config{Addr: addr, NamePath: namepath.Identity}, tally.NoopScope)
	require.NoError(err)
	defer closers.Close(c)

	_, err = Fetch(c, core.NamespaceFixture(), core.DigestFixture())
	require.Error(err)
}
