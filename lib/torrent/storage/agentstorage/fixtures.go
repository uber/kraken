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
package agentstorage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/store/disk"
	"github.com/uber/kraken/tracker/metainfoclient"
)

// TorrentArchiveFixture returns a TorrrentArchive for testing purposes.
func TorrentArchiveFixture(t *testing.T) *TorrentArchive {
	return NewTorrentArchive(tally.NoopScope, disk.Fixture(t), nil)
}

// TorrentFixture returns a Torrent for the given metainfo for testing purposes.
func TorrentFixture(t *testing.T, mi *core.MetaInfo) *Torrent {
	tc := metainfoclient.NewTestClient()

	ta := NewTorrentArchive(tally.NoopScope, disk.Fixture(t), tc)

	require.NoError(t, tc.Upload(mi))

	tor, err := ta.CreateTorrent("noexist", mi.Digest())
	require.NoError(t, err)

	torrent, ok := tor.(*Torrent)
	require.True(t, ok)
	return torrent
}
