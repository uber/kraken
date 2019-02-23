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
package agentstorage

import (
	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/store"
	"github.com/uber/kraken/tracker/metainfoclient"
	"github.com/uber/kraken/utils/testutil"
	"github.com/uber-go/tally"
)

// TorrentArchiveFixture returns a TorrrentArchive for testing purposes.
func TorrentArchiveFixture() (*TorrentArchive, func()) {
	cads, cleanup := store.CADownloadStoreFixture()
	archive := NewTorrentArchive(tally.NoopScope, cads, nil)
	return archive, cleanup
}

// TorrentFixture returns a Torrent for the given metainfo for testing purposes.
func TorrentFixture(mi *core.MetaInfo) (*Torrent, func()) {
	var cleanup testutil.Cleanup
	defer cleanup.Recover()

	cads, c := store.CADownloadStoreFixture()
	cleanup.Add(c)

	tc := metainfoclient.NewTestClient()

	ta := NewTorrentArchive(tally.NoopScope, cads, tc)

	if err := tc.Upload(mi); err != nil {
		panic(err)
	}

	t, err := ta.CreateTorrent("noexist", mi.Digest())
	if err != nil {
		panic(err)
	}

	return t.(*Torrent), cleanup.Run
}
