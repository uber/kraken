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
package metainfogen

import (
	"testing"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/store/metadata"
	"github.com/uber/kraken/lib/store/tiered"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/require"
)

func TestGenerate(t *testing.T) {
	require := require.New(t)

	tieredStore, _ := tiered.StoreFixture(t)

	pieceLength := 10

	generator, err := New(Config{
		PieceLengths: map[datasize.ByteSize]datasize.ByteSize{
			0: datasize.ByteSize(pieceLength),
		},
	}, tieredStore)
	require.NoError(err)

	blob := core.SizedBlobFixture(100, uint64(pieceLength))

	f, err := tieredStore.Create(blob.Digest.Hex(), uint64(len(blob.Content)))
	require.NoError(err)
	_, err = f.Write(blob.Content)
	require.NoError(err)
	require.NoError(f.Close())
	require.NoError(tieredStore.MarkComplete(blob.Digest.Hex()))

	require.NoError(generator.Generate(blob.Digest))

	var tm metadata.TorrentMeta
	ok, err := tieredStore.GetMetadata(blob.Digest.Hex(), &tm)
	require.NoError(err)
	require.True(ok)
	require.Equal(blob.MetaInfo, tm.MetaInfo)
}
