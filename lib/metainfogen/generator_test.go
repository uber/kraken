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
package metainfogen

import (
	"bytes"
	"testing"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/store"
	"github.com/uber/kraken/lib/store/metadata"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/require"
)

func TestGenerate(t *testing.T) {
	require := require.New(t)

	cas, cleanup := store.CAStoreFixture()
	defer cleanup()

	pieceLength := 10

	generator, err := New(Config{
		PieceLengths: map[datasize.ByteSize]datasize.ByteSize{
			0: datasize.ByteSize(pieceLength),
		},
	}, cas)
	require.NoError(err)

	blob := core.SizedBlobFixture(100, uint64(pieceLength))

	require.NoError(cas.CreateCacheFile(blob.Digest.Hex(), bytes.NewReader(blob.Content)))

	require.NoError(generator.Generate(blob.Digest))

	var tm metadata.TorrentMeta
	require.NoError(cas.GetCacheFileMetadata(blob.Digest.Hex(), &tm))
	require.Equal(blob.MetaInfo, tm.MetaInfo)
}
