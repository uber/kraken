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
package store

import (
	"os"
	"sync"
	"testing"

	"github.com/uber/kraken/core"

	"github.com/stretchr/testify/require"
)

func TestCADownloadStoreDownloadAndDeleteFiles(t *testing.T) {
	require := require.New(t)

	s, cleanup := CADownloadStoreFixture()
	defer cleanup()

	var names []string
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		name := core.DigestFixture().Hex()
		names = append(names, name)
		wg.Add(1)
		go func() {
			defer wg.Done()
			require.NoError(s.CreateDownloadFile(name, 1))
			require.NoError(s.MoveDownloadFileToCache(name))
			require.NoError(s.Cache().DeleteFile(name))
		}()
	}
	wg.Wait()

	for _, name := range names {
		_, err := s.Cache().GetFileStat(name)
		require.True(os.IsNotExist(err))
	}
}
