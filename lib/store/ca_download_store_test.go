package store

import (
	"os"
	"sync"
	"testing"

	"code.uber.internal/infra/kraken/core"

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
