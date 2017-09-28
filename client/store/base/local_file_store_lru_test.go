package base

import (
	"log"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// This is a test specifically for LocalFileStoreLRU
func TestLocalStoreLRU(t *testing.T) {
	require := require.New(t)
	storeBundle, cleanup := fileStoreLRUFixture(1)
	defer cleanup()
	store := storeBundle.store

	fileBundle, ok := storeBundle.files[storeBundle.state1]
	if !ok {
		log.Fatal("file not found in state1")
	}
	fn := fileBundle.name

	// Verify an existing file
	_, err := store.GetFileStat(fn, []FileState{storeBundle.state1})
	require.NoError(err)

	// Create one more file should evict the previous one
	fn2 := "file2"
	err = store.CreateFile(fn2, []FileState{}, storeBundle.state1, 0)
	require.NoError(err)
	_, err = store.GetFileStat(fn2, []FileState{storeBundle.state1})
	require.NoError(err)

	// Verify the previous one is deleted
	_, err = store.GetFileStat(fn, []FileState{storeBundle.state1})
	require.True(os.IsNotExist(err))
}
