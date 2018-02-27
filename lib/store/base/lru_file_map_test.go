package base

import (
	"fmt"
	"testing"

	"github.com/andres-erbsen/clock"
	"github.com/stretchr/testify/require"
)

func TestLRUFileMapSizeLimit(t *testing.T) {
	require := require.New(t)

	state, cleanup := fileStateFixture()
	defer cleanup()

	fm, err := NewLRUFileMap(100, clock.New())
	require.NoError(err)

	insert := func(name string) {
		entry := DefaultLocalFileEntryFactory(clock.New()).Create(name, state)
		_, loaded := fm.LoadOrStore(name, entry, func(name string, entry FileEntry) error {
			require.NoError(entry.Create(state, 0))
			return nil
		})
		require.False(loaded)
	}

	// Generate 101 file names.
	var names []string
	for i := 0; i < 101; i++ {
		names = append(names, fmt.Sprintf("test_file_%d", i))
	}

	// After inserting 100 files, the first file still exists in map.
	for _, name := range names[:100] {
		insert(name)
	}
	require.True(fm.Contains(names[0]))

	// Insert one more file entry beyond size limit.
	insert(names[100])

	// The first file should have been removed.
	require.False(fm.Contains(names[0]))
}
