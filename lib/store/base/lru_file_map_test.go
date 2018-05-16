package base

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"testing"
	"time"

	"github.com/andres-erbsen/clock"
	"github.com/stretchr/testify/require"
)

func TestLRUFileMapSizeLimit(t *testing.T) {
	require := require.New(t)
	bundle, cleanup := fileStoreLRUFixture(100)
	defer cleanup()

	fm := bundle.store.fileMap
	state := bundle.state1

	insert := func(name string) {
		entry := NewLocalFileEntryFactory().Create(name, state)
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

func TestLRUCreateLastAccessTimeOnCreateFile(t *testing.T) {
	require := require.New(t)
	bundle, cleanup := fileStoreLRUFixture(100)
	defer cleanup()

	store := bundle.store
	clk := bundle.clk.(*clock.Mock)

	t0 := time.Now()
	clk.Set(t0)

	fn := "testfile123"
	s1 := bundle.state1

	require.NoError(store.NewFileOp().AcceptState(s1).CreateFile(fn, s1, 5))

	// Verify file exists.
	_, err := os.Stat(path.Join(s1.GetDirectory(), store.fileEntryFactory.GetRelativePath(fn)))
	require.NoError(err)

	b, err := store.NewFileOp().AcceptState(s1).GetFileMetadata(fn, NewLastAccessTime())
	require.NoError(err)
	lat, err := UnmarshalLastAccessTime(b)
	require.NoError(err)
	require.Equal(t0.Truncate(time.Second), lat)
}

func TestLRUUpdateLastAccessTimeOnMoveFrom(t *testing.T) {
	require := require.New(t)
	bundle, cleanup := fileStoreLRUFixture(100)
	defer cleanup()

	store := bundle.store
	clk := bundle.clk.(*clock.Mock)

	t0 := time.Now()
	clk.Set(t0)

	s1, s2 := bundle.state1, bundle.state2

	source, err := ioutil.TempFile(s1.GetDirectory(), "")
	require.NoError(err)

	require.NoError(store.NewFileOp().AcceptState(s2).MoveFileFrom(source.Name(), s2, source.Name()))

	b, err := store.NewFileOp().AcceptState(s2).GetFileMetadata(source.Name(), NewLastAccessTime())
	require.NoError(err)
	lat, err := UnmarshalLastAccessTime(b)
	require.NoError(err)
	require.Equal(t0.Truncate(time.Second), lat)
}

func TestLRUUpdateLastAccessTimeOnMove(t *testing.T) {
	require := require.New(t)
	bundle, cleanup := fileStoreLRUFixture(100)
	defer cleanup()

	store := bundle.store
	clk := bundle.clk.(*clock.Mock)

	t0 := time.Now()
	clk.Set(t0)

	fn := "testfile123"
	s1, s2 := bundle.state1, bundle.state2

	require.NoError(store.NewFileOp().AcceptState(s1).CreateFile(fn, s1, 1))

	clk.Add(time.Hour)
	require.NoError(store.NewFileOp().AcceptState(s1).MoveFile(fn, s2))

	b, err := store.NewFileOp().AcceptState(s2).GetFileMetadata(fn, NewLastAccessTime())
	require.NoError(err)
	lat, err := UnmarshalLastAccessTime(b)
	require.NoError(err)
	require.Equal(clk.Now().Truncate(time.Second), lat)
}

func TestLRUUpdateLastAccessTimeOnOpen(t *testing.T) {
	require := require.New(t)
	bundle, cleanup := fileStoreLRUFixture(100)
	defer cleanup()

	store := bundle.store
	clk := bundle.clk.(*clock.Mock)

	t0 := time.Now()
	clk.Set(t0)

	fn := "testfile123"
	s1 := bundle.state1

	require.NoError(store.NewFileOp().AcceptState(s1).CreateFile(fn, s1, 1))

	checkLAT := func(op FileOp, expected time.Time) {
		b, err := op.GetFileMetadata(fn, NewLastAccessTime())
		require.NoError(err)
		lat, err := UnmarshalLastAccessTime(b)
		require.NoError(err)
		require.Equal(expected.Truncate(time.Second), lat)
	}

	// No LAT change below resolution.
	clk.Add(time.Minute)
	_, err := store.NewFileOp().AcceptState(s1).GetFileReader(fn)
	require.NoError(err)
	checkLAT(store.NewFileOp().AcceptState(s1), t0)

	clk.Add(time.Hour)
	_, err = store.NewFileOp().AcceptState(s1).GetFileReader(fn)
	require.NoError(err)
	checkLAT(store.NewFileOp().AcceptState(s1), clk.Now())

	clk.Add(time.Hour)
	_, err = store.NewFileOp().AcceptState(s1).GetFileReadWriter(fn)
	require.NoError(err)
	checkLAT(store.NewFileOp().AcceptState(s1), clk.Now())
}

func TestLRUKeepLastAccessTimeOnPeek(t *testing.T) {
	require := require.New(t)
	bundle, cleanup := fileStoreLRUFixture(100)
	defer cleanup()

	store := bundle.store
	clk := bundle.clk.(*clock.Mock)

	t0 := time.Now()
	clk.Set(t0)

	fn := "testfile123"
	s1 := bundle.state1

	require.NoError(store.NewFileOp().AcceptState(s1).CreateFile(fn, s1, 1))

	clk.Add(time.Hour)
	_, err := store.NewFileOp().AcceptState(s1).GetFileStat(fn)
	require.NoError(err)

	b, err := store.NewFileOp().AcceptState(s1).GetFileMetadata(fn, NewLastAccessTime())
	require.NoError(err)
	lat, err := UnmarshalLastAccessTime(b)
	require.NoError(err)
	require.Equal(t0.Truncate(time.Second), lat)

	clk.Add(time.Hour)
	_, err = store.NewFileOp().AcceptState(s1).GetFilePath(fn)
	require.NoError(err)

	b, err = store.NewFileOp().AcceptState(s1).GetFileMetadata(fn, NewLastAccessTime())
	require.NoError(err)
	lat, err = UnmarshalLastAccessTime(b)
	require.NoError(err)
	require.Equal(t0.Truncate(time.Second), lat)
}
