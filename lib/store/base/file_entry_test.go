package base

import (
	"io"
	"io/ioutil"
	"os"
	"path"
	"reflect"
	"runtime"
	"sort"
	"testing"
	"time"

	"code.uber.internal/infra/kraken/core"

	"github.com/andres-erbsen/clock"
	"github.com/stretchr/testify/require"
)

func checkListNames(t *testing.T, factory FileEntryFactory, state FileState, expected []FileEntry) {
	t.Helper()

	var expectedNames []string
	for _, e := range expected {
		expectedNames = append(expectedNames, e.GetName())
	}
	sort.Strings(expectedNames)

	names, err := factory.ListNames(state)
	require.NoError(t, err)

	sort.Strings(names)
	require.Equal(t, expectedNames, names)
}

func TestFileEntryFactoryListNames(t *testing.T) {
	for _, factory := range []FileEntryFactory{
		DefaultLocalFileEntryFactory(clock.New()),
		DefaultCASFileEntryFactory(clock.New()),
	} {
		fname := reflect.Indirect(reflect.ValueOf(factory)).Type().Name()
		t.Run(fname, func(t *testing.T) {
			require := require.New(t)

			state, _, _, cleanup := fileStatesFixture()
			defer cleanup()

			// ListNames should show all created entries.
			var entries []FileEntry
			for i := 0; i < 100; i++ {
				entry := factory.Create(core.DigestFixture().Hex(), state)
				require.NoError(entry.Create(state, 1))
				entries = append(entries, entry)
			}
			checkListNames(t, factory, state, entries)

			// ListNames should not show deleted entries.
			for _, e := range entries[:50] {
				require.NoError(e.Delete())
			}
			checkListNames(t, factory, state, entries[50:])
		})
	}
}

func TestFileEntryFactoryLocalToCASListNames(t *testing.T) {
	require := require.New(t)

	state, _, _, cleanup := fileStatesFixture()
	defer cleanup()

	localFactory := DefaultLocalFileEntryFactory(clock.New())
	casFactory := DefaultCASFileEntryFactory(clock.New())

	localEntry := localFactory.Create(core.DigestFixture().Hex(), state)
	require.NoError(localEntry.Create(state, 1))

	casEntry := casFactory.Create(core.DigestFixture().Hex(), state)
	require.NoError(casEntry.Create(state, 1))

	// Local entry should not show up.
	checkListNames(t, casFactory, state, []FileEntry{casEntry})
}

// TODO(codyg): Dismantle this as only one FileEntry implementation will be used.
// These tests should pass for all FileEntry implementations
func TestFileEntry(t *testing.T) {
	stores := []struct {
		name    string
		fixture func() (bundle *fileEntryTestBundle, cleanup func())
	}{
		{"LocalFileEntry", fileEntryLocalFixture},
	}

	tests := []func(require *require.Assertions, bundle *fileEntryTestBundle){
		testCreate,
		testCreateExisting,
		testCreateFail,
		testMoveFrom,
		testMoveFromExisting,
		testMoveFromWrongState,
		testMoveFromWrongSourcePath,
		testMove,
		testDelete,
		testGetMetadataAndSetMetadata,
		testGetMetadataFail,
		testGetMetadataAtAndSetMetadataAt,
		testGetMetadataAtAndSetMetadataAtFail,
		testGetOrSetMetadata,
		testDeleteMetadata,
	}

	for _, store := range stores {
		t.Run(store.name, func(t *testing.T) {
			for _, test := range tests {
				testName := runtime.FuncForPC(reflect.ValueOf(test).Pointer()).Name()
				t.Run(testName, func(t *testing.T) {
					require := require.New(t)
					s, cleanup := store.fixture()
					defer cleanup()
					test(require, s)
				})
			}
		})
	}
}

func testCreate(require *require.Assertions, bundle *fileEntryTestBundle) {
	fe := bundle.entry
	s1 := bundle.state1

	fp := fe.GetPath()
	testFileSize := int64(123)

	// Create succeeds with correct state.
	err := fe.Create(s1, testFileSize)
	require.NoError(err)
	info, err := os.Stat(fp)
	require.NoError(err)
	require.Equal(info.Size(), testFileSize)
}

func testCreateExisting(require *require.Assertions, bundle *fileEntryTestBundle) {
	fe := bundle.entry
	s1 := bundle.state1

	fp := fe.GetPath()
	testFileSize := int64(123)

	// Create succeeds with correct state.
	err := fe.Create(s1, testFileSize)
	require.NoError(err)
	info, err := os.Stat(fp)
	require.NoError(err)
	require.Equal(info.Size(), testFileSize)

	// Create fails with existing file.
	err = fe.Create(s1, testFileSize-1)
	require.True(os.IsExist(err))
	info, err = os.Stat(fp)
	require.NoError(err)
	require.Equal(info.Size(), testFileSize)
}

func testCreateFail(require *require.Assertions, bundle *fileEntryTestBundle) {
	fe := bundle.entry
	s2 := bundle.state2

	fp := fe.GetPath()
	testFileSize := int64(123)

	// Create fails with wrong state.
	err := fe.Create(s2, testFileSize)
	require.Error(err)
	require.True(IsFileStateError(err))
	_, err = os.Stat(fp)
	require.Error(err)
	require.True(os.IsNotExist(err))
}

func testMoveFrom(require *require.Assertions, bundle *fileEntryTestBundle) {
	fe := bundle.entry
	s1 := bundle.state1
	s3 := bundle.state3

	fp := fe.GetPath()
	testSourceFile, err := ioutil.TempFile(s3.GetDirectory(), "")
	require.NoError(err)

	// MoveFrom succeeds with correct state and source path.
	err = fe.MoveFrom(s1, testSourceFile.Name())
	require.NoError(err)
	_, err = os.Stat(fp)
	require.NoError(err)
}

func testMoveFromExisting(require *require.Assertions, bundle *fileEntryTestBundle) {
	fe := bundle.entry
	s1 := bundle.state1
	s3 := bundle.state3

	fp := fe.GetPath()
	testSourceFile, err := ioutil.TempFile(s3.GetDirectory(), "")
	require.NoError(err)

	// MoveFrom succeeds with correct state and source path.
	err = fe.MoveFrom(s1, testSourceFile.Name())
	require.NoError(err)
	_, err = os.Stat(fp)
	require.NoError(err)

	// MoveFrom fails with existing file.
	testSourceFile2, err := ioutil.TempFile(s3.GetDirectory(), "")
	err = fe.MoveFrom(s1, testSourceFile2.Name())
	require.True(os.IsExist(err))
	_, err = os.Stat(fp)
	require.NoError(err)
}

func testMoveFromWrongState(require *require.Assertions, bundle *fileEntryTestBundle) {
	fe := bundle.entry
	s2 := bundle.state2
	s3 := bundle.state3

	fp := fe.GetPath()
	testSourceFile, err := ioutil.TempFile(s3.GetDirectory(), "")
	require.NoError(err)

	// MoveFrom fails with wrong state.
	err = fe.MoveFrom(s2, testSourceFile.Name())
	require.Error(err)
	require.True(IsFileStateError(err))
	_, err = os.Stat(fp)
	require.Error(err)
	require.True(os.IsNotExist(err))
}

func testMoveFromWrongSourcePath(require *require.Assertions, bundle *fileEntryTestBundle) {
	fe := bundle.entry
	s1 := bundle.state1

	fp := fe.GetPath()

	// MoveFrom fails with wrong source path.
	err := fe.MoveFrom(s1, "")
	require.Error(err)
	require.True(os.IsNotExist(err))
	_, err = os.Stat(fp)
	require.Error(err)
	require.True(os.IsNotExist(err))
}

func testMove(require *require.Assertions, bundle *fileEntryTestBundle) {
	fe := bundle.entry
	s1 := bundle.state1
	s2 := bundle.state2
	s3 := bundle.state3

	fn := fe.GetName()
	fp := fe.GetPath()
	testFileSize := int64(123)
	m1 := getMockMetadataOne()
	b1 := make([]byte, 2)
	m2 := getMockMetadataMovable()
	b2 := make([]byte, 1)

	// Create file first.
	err := fe.Create(s1, testFileSize)
	require.NoError(err)

	// Write metadata
	updated, err := fe.SetMetadata(m1, []byte{uint8(0), uint8(1)})
	require.NoError(err)
	require.True(updated)
	updated, err = fe.SetMetadata(m2, []byte{uint8(3)})
	require.NoError(err)
	require.True(updated)

	// Verify metadata is readable.
	b1, err = fe.GetMetadata(m1)
	require.NoError(err)
	require.NotNil(b1)
	require.Equal(uint8(0), b1[0])
	require.Equal(uint8(1), b1[1])
	b2, err = fe.GetMetadata(m2)
	require.NoError(err)
	require.NotNil(b2)
	require.Equal(uint8(3), b2[0])

	// Move file, removes non-movable metadata.
	err = fe.Move(s3)
	require.NoError(err)
	_, err = os.Stat(fp)
	require.Error(err)
	require.True(os.IsNotExist(err))
	_, err = os.Stat(fe.GetPath())
	require.NoError(err)

	// Verify metadata that's not movable is deleted.
	_, err = fe.GetMetadata(m1)
	require.Error(err)
	require.True(os.IsNotExist(err))

	_, err = os.Stat(path.Join(s1.GetDirectory(), fn, getMockMetadataOne().GetSuffix()))
	require.Error(err)
	require.True(os.IsNotExist(err))
	_, err = os.Stat(path.Join(s2.GetDirectory(), fn, getMockMetadataOne().GetSuffix()))
	require.Error(err)
	require.True(os.IsNotExist(err))
	_, err = os.Stat(path.Join(s3.GetDirectory(), fn, getMockMetadataOne().GetSuffix()))
	require.Error(err)
	require.True(os.IsNotExist(err))

	// Verify metadata that's movable should have been moved along with the file entry.
	b2Moved, err := fe.GetMetadata(m2)
	require.NoError(err)
	require.NotNil(b2Moved)
	require.Equal(uint8(3), b2Moved[0])

	_, err = os.Stat(path.Join(s3.GetDirectory(), fn))
	require.Nil(err)
	_, err = os.Stat(path.Join(s1.GetDirectory(), fn, getMockMetadataMovable().GetSuffix()))
	require.Error(err)
	require.True(os.IsNotExist(err))
	_, err = os.Stat(path.Join(s2.GetDirectory(), fn, getMockMetadataMovable().GetSuffix()))
	require.Error(err)
	require.True(os.IsNotExist(err))
	_, err = os.Stat(path.Join(s3.GetDirectory(), fn, getMockMetadataMovable().GetSuffix()))
	require.NoError(err)
}

func testDelete(require *require.Assertions, bundle *fileEntryTestBundle) {
	fe := bundle.entry
	s1 := bundle.state1

	fn := fe.GetName()
	fp := fe.GetPath()
	testFileSize := int64(123)
	m1 := getMockMetadataOne()
	m2 := getMockMetadataMovable()

	// Create file first.
	err := fe.Create(s1, testFileSize)
	require.NoError(err)

	// Write metadata.
	updated, err := fe.SetMetadata(m1, []byte{uint8(0), uint8(1)})
	require.NoError(err)
	require.True(updated)
	updated, err = fe.SetMetadata(m2, []byte{uint8(3)})
	require.NoError(err)
	require.True(updated)

	// Delete.
	err = fe.Delete()
	require.NoError(err)

	// Verify the data file and metadata files are all deleted.
	_, err = os.Stat(fp)
	require.Error(err)
	require.True(os.IsNotExist(err))
	_, err = os.Stat(path.Join(s1.GetDirectory(), fn, getMockMetadataOne().GetSuffix()))
	require.Error(err)
	require.True(os.IsNotExist(err))
	_, err = os.Stat(path.Join(s1.GetDirectory(), fn, getMockMetadataMovable().GetSuffix()))
	require.Error(err)
	require.True(os.IsNotExist(err))
}

func testGetMetadataAndSetMetadata(require *require.Assertions, bundle *fileEntryTestBundle) {
	fe := bundle.entry

	m1 := getMockMetadataOne()
	b := make([]byte, 2)

	// Write metadata.
	updated, err := fe.SetMetadata(m1, []byte{uint8(0), uint8(0)})
	require.NoError(err)
	require.True(updated)

	updated, err = fe.SetMetadata(m1, []byte{uint8(0), uint8(0)})
	require.NoError(err)
	require.False(updated)

	// Read metadata.
	b, err = fe.GetMetadata(m1)
	require.NoError(err)
	require.NotNil(b)
	require.Equal(uint8(0), b[0])
	require.Equal(uint8(0), b[1])
}

func testGetMetadataFail(require *require.Assertions, bundle *fileEntryTestBundle) {
	fe := bundle.entry

	m1 := getMockMetadataOne()
	m2 := getMockMetadataTwo()

	// Invalid read.
	_, err := fe.GetMetadata(m1)
	require.True(os.IsNotExist(err))

	// Invalid read.
	_, err = fe.GetMetadata(m2)
	require.True(os.IsNotExist(err))
}

func testGetMetadataAtAndSetMetadataAt(require *require.Assertions, bundle *fileEntryTestBundle) {
	fe := bundle.entry

	m1 := getMockMetadataOne()
	b := make([]byte, 1)

	// Write metadata.
	updated, err := fe.SetMetadata(m1, []byte{uint8(0), uint8(0)})
	require.NoError(err)
	require.True(updated)

	// Write metadata at.
	n, err := fe.SetMetadataAt(m1, []byte{uint8(1)}, 1)
	require.NoError(err)
	require.Equal(n, 1)

	n, err = fe.SetMetadataAt(m1, []byte{uint8(1)}, 1)
	require.NoError(err)
	require.Equal(n, 0)

	// Read metadata at.
	n, err = fe.GetMetadataAt(m1, b, 0)
	require.NoError(err)
	require.Equal(n, 1)
	require.Equal(uint8(0), b[0])

	n, err = fe.GetMetadataAt(m1, b, 1)
	require.NoError(err)
	require.Equal(n, 1)
	require.Equal(uint8(1), b[0])
}

func testGetMetadataAtAndSetMetadataAtFail(require *require.Assertions, bundle *fileEntryTestBundle) {
	fe := bundle.entry

	m1 := getMockMetadataOne()
	b := make([]byte, 2)

	// Invalid write at.
	n, err := fe.SetMetadataAt(m1, b, 0)
	require.Error(err)
	require.Equal(n, 0)

	// Invalid read at.
	_, err = fe.GetMetadataAt(m1, b, 0)
	require.True(os.IsNotExist(err))

	// Write metadata.
	updated, err := fe.SetMetadata(m1, []byte{uint8(0), uint8(1)})
	require.NoError(err)
	require.True(updated)

	// Valid read at, with io.EOF.
	n, err = fe.GetMetadataAt(m1, b, 1)
	require.Error(err)
	require.Equal(n, 1)
	require.Equal(err, io.EOF)
	require.Equal(uint8(1), b[0])
}

func testGetOrSetMetadata(require *require.Assertions, bundle *fileEntryTestBundle) {
	fe := bundle.entry

	m := getMockMetadataOne()
	content := []byte("foo")

	// First GetOrSet should write.
	b, err := fe.GetOrSetMetadata(m, content)
	require.NoError(err)
	require.Equal(content, b)

	// Second GetOrSet should read.
	b, err = fe.GetOrSetMetadata(m, []byte("bar"))
	require.NoError(err)
	require.Equal(content, b)
}

func testDeleteMetadata(require *require.Assertions, bundle *fileEntryTestBundle) {
	fe := bundle.entry
	s1 := bundle.state1

	fn := fe.GetName()
	m1 := getMockMetadataOne()

	// Write metadata.
	updated, err := fe.SetMetadata(m1, []byte{uint8(0), uint8(0)})
	require.NoError(err)
	require.True(updated)
	_, e := fe.GetMetadata(m1)
	require.NoError(e)

	// Stat metadatafile before and after deletion to ensure that it is deleted.
	_, err = os.Stat(path.Join(s1.GetDirectory(), fn, getMockMetadataOne().GetSuffix()))
	require.NoError(err)
	err = fe.DeleteMetadata(m1)
	require.NoError(err)
	_, err = os.Stat(path.Join(s1.GetDirectory(), fn, getMockMetadataOne().GetSuffix()))
	require.Error(err)
}

func TestFileEntryLastAccessTimeUpdatedOnCreate(t *testing.T) {
	require := require.New(t)

	clk := clock.NewMock()
	t0 := time.Now()
	clk.Set(t0)

	state, cleanup := fileStateFixture()
	defer cleanup()

	fe := DefaultLocalFileEntryFactory(clk).Create("test", state)

	require.NoError(fe.Create(state, 1))

	lat, err := getLastAccessTime(fe)
	require.NoError(err)
	require.Equal(t0.Truncate(time.Second), lat.Truncate(time.Second))
}

func TestFileEntryLastAccessTimeUpdatedOnMoveFrom(t *testing.T) {
	require := require.New(t)

	clk := clock.NewMock()
	t0 := time.Now()
	clk.Set(t0)

	s1, s2, _, cleanup := fileStatesFixture()
	defer cleanup()

	fe := DefaultLocalFileEntryFactory(clk).Create("test", s2)

	source, err := ioutil.TempFile(s1.GetDirectory(), "")
	require.NoError(err)

	require.NoError(fe.MoveFrom(s2, source.Name()))

	lat, err := getLastAccessTime(fe)
	require.NoError(err)
	require.Equal(t0.Truncate(time.Second), lat.Truncate(time.Second))
}

func TestFileEntryLastAccessTimeUpdatedOnMove(t *testing.T) {
	require := require.New(t)

	clk := clock.NewMock()
	clk.Set(time.Now())

	s1, s2, _, cleanup := fileStatesFixture()
	defer cleanup()

	config := fileEntryConfig{lastAccessResolution: 5 * time.Second}
	fe := NewLocalFileEntryFactory(config, clk).Create("test", s1)

	require.NoError(fe.Create(s1, 1))

	clk.Add(time.Hour)
	require.NoError(fe.Move(s2))
	lat, err := getLastAccessTime(fe)
	require.NoError(err)
	require.Equal(clk.Now().Truncate(time.Second), lat.Truncate(time.Second))
}

func TestFileEntryLastAccessTimeUpdatedOnOpen(t *testing.T) {
	require := require.New(t)

	clk := clock.NewMock()
	clk.Set(time.Now())

	state, cleanup := fileStateFixture()
	defer cleanup()

	config := fileEntryConfig{lastAccessResolution: 5 * time.Second}
	fe := NewLocalFileEntryFactory(config, clk).Create("test", state)

	require.NoError(fe.Create(state, 1))

	checkLAT := func(expected time.Time) {
		lat, err := getLastAccessTime(fe)
		require.NoError(err)
		require.Equal(expected.Truncate(time.Second), lat.Truncate(time.Second))
	}

	clk.Add(time.Hour)
	_, err := fe.GetReader()
	require.NoError(err)
	checkLAT(clk.Now())

	clk.Add(time.Hour)
	_, err = fe.GetReadWriter()
	require.NoError(err)
	checkLAT(clk.Now())
}

func TestFileEntryLastAccessTimeResolution(t *testing.T) {
	require := require.New(t)

	clk := clock.NewMock()
	t0 := time.Now()
	clk.Set(t0)

	state, cleanup := fileStateFixture()
	defer cleanup()

	config := fileEntryConfig{lastAccessResolution: 5 * time.Second}
	fe := NewLocalFileEntryFactory(config, clk).Create("test", state)

	require.NoError(fe.Create(state, 1))

	accessAndCheckLAT := func(expected time.Time) {
		_, err := fe.GetReader()
		require.NoError(err)

		lat, err := getLastAccessTime(fe)
		require.NoError(err)

		require.Equal(expected.Truncate(time.Second), lat.Truncate(time.Second))
	}

	// Advancing the clock within the resolution should not cause a metadata write.

	clk.Add(2 * time.Second)
	accessAndCheckLAT(t0)

	clk.Add(2 * time.Second)
	accessAndCheckLAT(t0)

	// Advancing the clock past the resolution should update metadata with current time.

	clk.Add(2 * time.Second)
	accessAndCheckLAT(clk.Now())
}
