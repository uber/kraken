package refcountable

import (
	"os"

	"code.uber.internal/infra/kraken/lib/store/base"
)

// Mock state
const (
	testRoot     = "./.tmp/test/"
	testDir1     = "./.tmp/test/test1"
	testDir2     = "./.tmp/test/test2"
	testDir3     = "./.tmp/test/test3"
	testFileName = "test_file.txt"
)

type mockFileState int

const (
	stateTest1 mockFileState = iota
	stateTest2
	stateTest3
)

var _mockFileStateLookup = [...]string{testDir1, testDir2, testDir3}

func (state mockFileState) GetDirectory() string { return _mockFileStateLookup[state] }

// Test file entry
func getTestRCFileEntry() (*LocalRCFileStore, RCFileEntry, error) {
	if _, err := os.Stat(testRoot); os.IsNotExist(err) {
		os.MkdirAll(testRoot, 0777)
	}
	if _, err := os.Stat(testDir1); os.IsNotExist(err) {
		os.MkdirAll(testDir1, 0777)
	}
	if _, err := os.Stat(testDir2); os.IsNotExist(err) {
		os.MkdirAll(testDir2, 0777)
	}
	if _, err := os.Stat(testDir3); os.IsNotExist(err) {
		os.MkdirAll(testDir3, 0777)
	}

	// Create empty file
	backend, err := NewLocalRCFileStoreDefault()
	if err != nil {
		return nil, nil, err
	}

	err = backend.CreateFile(testFileName, []base.FileState{}, stateTest1, 5)
	if err != nil {
		return nil, nil, err
	}
	entry, _, err := backend.(*LocalRCFileStore).LoadFileEntry(testFileName, []base.FileState{stateTest1})
	return backend.(*LocalRCFileStore), entry.(*LocalRCFileEntry), err
}

func cleanupTestRCFileEntry() {
	os.RemoveAll(testRoot)
}

func dummyVerify(entry base.FileEntry) error {
	return nil
}
