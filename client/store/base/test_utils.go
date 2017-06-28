package base

import (
	"os"
	"regexp"
	"strings"
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

// Mock metadata
func init() {
	RegisterMetadata(regexp.MustCompile("_mocksuffix_\\w+"), &mockMetadataFactory{})
}

type mockMetadataFactory struct{}

func (f mockMetadataFactory) Create(suffix string) MetadataType {
	if strings.HasSuffix(suffix, getMockMetadataOne().GetSuffix()) {
		return getMockMetadataOne()
	}
	if strings.HasSuffix(suffix, getMockMetadataTwo().GetSuffix()) {
		return getMockMetadataTwo()
	}
	return nil
}

type mockMetadata struct {
	randomSuffix string
}

func getMockMetadataOne() MetadataType {
	return mockMetadata{
		randomSuffix: "_mocksuffix_one",
	}
}

func getMockMetadataTwo() MetadataType {
	return mockMetadata{
		randomSuffix: "_mocksuffix_two",
	}
}

func (m mockMetadata) GetSuffix() string {
	return m.randomSuffix
}

// Test file entry
func getTestFileEntry() (FileEntry, error) {
	// Setup
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
	backend := NewLocalFileStore(&LocalFileEntryInternalFactory{}, &LocalFileEntryFactory{})
	err := backend.CreateFile(testFileName, []FileState{}, stateTest1, 5)
	if err != nil {
		return nil, err
	}
	entry, _, err := backend.(*LocalFileStore).LoadFileEntry(testFileName, []FileState{stateTest1})
	return entry.(*LocalFileEntry), err
}

func cleanupTestFileEntry() {
	os.RemoveAll(testRoot)
}

func dummyVerify(entry FileEntry) error {
	return nil
}

// Test file store
func getTestFileStore() (*LocalFileStore, error) {
	// Setup
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
	backend := NewLocalFileStore(&LocalFileEntryInternalFactory{}, &LocalFileEntryFactory{})
	return backend.(*LocalFileStore), nil
}

func cleanupTestFileStore() {
	os.RemoveAll(testRoot)
}
