package base

import (
	"log"
	"os"
	"path"
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
	RegisterMetadata(regexp.MustCompile("_mocksuffix_movable"), &mockMetadataFactoryMovable{})
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

func (m mockMetadata) Movable() bool {
	return false
}

type mockMetadataFactoryMovable struct{}

func (f mockMetadataFactoryMovable) Create(suffix string) MetadataType {
	if strings.HasSuffix(suffix, getMockMetadataMovable().GetSuffix()) {
		return getMockMetadataMovable()
	}
	return nil
}

type mockMetadataMovable struct {
	randomSuffix string
}

func getMockMetadataMovable() MetadataType {
	return mockMetadataMovable{
		randomSuffix: "_mocksuffix_movable",
	}
}

func (m mockMetadataMovable) GetSuffix() string {
	return m.randomSuffix
}

func (m mockMetadataMovable) Movable() bool {
	return true
}

// Test file entry
func getTestFileEntry() (FileEntry, func()) {
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
	backend := NewLocalFileStore(&ShardedFileEntryInternalFactory{}, &LocalFileEntryFactory{})
	if err := backend.CreateFile(testFileName, []FileState{}, stateTest1, 5); err != nil {
		log.Panic(err)
	}
	entry, _, err := backend.(*LocalFileStore).LoadFileEntry(testFileName, []FileState{stateTest1})
	if err != nil {
		log.Panic(err)
	}
	cleanup := func() {
		if err := os.RemoveAll(testRoot); err != nil {
			log.Panic(err)
		}
	}
	return entry.(*LocalFileEntry), cleanup
}

func dummyVerify(entry FileEntry) error {
	return nil
}

// Test file store
func getTestFileStore() (*LocalFileStore, func()) {
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
	backend := NewLocalFileStore(&ShardedFileEntryInternalFactory{}, &LocalFileEntryFactory{})

	cleanup := func() {
		os.RemoveAll(testRoot)
	}
	return backend.(*LocalFileStore), cleanup
}

func getShardedRelativePath(name string) string {
	filePath := ""
	for i := 0; i < DefaultShardIDLength && i < len(name)/2; i++ {
		// (1 byte = 2 char of file name assumming file name is in HEX)
		dirName := name[i*2 : i*2+2]
		filePath = path.Join(filePath, dirName)
	}

	return path.Join(filePath, name)
}
