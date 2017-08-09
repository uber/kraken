package testutil

import (
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"code.uber.internal/infra/kraken/client/torrent/bencode"
	"code.uber.internal/infra/kraken/client/torrent/meta"
)

const (
	// TestFileContents is test content
	TestFileContents = "When I grow up then will be a day and everybody has to do what I say\n"
	// TestFileName is test file name
	TestFileName = "clawfinger"
)

// CreateDummyTorrentData creates a content for torrent
func CreateDummyTorrentData(dirName string) string {
	f, _ := os.Create(filepath.Join(dirName, TestFileName))
	defer f.Close()
	f.WriteString(TestFileContents)
	return f.Name()
}

// DummyTorrentMetaInfo generates torrent meta info based on a torrent data
func DummyTorrentMetaInfo() *meta.TorrentInfo {
	info := meta.Info{
		Name:        TestFileName,
		Length:      int64(len(TestFileContents)),
		PieceLength: 5,
	}
	err := info.GeneratePieces(func(meta.FileInfo) (io.ReadCloser, error) {
		return ioutil.NopCloser(strings.NewReader(TestFileContents)), nil
	})
	if err != nil {
		panic(err)
	}
	mi := &meta.TorrentInfo{}
	mi.InfoBytes, err = bencode.Marshal(info)
	if err != nil {
		panic(err)
	}
	return mi
}

// DummyTestTorrent gives a temporary directory containing the completed "greeting" torrent,
// and a corresponding metainfo describing it. The temporary directory can be
// cleaned away with os.RemoveAll.
func DummyTestTorrent() (tempDir string, metaInfo *meta.TorrentInfo) {
	tempDir, err := ioutil.TempDir(os.TempDir(), "")
	if err != nil {
		panic(err)
	}
	CreateDummyTorrentData(tempDir)
	metaInfo = DummyTorrentMetaInfo()
	return
}

// PollUntilTrue calls f until f returns true. Fails if true is not received
// within timeout.
func PollUntilTrue(t *testing.T, timeout time.Duration, f func() bool) {
	timer := time.NewTimer(timeout)
	for {
		result := make(chan bool, 1)
		go func() {
			result <- f()
		}()
		select {
		case ok := <-result:
			if ok {
				return
			}
			time.Sleep(100 * time.Millisecond)
		case <-timer.C:
			t.Fatalf("PollUntilTrue timed out after %.2f seconds", timeout.Seconds())
		}
	}
}
