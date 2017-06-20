package torrent

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"code.uber.internal/infra/kraken/client/torrent/storage"
	"code.uber.internal/infra/kraken/utils"
	"code.uber.internal/infra/kraken/utils/testutil"
)

var TestingConfigOrigin = Config{
	ListenAddr: "127.0.0.1:4001",
	DataDir:    "/tmp/kraken",
	Debug:      true,
}

var TestingConfigPeer = Config{
	ListenAddr: "127.0.0.1:4002",
	DataDir:    "/tmp/peer",
	Debug:      true,
}

func assertReadAllTestTorrent(t *testing.T) {
	testf, err := ioutil.ReadFile(
		filepath.Join(TestingConfigPeer.DataDir, testutil.TestFileName)) // just pass the file name
	assert.NoError(t, err)
	assert.EqualValues(t, testutil.TestFileContents, string(testf))
}

func testClientTransfer(t *testing.T) {
	tempDir, mi := testutil.DummyTestTorrent()
	defer os.RemoveAll(tempDir)

	// Create origin and a Torrent.
	TestingConfigOrigin.DefaultStorage = storage.NewFileStorage(tempDir)
	origin, err := NewClient(&TestingConfigOrigin)
	require.NoError(t, err)
	defer origin.Close()

	_, new, err := origin.AddTorrentSpec(SpecFromMetaInfo(mi))
	require.NoError(t, err)
	assert.False(t, new)
}

func TestClientTransfer(t *testing.T) {

	tempDir, mi := testutil.DummyTestTorrent()
	defer os.RemoveAll(tempDir)

	// Create origin and a Torrent.
	TestingConfigOrigin.DefaultStorage = storage.NewFileStorage(tempDir)
	origin, err := NewClient(&TestingConfigOrigin)
	require.NoError(t, err)
	defer origin.Close()

	originTorrent, new, err := origin.AddTorrentSpec(SpecFromMetaInfo(mi))
	require.NoError(t, err)
	assert.True(t, new)

	defer os.RemoveAll(TestingConfigPeer.DataDir)
	defer os.RemoveAll(TestingConfigPeer.DataDir)

	TestingConfigPeer.DefaultStorage = storage.NewFileStorage(TestingConfigPeer.DataDir)
	peer, err := NewClient(&TestingConfigPeer)
	require.NoError(t, err)
	defer peer.Close()

	peerTorrent, new, err := peer.AddTorrentSpec(func() (ret *Spec) {
		ret = SpecFromMetaInfo(mi)
		return
	}())
	require.NoError(t, err)
	assert.True(t, new)

	ip, err := utils.AddrIP(origin.ListenAddr().String())
	require.NoError(t, err)

	port, err := utils.AddrPort(origin.ListenAddr().String())
	require.NoError(t, err)

	peerTorrent.AddPeers([]Peer{
		{
			IP:       ip,
			Port:     port,
			Priority: 0,
		},
	})

	ticker := time.NewTicker(2 * time.Second)
	quit := make(chan struct{})
	go func() {
		for {
			select {
			case <-ticker.C:
				if peerTorrent.IsComplete() {
					close(quit)
					peerTorrent.Close()
					originTorrent.Close()
				}

				// do stuff
			case <-quit:
				ticker.Stop()
				return
			}
		}
	}()

	peerTorrent.Wait()
	peerTorrent.Wait()

	assertReadAllTestTorrent(t)
}
