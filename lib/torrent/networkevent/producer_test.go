package networkevent

import (
	"bufio"
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"code.uber.internal/infra/kraken/core"

	"github.com/stretchr/testify/require"
)

func TestProducerCreatesAndReusesFile(t *testing.T) {
	require := require.New(t)

	h := core.InfoHashFixture()
	peer1 := core.PeerIDFixture()
	peer2 := core.PeerIDFixture()

	dir, err := ioutil.TempDir("", "")
	require.NoError(err)
	defer os.RemoveAll(dir)

	config := Config{
		Enabled: true,
		LogPath: filepath.Join(dir, "netevents"),
	}

	events := []*Event{
		ReceivePieceEvent(h, peer1, peer2, 1),
		ReceivePieceEvent(h, peer1, peer2, 2),
		ReceivePieceEvent(h, peer1, peer2, 3),
		ReceivePieceEvent(h, peer1, peer2, 4),
	}

	// First producer should create the file.
	p, err := NewProducer(config)
	require.NoError(err)
	for _, e := range events[:2] {
		p.Produce(e)
	}
	require.NoError(p.Close())

	// Second producer should reuse the existing file.
	p, err = NewProducer(config)
	require.NoError(err)
	for _, e := range events[2:] {
		p.Produce(e)
	}
	require.NoError(p.Close())

	f, err := os.Open(config.LogPath)
	require.NoError(err)
	defer f.Close()

	var results []*Event
	s := bufio.NewScanner(f)
	s.Split(bufio.ScanLines)
	for s.Scan() {
		e := new(Event)
		require.NoError(json.Unmarshal(s.Bytes(), e))
		results = append(results, e)
	}

	require.Equal(StripTimestamps(events), StripTimestamps(results))
}

func TestDisabledProducerNoops(t *testing.T) {
	require := require.New(t)

	h := core.InfoHashFixture()
	peer1 := core.PeerIDFixture()
	peer2 := core.PeerIDFixture()

	p, err := NewProducer(Config{})
	require.NoError(err)

	p.Produce(ReceivePieceEvent(h, peer1, peer2, 1))
}
