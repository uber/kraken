// Copyright (c) 2016-2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package networkevent

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber/kraken/core"
)

func TestProducerCreatesAndReusesFile(t *testing.T) {
	h := core.InfoHashFixture()
	peer1 := core.PeerIDFixture()
	peer2 := core.PeerIDFixture()

	dir := t.TempDir()

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
	require.NoError(t, err)
	for _, e := range events[:2] {
		p.Produce(e)
	}
	require.NoError(t, p.Close())

	// Second producer should reuse the existing file.
	p, err = NewProducer(config)
	require.NoError(t, err)
	for _, e := range events[2:] {
		p.Produce(e)
	}
	require.NoError(t, p.Close())

	f, err := os.Open(config.LogPath)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, f.Close())
	}()

	var results []*Event
	s := bufio.NewScanner(f)
	s.Split(bufio.ScanLines)
	for s.Scan() {
		e := new(Event)
		require.NoError(t, json.Unmarshal(s.Bytes(), e))
		results = append(results, e)
	}

	require.Equal(t, StripTimestamps(events), StripTimestamps(results))
}

func TestDisabledProducerNoops(t *testing.T) {
	h := core.InfoHashFixture()
	peer1 := core.PeerIDFixture()
	peer2 := core.PeerIDFixture()

	p, err := NewProducer(Config{})
	require.NoError(t, err)

	p.Produce(ReceivePieceEvent(h, peer1, peer2, 1))
}
