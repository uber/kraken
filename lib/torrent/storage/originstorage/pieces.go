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
package originstorage

import (
	"fmt"
	"sync"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/store"
	"github.com/uber/kraken/utils/log"
)

// _pieceStatusSuffix matches the per-piece status metadata used by the partial
// (cold) origin torrent. The factory for this suffix is registered globally by
// agentstorage's init, which is always linked into the origin binary via the
// scheduler constructors, so we deliberately do not re-register it here.
const _pieceStatusSuffix = "_status"

type pieceStatus int

const (
	_empty pieceStatus = iota
	_complete
	_dirty
)

// pieceStatusMetadata stores piece statuses as metadata on disk, mirroring the
// agentstorage layout so a partial origin recovers completed pieces on restart.
type pieceStatusMetadata struct {
	pieces []*piece
}

func newPieceStatusMetadata(pieces []*piece) *pieceStatusMetadata {
	return &pieceStatusMetadata{pieces}
}

func (m *pieceStatusMetadata) GetSuffix() string {
	return _pieceStatusSuffix
}

func (m *pieceStatusMetadata) Movable() bool {
	return true
}

func (m *pieceStatusMetadata) Serialize() ([]byte, error) {
	b := make([]byte, len(m.pieces))
	for i, p := range m.pieces {
		b[i] = byte(p.status)
	}
	return b, nil
}

func (m *pieceStatusMetadata) Deserialize(b []byte) error {
	m.pieces = make([]*piece, len(b))
	for i := range b {
		status := pieceStatus(b[i])
		if status != _empty && status != _complete {
			log.Errorf("Unexpected status in piece metadata: %d", status)
			status = _empty
		}
		m.pieces[i] = &piece{status: status}
	}
	return nil
}

type piece struct {
	sync.RWMutex
	status pieceStatus
}

func (p *piece) snapshot() pieceStatus {
	p.RLock()
	defer p.RUnlock()
	return p.status
}

func (p *piece) complete() bool {
	return p.snapshot() == _complete
}

// tryMarkDirty attempts to claim the right to fetch the piece. Exactly one
// caller observes neither dirty nor complete and is responsible for the fetch.
func (p *piece) tryMarkDirty() (dirty, complete bool) {
	p.Lock()
	defer p.Unlock()

	switch p.status {
	case _empty:
		p.status = _dirty
	case _dirty:
		dirty = true
	case _complete:
		complete = true
	default:
		log.Fatalf("Unknown piece status: %d", p.status)
	}
	return
}

func (p *piece) markEmpty() {
	p.Lock()
	defer p.Unlock()
	p.status = _empty
}

func (p *piece) markComplete() {
	p.Lock()
	defer p.Unlock()
	p.status = _complete
}

// restorePieces restores in-memory piece statuses from on-disk metadata so a
// restarted origin does not re-fetch pieces it already streamed.
func restorePieces(
	d core.Digest,
	cads *store.CADownloadStore,
	numPieces int) (pieces []*piece, numComplete int, err error) {

	for i := 0; i < numPieces; i++ {
		pieces = append(pieces, &piece{status: _empty})
	}
	md := newPieceStatusMetadata(pieces)
	if err := cads.Download().GetOrSetMetadata(d.Hex(), md); cads.InCacheError(err) {
		for _, p := range pieces {
			p.status = _complete
		}
		return pieces, numPieces, nil
	} else if err != nil {
		return nil, 0, fmt.Errorf("get or set piece metadata: %s", err)
	}
	for _, p := range md.pieces {
		if p.status == _complete {
			numComplete++
		}
	}
	return md.pieces, numComplete, nil
}
