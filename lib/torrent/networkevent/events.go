package networkevent

import (
	"encoding/json"
	"time"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/utils/log"

	"github.com/willf/bitset"
)

// Name defines event names.
type Name string

// Possible event names.
const (
	AddTorrent       Name = "add_torrent"
	AddActiveConn    Name = "add_active_conn"
	DropActiveConn   Name = "drop_active_conn"
	BlacklistConn    Name = "blacklist_conn"
	ReceivePiece     Name = "receive_piece"
	TorrentComplete  Name = "torrent_complete"
	TorrentCancelled Name = "torrent_cancelled"
)

// Event consolidates all possible event fields.
type Event struct {
	Name    Name      `json:"event"`
	Torrent string    `json:"torrent"`
	Self    string    `json:"self"`
	Time    time.Time `json:"ts"`

	// Optional fields.
	Peer         string `json:"peer,omitempty"`
	Piece        int    `json:"piece,omitempty"`
	Bitfield     []bool `json:"bitfield,omitempty"`
	DurationMS   int64  `json:"duration_ms,omitempty"`
	ConnCapacity int    `json:"conn_capacity,omitempty"`
}

func baseEvent(name Name, h core.InfoHash, self core.PeerID) *Event {
	return &Event{
		Name:    name,
		Torrent: h.String(),
		Self:    self.String(),
		Time:    time.Now(),
	}
}

// JSON converts event into a json string primarely for logging purposes
func (e *Event) JSON() string {
	b, err := json.Marshal(e)
	if err != nil {
		log.Errorf("json marshal error %s", err)
		return ""
	}
	return string(b)
}

// AddTorrentEvent returns an event for an added torrent with initial bitfield.
func AddTorrentEvent(h core.InfoHash, self core.PeerID, b *bitset.BitSet, connCapacity int) *Event {
	e := baseEvent(AddTorrent, h, self)
	bools := make([]bool, b.Len())
	for i := uint(0); i < b.Len(); i++ {
		bools[i] = b.Test(i)
	}
	e.Bitfield = bools
	e.ConnCapacity = connCapacity
	return e
}

// AddActiveConnEvent returns an event for an added active conn from self to peer.
func AddActiveConnEvent(h core.InfoHash, self core.PeerID, peer core.PeerID) *Event {
	e := baseEvent(AddActiveConn, h, self)
	e.Peer = peer.String()
	return e
}

// DropActiveConnEvent returns an event for a dropped active conn from self to peer.
func DropActiveConnEvent(h core.InfoHash, self core.PeerID, peer core.PeerID) *Event {
	e := baseEvent(DropActiveConn, h, self)
	e.Peer = peer.String()
	return e
}

// BlacklistConnEvent returns an event for a blacklisted connection.
func BlacklistConnEvent(h core.InfoHash, self core.PeerID, peer core.PeerID, dur time.Duration) *Event {
	e := baseEvent(BlacklistConn, h, self)
	e.Peer = peer.String()
	e.DurationMS = int64(dur.Seconds() * 1000)
	return e
}

// ReceivePieceEvent returns an event for a piece received from peer.
func ReceivePieceEvent(h core.InfoHash, self core.PeerID, peer core.PeerID, piece int) *Event {
	e := baseEvent(ReceivePiece, h, self)
	e.Peer = peer.String()
	e.Piece = piece
	return e
}

// TorrentCompleteEvent returns an event for a completed torrent.
func TorrentCompleteEvent(h core.InfoHash, self core.PeerID) *Event {
	return baseEvent(TorrentComplete, h, self)
}

// TorrentCancelledEvent returns an event for a cancelled torrent.
func TorrentCancelledEvent(h core.InfoHash, self core.PeerID) *Event {
	return baseEvent(TorrentCancelled, h, self)
}
