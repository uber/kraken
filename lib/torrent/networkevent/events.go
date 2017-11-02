package networkevent

import (
	"time"

	"code.uber.internal/infra/kraken/lib/torrent/storage"
	"code.uber.internal/infra/kraken/torlib"
)

// Name defines event names.
type Name string

// Possible event names.
const (
	AddTorrent       Name = "add_torrent"
	AddConn          Name = "add_conn"
	DropConn         Name = "drop_conn"
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
	Peer     string `json:"peer,omitempty"`
	Piece    int    `json:"piece,omitempty"`
	Bitfield []bool `json:"bitfield,omitempty"`
}

func baseEvent(name Name, h torlib.InfoHash, self torlib.PeerID) Event {
	return Event{
		Name:    name,
		Torrent: h.String(),
		Self:    self.String(),
		Time:    time.Now(),
	}
}

// AddTorrentEvent returns an event for an added torrent with initial bitfield.
func AddTorrentEvent(h torlib.InfoHash, self torlib.PeerID, bitfield storage.Bitfield) Event {
	e := baseEvent(AddTorrent, h, self)
	e.Bitfield = []bool(bitfield)
	return e
}

// AddConnEvent returns an event for an added conn from self to peer.
func AddConnEvent(h torlib.InfoHash, self torlib.PeerID, peer torlib.PeerID) Event {
	e := baseEvent(AddConn, h, self)
	e.Peer = peer.String()
	return e
}

// DropConnEvent returns an event for a dropped conn from self to peer.
func DropConnEvent(h torlib.InfoHash, self torlib.PeerID, peer torlib.PeerID) Event {
	e := baseEvent(DropConn, h, self)
	e.Peer = peer.String()
	return e
}

// ReceivePieceEvent returns an event for a piece received from peer.
func ReceivePieceEvent(h torlib.InfoHash, self torlib.PeerID, peer torlib.PeerID, piece int) Event {
	e := baseEvent(ReceivePiece, h, self)
	e.Peer = peer.String()
	e.Piece = piece
	return e
}

// TorrentCompleteEvent returns an event for a completed torrent.
func TorrentCompleteEvent(h torlib.InfoHash, self torlib.PeerID) Event {
	return baseEvent(TorrentComplete, h, self)
}

// TorrentCancelledEvent returns an event for a cancelled torrent.
func TorrentCancelledEvent(h torlib.InfoHash, self torlib.PeerID) Event {
	return baseEvent(TorrentCancelled, h, self)
}
