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
	AddPendingConn   Name = "add_pending_conn"
	DropPendingConn  Name = "drop_pending_conn"
	AddActiveConn    Name = "add_active_conn"
	DropActiveConn   Name = "drop_active_conn"
	BlacklistConn    Name = "blacklist_conn"
	ReceivePiece     Name = "receive_piece"
	TorrentComplete  Name = "torrent_complete"
	TorrentCancelled Name = "torrent_cancelled"

	// Piece request timing:

	// Leecher
	DispatcherSentPieceRequest Name = "dispatcher_sent_piece_request"
	ConnSenderGotPieceRequest  Name = "conn_sender_got_piece_request"
	ConnSenderSentPieceRequest Name = "conn_sender_sent_piece_request"

	// Seeder
	ConnReaderGotPieceRequest  Name = "conn_reader_got_piece_request"
	ConnReaderSentPieceRequest Name = "conn_reader_sent_piece_request"
	DispatcherGotPieceRequest  Name = "dispatcher_got_piece_request"
	DispatcherReadPiece        Name = "dispatcher_read_piece"
	DispatcherSentPiecePayload Name = "dispatcher_sent_piece_payload"
	ConnSenderGotPiecePayload  Name = "conn_sender_got_piece_payload"
	ConnSenderSentPiecePayload Name = "conn_sender_sent_piece_payload"

	// Leecher
	ConnReaderGotPiecePayload  Name = "conn_reader_got_piece_payload"
	ConnReaderSentPiecePayload Name = "conn_reader_sent_piece_payload"
	DispatcherGotPiecePayload  Name = "dispatcher_got_piece_payload"
	DispatcherWrotePiece       Name = "dispatcher_wrote_piece"

	// Errors
	ConnSendDroppedPieceRequest Name = "conn_send_dropped_piece_request"
	ConnSendDroppedPiecePayload Name = "conn_send_dropped_piece_payload"
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

// AddPendingConnEvent returns an event for an added pending conn from self to peer.
func AddPendingConnEvent(h core.InfoHash, self core.PeerID, peer core.PeerID) *Event {
	e := baseEvent(AddPendingConn, h, self)
	e.Peer = peer.String()
	return e
}

// DropPendingConnEvent returns an event for a dropped pending conn from self to peer.
func DropPendingConnEvent(h core.InfoHash, self core.PeerID, peer core.PeerID) *Event {
	e := baseEvent(DropPendingConn, h, self)
	e.Peer = peer.String()
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

func pieceEvent(
	name Name, h core.InfoHash, self core.PeerID, peer core.PeerID, piece int) *Event {

	e := baseEvent(name, h, self)
	e.Peer = peer.String()
	e.Piece = piece
	return e
}

// DispatcherSentPieceRequestEvent ...
func DispatcherSentPieceRequestEvent(
	h core.InfoHash, self core.PeerID, peer core.PeerID, piece int) *Event {

	return pieceEvent(DispatcherSentPieceRequest, h, self, peer, piece)
}

// ConnSenderSentPieceRequestEvent ...
func ConnSenderSentPieceRequestEvent(
	h core.InfoHash, self core.PeerID, peer core.PeerID, piece int) *Event {

	return pieceEvent(ConnSenderSentPieceRequest, h, self, peer, piece)
}

// ConnSenderGotPieceRequestEvent ...
func ConnSenderGotPieceRequestEvent(
	h core.InfoHash, self core.PeerID, peer core.PeerID, piece int) *Event {

	return pieceEvent(ConnSenderGotPieceRequest, h, self, peer, piece)
}

// ConnReaderGotPieceRequestEvent ...
func ConnReaderGotPieceRequestEvent(
	h core.InfoHash, self core.PeerID, peer core.PeerID, piece int) *Event {

	return pieceEvent(ConnReaderGotPieceRequest, h, self, peer, piece)
}

// ConnReaderSentPieceRequestEvent ...
func ConnReaderSentPieceRequestEvent(
	h core.InfoHash, self core.PeerID, peer core.PeerID, piece int) *Event {

	return pieceEvent(ConnReaderSentPieceRequest, h, self, peer, piece)
}

// DispatcherGotPieceRequestEvent ...
func DispatcherGotPieceRequestEvent(
	h core.InfoHash, self core.PeerID, peer core.PeerID, piece int) *Event {

	return pieceEvent(DispatcherGotPieceRequest, h, self, peer, piece)
}

// DispatcherReadPieceEvent ...
func DispatcherReadPieceEvent(
	h core.InfoHash, self core.PeerID, peer core.PeerID, piece int) *Event {

	return pieceEvent(DispatcherReadPiece, h, self, peer, piece)
}

// DispatcherSentPiecePayloadEvent ...
func DispatcherSentPiecePayloadEvent(
	h core.InfoHash, self core.PeerID, peer core.PeerID, piece int) *Event {

	return pieceEvent(DispatcherSentPiecePayload, h, self, peer, piece)
}

// ConnSenderGotPiecePayloadEvent ...
func ConnSenderGotPiecePayloadEvent(
	h core.InfoHash, self core.PeerID, peer core.PeerID, piece int) *Event {

	return pieceEvent(ConnSenderGotPiecePayload, h, self, peer, piece)
}

// ConnSenderSentPiecePayloadEvent ...
func ConnSenderSentPiecePayloadEvent(
	h core.InfoHash, self core.PeerID, peer core.PeerID, piece int) *Event {

	return pieceEvent(ConnSenderSentPiecePayload, h, self, peer, piece)
}

// ConnReaderGotPiecePayloadEvent ...
func ConnReaderGotPiecePayloadEvent(
	h core.InfoHash, self core.PeerID, peer core.PeerID, piece int) *Event {

	return pieceEvent(ConnReaderGotPiecePayload, h, self, peer, piece)
}

// ConnReaderSentPiecePayloadEvent ...
func ConnReaderSentPiecePayloadEvent(
	h core.InfoHash, self core.PeerID, peer core.PeerID, piece int) *Event {

	return pieceEvent(ConnReaderSentPiecePayload, h, self, peer, piece)
}

// DispatcherGotPiecePayloadEvent ...
func DispatcherGotPiecePayloadEvent(
	h core.InfoHash, self core.PeerID, peer core.PeerID, piece int) *Event {

	return pieceEvent(DispatcherGotPiecePayload, h, self, peer, piece)
}

// DispatcherWrotePieceEvent ...
func DispatcherWrotePieceEvent(
	h core.InfoHash, self core.PeerID, peer core.PeerID, piece int) *Event {

	return pieceEvent(DispatcherWrotePiece, h, self, peer, piece)
}

// ConnSendDroppedPieceRequestEvent ...
func ConnSendDroppedPieceRequestEvent(
	h core.InfoHash, self core.PeerID, peer core.PeerID, piece int) *Event {

	return pieceEvent(ConnSendDroppedPieceRequest, h, self, peer, piece)
}

// ConnSendDroppedPiecePayloadEvent ...
func ConnSendDroppedPiecePayloadEvent(
	h core.InfoHash, self core.PeerID, peer core.PeerID, piece int) *Event {

	return pieceEvent(ConnSendDroppedPiecePayload, h, self, peer, piece)
}
