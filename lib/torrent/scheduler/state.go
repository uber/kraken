package scheduler

import (
	"errors"
	"fmt"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/torrent/networkevent"
	"code.uber.internal/infra/kraken/lib/torrent/scheduler/announcequeue"
	"code.uber.internal/infra/kraken/lib/torrent/scheduler/conn"
	"code.uber.internal/infra/kraken/lib/torrent/scheduler/connstate"
	"code.uber.internal/infra/kraken/lib/torrent/scheduler/dispatch"
	"code.uber.internal/infra/kraken/lib/torrent/storage"
	"go.uber.org/zap"

	"github.com/willf/bitset"
)

// torrentControl bundles torrent control structures.
type torrentControl struct {
	namespace    string
	dispatcher   *dispatch.Dispatcher
	errors       []chan error
	localRequest bool
}

// state is a superset of scheduler, which includes protected state which can
// only be accessed from the event loop. state is free to access scheduler fields
// and methods, however scheduler has no reference to state.
//
// Any network I/O, such as opening connections, does not belong at the state
// level. These operations should be defined as scheduler methods, and executed
// from a separate goroutine when calling from the event loop. Results from I/O
// may transform state by sending events into the event loop.
type state struct {
	sched *scheduler

	// Protected state.
	torrentControls map[core.InfoHash]*torrentControl
	conns           *connstate.State
	announceQueue   announcequeue.Queue
}

// addTorrent initializes a new torrentControl for t. Overwrites any existing
// torrentControl for t, so callers should check if one exists first.
func (s *state) addTorrent(
	namespace string, t storage.Torrent, localRequest bool) (*torrentControl, error) {

	d, err := dispatch.New(
		s.sched.config.Dispatch,
		s.sched.stats,
		s.sched.clock,
		s.sched.netevents,
		s.sched.eventLoop,
		s.sched.pctx.PeerID,
		t,
		s.sched.logger,
		s.sched.torrentlog)
	if err != nil {
		return nil, fmt.Errorf("new dispatcher: %s", err)
	}
	ctrl := &torrentControl{
		namespace:    namespace,
		dispatcher:   d,
		localRequest: localRequest,
	}
	s.announceQueue.Add(t.InfoHash())
	s.sched.netevents.Produce(networkevent.AddTorrentEvent(
		t.InfoHash(),
		s.sched.pctx.PeerID,
		t.Bitfield(),
		s.sched.config.ConnState.MaxOpenConnectionsPerTorrent))
	s.torrentControls[t.InfoHash()] = ctrl
	return ctrl, nil
}

// removeTorrent tears down the torrentControl associated with h, sending err to
// all clients waiting on this torrent.
func (s *state) removeTorrent(h core.InfoHash, err error) {
	ctrl, ok := s.torrentControls[h]
	if !ok {
		return
	}
	if !ctrl.dispatcher.Complete() {
		ctrl.dispatcher.TearDown()
		s.announceQueue.Eject(h)
		for _, errc := range ctrl.errors {
			errc <- err
		}
		s.sched.netevents.Produce(networkevent.TorrentCancelledEvent(h, s.sched.pctx.PeerID))
		s.sched.torrentArchive.DeleteTorrent(ctrl.dispatcher.Digest())
	}
	delete(s.torrentControls, h)
}

// addOutgoingConn adds a conn, initialized by us, to state. The conn must already
// be in a pending state, and the torrent control must already be initialized.
func (s *state) addOutgoingConn(c *conn.Conn, b *bitset.BitSet, info *storage.TorrentInfo) error {
	if err := s.conns.MovePendingToActive(c); err != nil {
		return fmt.Errorf("move pending to active: %s", err)
	}
	ctrl, ok := s.torrentControls[info.InfoHash()]
	if !ok {
		return errors.New("torrent controls must be created before sending handshake")
	}
	if err := ctrl.dispatcher.AddPeer(c.PeerID(), b, c); err != nil {
		return fmt.Errorf("add conn to dispatcher: %s", err)
	}
	return nil
}

// addIncomingConn adds a conn, initialized by a remote peer, to state. The conn
// must already be in a pending state. Initializes a torrent control if not
// present.
func (s *state) addIncomingConn(
	namespace string, c *conn.Conn, b *bitset.BitSet, info *storage.TorrentInfo) error {

	if err := s.conns.MovePendingToActive(c); err != nil {
		return fmt.Errorf("move pending to active: %s", err)
	}
	ctrl, ok := s.torrentControls[info.InfoHash()]
	if !ok {
		t, err := s.sched.torrentArchive.GetTorrent(namespace, info.Digest())
		if err != nil {
			return fmt.Errorf("get torrent: %s", err)
		}
		ctrl, err = s.addTorrent(namespace, t, false)
		if err != nil {
			return err
		}
	}
	if err := ctrl.dispatcher.AddPeer(c.PeerID(), b, c); err != nil {
		return fmt.Errorf("add conn to dispatcher: %s", err)
	}
	return nil
}

func (s *state) log(args ...interface{}) *zap.SugaredLogger {
	return s.sched.log(args...)
}
