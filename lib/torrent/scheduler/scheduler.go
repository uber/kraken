// Copyright (c) 2016-2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package scheduler

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/andres-erbsen/clock"
	"github.com/uber-go/tally"
	"go.uber.org/zap"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/torrent/networkevent"
	"github.com/uber/kraken/lib/torrent/scheduler/announcequeue"
	"github.com/uber/kraken/lib/torrent/scheduler/announcer"
	"github.com/uber/kraken/lib/torrent/scheduler/conn"
	"github.com/uber/kraken/lib/torrent/scheduler/connstate"
	"github.com/uber/kraken/lib/torrent/scheduler/torrentlog"
	"github.com/uber/kraken/lib/torrent/storage"
	"github.com/uber/kraken/tracker/announceclient"
	"github.com/uber/kraken/utils/log"
)

// Scheduler errors.
var (
	ErrTorrentNotFound   = errors.New("torrent not found")
	ErrSchedulerStopped  = errors.New("scheduler has been stopped")
	ErrTorrentTimeout    = errors.New("torrent timed out")
	ErrTorrentRemoved    = errors.New("torrent manually removed")
	ErrSendEventTimedOut = errors.New("event loop send timed out")
)

// Scheduler defines operations for scheduler.
type Scheduler interface {
	Stop()
	Download(namespace string, d core.Digest) error
	BlacklistSnapshot() ([]connstate.BlacklistedConn, error)
	RemoveTorrent(d core.Digest) error
	Probe() error
}

// scheduler manages global state for the peer. This includes:
// - Opening torrents.
// - Announcing to the tracker.
// - Handshaking incoming connections.
// - Initializing outgoing connections.
// - Dispatching connections to torrents.
// - Pre-empting existing connections when better options are available (TODO).
type scheduler struct {
	pctx           core.PeerContext
	config         Config
	clock          clock.Clock
	torrentArchive storage.TorrentArchive
	stats          tally.Scope

	handshaker *conn.Handshaker

	eventLoop *liftedEventLoop

	listener net.Listener

	preemptionTick <-chan time.Time
	emitStatsTick  <-chan time.Time

	// TODO(codyg): We only need this hold on this reference for reloading the scheduler...
	announceClient announceclient.Client

	announcer *announcer.Announcer

	netevents networkevent.Producer

	torrentlog *torrentlog.Logger

	logger *zap.SugaredLogger

	// The following fields orchestrate the stopping of the scheduler.
	stopOnce sync.Once      // Ensures the stop sequence is executed only once.
	done     chan struct{}  // Signals all goroutines to exit.
	wg       sync.WaitGroup // Waits for eventLoop and listenLoop to exit.
}

// schedOverrides defines scheduler fields which may be overrided for testing
// purposes.
type schedOverrides struct {
	clock     clock.Clock
	eventLoop eventLoop
}

type option func(*schedOverrides)

func withClock(c clock.Clock) option {
	return func(o *schedOverrides) { o.clock = c }
}

func withEventLoop(l eventLoop) option {
	return func(o *schedOverrides) { o.eventLoop = l }
}

// newScheduler creates and starts a scheduler.
func newScheduler(
	config Config,
	ta storage.TorrentArchive,
	stats tally.Scope,
	pctx core.PeerContext,
	announceClient announceclient.Client,
	netevents networkevent.Producer,
	options ...option) (*scheduler, error) {

	config = config.applyDefaults()

	logger, err := log.New(config.Log, nil)
	if err != nil {
		return nil, fmt.Errorf("log: %s", err)
	}
	slogger := logger.Sugar()

	done := make(chan struct{})

	stats = stats.Tagged(map[string]string{
		"module": "scheduler",
	})

	overrides := schedOverrides{
		clock:     clock.New(),
		eventLoop: newEventLoop(),
	}
	for _, opt := range options {
		opt(&overrides)
	}

	eventLoop := liftEventLoop(overrides.eventLoop)

	var preemptionTick <-chan time.Time
	if !config.DisablePreemption {
		preemptionTick = overrides.clock.Tick(config.PreemptionInterval)
	}

	handshaker, err := conn.NewHandshaker(
		config.Conn, stats, overrides.clock, netevents, pctx.PeerID, eventLoop, slogger)
	if err != nil {
		return nil, fmt.Errorf("conn: %s", err)
	}

	tlog, err := torrentlog.New(config.TorrentLog, pctx)
	if err != nil {
		return nil, fmt.Errorf("torrentlog: %s", err)
	}

	s := &scheduler{
		pctx:           pctx,
		config:         config,
		clock:          overrides.clock,
		torrentArchive: ta,
		stats:          stats,
		handshaker:     handshaker,
		eventLoop:      eventLoop,
		preemptionTick: preemptionTick,
		emitStatsTick:  overrides.clock.Tick(config.EmitStatsInterval),
		announceClient: announceClient,
		announcer:      announcer.Default(announceClient, eventLoop, overrides.clock, slogger),
		netevents:      netevents,
		torrentlog:     tlog,
		logger:         slogger,
		done:           done,
	}

	if config.DisablePreemption {
		s.log().Warn("Preemption disabled")
	}
	if config.ConnState.DisableBlacklist {
		s.log().Warn("Blacklisting disabled")
	}

	return s, nil
}

// start asynchronously starts all scheduler loops.
//
// Note: this has been split from the constructor so we can test against an
// "unstarted" scheduler in certain cases.
func (s *scheduler) start(aq announcequeue.Queue) error {
	s.log().Infof(
		"Scheduler starting as peer %s on addr %s:%d",
		s.pctx.PeerID, s.pctx.IP, s.pctx.Port)

	l, err := net.Listen("tcp", fmt.Sprintf(":%d", s.pctx.Port))
	if err != nil {
		return err
	}
	s.listener = l

	s.wg.Add(4)
	go s.runEventLoop(aq) // Careful, this should be the only reference to aq.
	go s.listenLoop()
	go s.tickerLoop()
	go s.announceLoop()

	return nil
}

// Stop shuts down the scheduler.
func (s *scheduler) Stop() {
	s.stopOnce.Do(func() {
		s.log().Info("Stopping scheduler...")

		close(s.done)
		s.listener.Close()
		s.eventLoop.send(shutdownEvent{})

		// Waits for all loops to stop.
		s.wg.Wait()

		s.torrentlog.Sync()

		s.log().Info("Scheduler stopped")
	})
}

func (s *scheduler) doDownload(namespace string, d core.Digest) (size int64, err error) {
	t, err := s.torrentArchive.CreateTorrent(namespace, d)
	if err != nil {
		if err == storage.ErrNotFound {
			return 0, ErrTorrentNotFound
		}
		return 0, fmt.Errorf("create torrent: %s", err)
	}

	// Buffer size of 1 so sends do not block.
	errc := make(chan error, 1)
	if !s.eventLoop.send(newTorrentEvent{namespace, t, errc}) {
		return 0, ErrSchedulerStopped
	}
	return t.Length(), <-errc
}

// Download downloads the torrent given metainfo. Once the torrent is downloaded,
// it will begin seeding asynchronously.
func (s *scheduler) Download(namespace string, d core.Digest) error {
	start := time.Now()
	size, err := s.doDownload(namespace, d)
	if err != nil {
		var errTag string
		switch err {
		case ErrTorrentNotFound:
			errTag = "not_found"
		case ErrTorrentTimeout:
			errTag = "timeout"
		case ErrSchedulerStopped:
			errTag = "scheduler_stopped"
		case ErrTorrentRemoved:
			errTag = "removed"
		default:
			errTag = "unknown"
		}
		s.stats.Tagged(map[string]string{
			"error": errTag,
		}).Counter("download_errors").Inc(1)
		s.torrentlog.DownloadFailure(namespace, d, size, err)
	} else {
		downloadTime := time.Since(start)
		recordDownloadTime(s.stats, size, downloadTime)
		s.torrentlog.DownloadSuccess(namespace, d, size, downloadTime)
	}
	return err
}

// BlacklistSnapshot returns a snapshot of the current connection blacklist.
func (s *scheduler) BlacklistSnapshot() ([]connstate.BlacklistedConn, error) {
	result := make(chan []connstate.BlacklistedConn)
	if !s.eventLoop.send(blacklistSnapshotEvent{result}) {
		return nil, ErrSchedulerStopped
	}
	return <-result, nil
}

// RemoveTorrent forcibly stops leeching / seeding torrent for d and removes
// the torrent from disk.
func (s *scheduler) RemoveTorrent(d core.Digest) error {
	// Buffer size of 1 so sends do not block.
	errc := make(chan error, 1)
	if !s.eventLoop.send(removeTorrentEvent{d, errc}) {
		return ErrSchedulerStopped
	}
	return <-errc
}

// Probe verifies that the scheduler event loop is running and unblocked.
func (s *scheduler) Probe() error {
	return s.eventLoop.sendTimeout(probeEvent{}, s.config.ProbeTimeout)
}

func (s *scheduler) runEventLoop(aq announcequeue.Queue) {
	defer s.wg.Done()

	s.eventLoop.run(newState(s, aq))
}

// listenLoop accepts incoming connections.
func (s *scheduler) listenLoop() {
	defer s.wg.Done()

	s.log().Infof("Listening on %s", s.listener.Addr().String())
	for {
		nc, err := s.listener.Accept()
		if err != nil {
			// TODO Need some way to make this gracefully exit.
			s.log().Infof("Error accepting new conn, exiting listen loop: %s", err)
			return
		}
		go func() {
			pc, err := s.handshaker.Accept(nc)
			if err != nil {
				s.log().Infof("Error accepting handshake, closing net conn: %s", err)
				nc.Close()
				return
			}
			s.eventLoop.send(incomingHandshakeEvent{pc})
		}()
	}
}

// tickerLoop periodically emits various tick events.
func (s *scheduler) tickerLoop() {
	defer s.wg.Done()

	for {
		select {
		case <-s.preemptionTick:
			s.eventLoop.send(preemptionTickEvent{})
		case <-s.emitStatsTick:
			s.eventLoop.send(emitStatsEvent{})
		case <-s.done:
			return
		}
	}
}

// announceLoop runs the announcer ticker.
func (s *scheduler) announceLoop() {
	defer s.wg.Done()

	s.announcer.Ticker(s.done)
}

func (s *scheduler) announce(d core.Digest, h core.InfoHash, complete bool) {
	peers, err := s.announcer.Announce(d, h, complete)
	if err != nil {
		if err != announceclient.ErrDisabled {
			s.eventLoop.send(announceErrEvent{h, err})
		}
		return
	}
	s.eventLoop.send(announceResultEvent{h, peers})
}

func (s *scheduler) failIncomingHandshake(pc *conn.PendingConn, err error) {
	s.log(
		"peer", pc.PeerID(),
		"hash", pc.InfoHash()).Infof("Error accepting incoming handshake: %s", err)
	pc.Close()
	s.eventLoop.send(failedIncomingHandshakeEvent{pc.PeerID(), pc.InfoHash()})
}

// establishIncomingHandshake attempts to establish a pending conn initialized
// by a remote peer. Success / failure is communicated via events.
func (s *scheduler) establishIncomingHandshake(pc *conn.PendingConn, rb conn.RemoteBitfields) {
	info, err := s.torrentArchive.Stat(pc.Namespace(), pc.Digest())
	if err != nil {
		s.failIncomingHandshake(pc, fmt.Errorf("torrent stat: %s", err))
		return
	}
	c, err := s.handshaker.Establish(pc, info, rb)
	if err != nil {
		s.failIncomingHandshake(pc, fmt.Errorf("establish handshake: %s", err))
		return
	}
	s.torrentlog.IncomingConnectionAccept(pc.Digest(), pc.InfoHash(), pc.PeerID())
	s.eventLoop.send(incomingConnEvent{pc.Namespace(), c, pc.Bitfield(), info})
}

// initializeOutgoingHandshake attempts to initialize a conn to a remote peer.
// Success / failure is communicated via events.
func (s *scheduler) initializeOutgoingHandshake(
	p *core.PeerInfo, info *storage.TorrentInfo, rb conn.RemoteBitfields, namespace string) {

	addr := fmt.Sprintf("%s:%d", p.IP, p.Port)
	result, err := s.handshaker.Initialize(p.PeerID, addr, info, rb, namespace)
	if err != nil {
		s.log(
			"peer", p.PeerID,
			"hash", info.InfoHash(),
			"addr", addr).Infof("Error initializing outgoing handshake: %s", err)
		s.eventLoop.send(failedOutgoingHandshakeEvent{p.PeerID, info.InfoHash()})
		s.torrentlog.OutgoingConnectionReject(info.Digest(), info.InfoHash(), p.PeerID, err)
		return
	}
	s.torrentlog.OutgoingConnectionAccept(info.Digest(), info.InfoHash(), p.PeerID)
	s.eventLoop.send(outgoingConnEvent{result.Conn, result.Bitfield, info})
}

func (s *scheduler) log(args ...interface{}) *zap.SugaredLogger {
	return s.logger.With(args...)
}
