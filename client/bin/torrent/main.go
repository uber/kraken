package main

// This file is a just a sketch for
// a future agen main.go

import (
	"code.uber.internal/go-common.git/x/log"
	"flag"
	"os"
	"time"

	"code.uber.internal/infra/kraken/client/torrent"
	"code.uber.internal/infra/kraken/client/torrent/storage"

	"code.uber.internal/infra/kraken/utils"
	"code.uber.internal/infra/kraken/utils/testutil"
)

// TestingConfigOrigin is a test configuration for origin
var TestingConfigOrigin = torrent.Config{
	ListenAddr: "127.0.0.1:4001",
	DataDir:    "/tmp/kraken",
	Debug:      true,
}

// TestingConfigPeer is a test configuration for peer
var TestingConfigPeer = torrent.Config{
	ListenAddr: "127.0.0.1:4002",
	DataDir:    "/tmp/peer",
	Debug:      true,
}

func main() {
	var seed bool
	flag.BoolVar(&seed, "seed", true, "start client in a seeding mode")
	flag.Parse()

	logConfig := &log.Configuration{
		Level:  log.DebugLevel,
		Stdout: true,
	}
	log.Configure(logConfig, true)

	greetingTempDir, mi := testutil.DummyTestTorrent()
	defer os.RemoveAll(greetingTempDir)

	if seed {
		log.Info("running in a seed mode")
		greetingTempDir, mi := testutil.DummyTestTorrent()
		defer os.RemoveAll(greetingTempDir)

		// Create origin and a Torrent.
		TestingConfigOrigin.DefaultStorage = storage.NewFileStorage(greetingTempDir)
		TestingConfigOrigin.PeerID = ""
		origin, err := torrent.NewClient(&TestingConfigOrigin)
		defer origin.Close()
		if err != nil {
			log.Fatal(err)
			return
		}

		seedt, err := origin.AddTorrentSpec(torrent.SpecFromMetaInfo(mi))

		defer os.RemoveAll(TestingConfigPeer.DataDir)
		seedt.Wait()
	} else {
		log.Info("running in a peer mode")
		TestingConfigPeer.PeerID = ""

		TestingConfigPeer.DefaultStorage = storage.NewFileStorage(TestingConfigPeer.DataDir)
		peer, err := torrent.NewClient(&TestingConfigPeer)
		defer peer.Close()
		if err != nil {
			log.Error(err)
			return
		}

		peerGreeting, err := peer.AddTorrentSpec(func() *torrent.Spec {
			return torrent.SpecFromMetaInfo(mi)
		}())

		ip, err := utils.AddrIP("127.0.0.1:4001")
		port, err := utils.AddrPort("127.0.0.1:4001")
		if err != nil {
			log.Fatal(err)
			return
		}

		peerGreeting.AddPeers([]torrent.Peer{
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
					log.Infof("torrent complete %t: ", peerGreeting.IsComplete())
					if peerGreeting.IsComplete() {
						close(quit)
						peerGreeting.Close()
					}

					// do stuff
				case <-quit:
					ticker.Stop()
					return
				}
			}
		}()
		peerGreeting.Wait()
	}
	log.Info("Torrent agent shut down")
}
