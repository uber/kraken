package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/backend"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/lib/torrent"
	"code.uber.internal/infra/kraken/lib/torrent/announcequeue"
	torrentstorage "code.uber.internal/infra/kraken/lib/torrent/storage"
	"code.uber.internal/infra/kraken/metrics"
	"code.uber.internal/infra/kraken/origin/blobclient"
	"code.uber.internal/infra/kraken/origin/blobserver"
	"code.uber.internal/infra/kraken/origin/torrentserver"
	"code.uber.internal/infra/kraken/tracker/announceclient"
	"code.uber.internal/infra/kraken/utils/configutil"
	"code.uber.internal/infra/kraken/utils/log"

	"github.com/andres-erbsen/clock"
)

func runTorrentServer(server *torrentserver.Server, port int) {
	addr := fmt.Sprintf(":%d", port)
	log.Infof("Starting torrent server on %s", addr)
	log.Fatal(http.ListenAndServe(addr, server.Handler()))
}

func runBlobServer(server *blobserver.Server, port int) {
	addr := fmt.Sprintf(":%d", port)
	log.Infof("Starting blob server on %d", addr)
	log.Fatal(http.ListenAndServe(addr, server.Handler()))
}

func main() {
	blobServerPort := flag.Int("blobserver_port", 0, "port which registry listens on")
	blobServerHostName := flag.String("blobserver_hostname", "", "optional hostname which blobserver will use to lookup a local host in a blob server hashnode config")
	peerIP := flag.String("peer_ip", "", "ip which peer will announce itself as")
	peerPort := flag.Int("peer_port", 0, "port which peer will announce itself as")
	configFile := flag.String("config", "", "Configuration file that has to be loaded from one of UBER_CONFIG_DIR locations")
	torrentServerPort := flag.Int("torrent_server_port", 0, "port which torrent server will listen on")
	zone := flag.String("zone", "", "zone/datacenter name")

	flag.Parse()

	if blobServerPort == nil || *blobServerPort == 0 {
		panic("0 is not a valid port for blob server")
	}
	if *torrentServerPort == 0 {
		panic("-torrent_server_port must be non-zero")
	}

	var config Config
	if err := configutil.Load(*configFile, &config); err != nil {
		panic(err)
	}

	zlog := log.ConfigureLogger(config.ZapLogging)
	defer zlog.Sync()

	stats, closer, err := metrics.New(config.Metrics)
	if err != nil {
		log.Fatalf("Failed to init metrics: %s", err)
	}
	defer closer.Close()

	fs, err := store.NewOriginFileStore(config.OriginStore, clock.New())
	if err != nil {
		log.Fatalf("Failed to create origin file store: %s", err)
	}

	pctx, err := core.NewPeerContext(
		core.PeerIDFactory(config.Torrent.PeerIDFactory), *zone, *peerIP, *peerPort, true)
	if err != nil {
		log.Fatalf("Failed to create peer context: %s", err)
	}

	c, err := torrent.NewSchedulerClient(
		config.Torrent,
		stats,
		pctx,
		announceclient.Disabled(),
		announcequeue.Disabled(),
		torrentstorage.NewOriginTorrentArchive(fs))
	if err != nil {
		log.Fatalf("Failed to create scheduler client: %s", err)
	}

	go runTorrentServer(torrentserver.New(c), *torrentServerPort)

	var hostname string
	if blobServerHostName == nil || *blobServerHostName == "" {
		hostname, err = os.Hostname()
		if err != nil {
			log.Fatalf("Error getting hostname: %s", err)
		}
	} else {
		hostname = *blobServerHostName
	}
	log.Infof("Configuring blob server with hostname '%s'", hostname)

	backendManager, err := backend.NewManager(config.Namespace)
	if err != nil {
		log.Fatalf("Error creating backend manager: %s", err)
	}

	bs, err := blobserver.New(
		config.BlobServer,
		stats,
		fmt.Sprintf("%s:%d", hostname, *blobServerPort),
		fs,
		blobclient.NewProvider(),
		pctx,
		backendManager)
	if err != nil {
		log.Fatalf("Error initializing blob server: %s", err)
	}
	go runBlobServer(bs, *blobServerPort)

	select {}
}
