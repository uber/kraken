package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"

	"code.uber.internal/infra/kraken/lib/backend"
	"code.uber.internal/infra/kraken/lib/peercontext"
	"code.uber.internal/infra/kraken/lib/serverset"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/lib/torrent"
	torrentstorage "code.uber.internal/infra/kraken/lib/torrent/storage"
	"code.uber.internal/infra/kraken/metrics"
	"code.uber.internal/infra/kraken/origin/blobclient"
	"code.uber.internal/infra/kraken/origin/blobserver"
	"code.uber.internal/infra/kraken/origin/torrentserver"
	"code.uber.internal/infra/kraken/tracker/announceclient"
	"code.uber.internal/infra/kraken/utils/configutil"
	"code.uber.internal/infra/kraken/utils/log"
)

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

	// Stats
	stats, closer, err := metrics.New(config.Metrics)
	if err != nil {
		log.Fatalf("Failed to init metrics: %s", err)
	}
	defer closer.Close()

	// Root metrics scope for origin.
	stats = stats.SubScope("kraken.origin")

	// Initialize file storage.
	fs, err := store.NewOriginFileStore(config.OriginStore)
	if err != nil {
		log.Fatalf("Failed to create origin file store: %s", err)
	}

	var pctx peercontext.PeerContext

	// Initialize and start P2P scheduler client.
	if config.Torrent.Enabled {
		pctx, err = peercontext.NewOrigin(
			peercontext.PeerIDFactory(config.Torrent.PeerIDFactory), *zone, *peerIP, *peerPort)
		if err != nil {
			log.Fatalf("Failed to create peer context: %s", err)
		}

		trackers, err := serverset.NewRoundRobin(config.Tracker.RoundRobin)
		if err != nil {
			log.Fatalf("Error creating tracker round robin: %s", err)
		}

		archive := torrentstorage.NewOriginTorrentArchive(fs)

		c, err := torrent.NewSchedulerClient(
			config.Torrent,
			fs.(store.ReadOnlyFileStore),
			stats,
			pctx,
			// TODO(codyg): Get rid of this dependency.
			announceclient.Default(pctx, trackers),
			archive)
		if err != nil {
			log.Fatalf("Failed to create scheduler client: %s", err)
		}

		torrentServer := torrentserver.New(c)
		addr := fmt.Sprintf(":%d", *torrentServerPort)
		log.Infof("Starting torrent server on %s", addr)
		go func() {
			log.Fatal(http.ListenAndServe(addr, torrentServer.Handler()))
		}()
	} else {
		log.Warn("Torrent disabled")
	}

	var hostname string
	// The code below starts Blob HTTP server.
	if blobServerHostName == nil || *blobServerHostName == "" {
		hostname, err = os.Hostname()
		if err != nil {
			log.Fatalf("Error getting hostname: %s", err)
		}
	} else {
		hostname = *blobServerHostName
	}

	stats, closer, err = metrics.New(config.Metrics)
	if err != nil {
		log.Fatalf("Failed to create metrics: %s", err)
	}
	defer closer.Close()

	backendManager, err := backend.NewManager(config.Namespaces)
	if err != nil {
		log.Fatalf("Error creating backend manager: %s", err)
	}

	addr := fmt.Sprintf("%s:%d", hostname, *blobServerPort)
	server, err := blobserver.New(
		config.BlobServer,
		stats,
		addr,
		fs,
		blobclient.NewProvider(),
		pctx,
		backendManager)
	if err != nil {
		log.Fatalf("Error initializing blob server %s: %s", addr, err)
	}

	log.Infof("Starting origin server %s on %d", hostname, *blobServerPort)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", *blobServerPort), server.Handler()))
}
