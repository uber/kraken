package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/backend"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/lib/torrent"
	"code.uber.internal/infra/kraken/lib/torrent/announcequeue"
	"code.uber.internal/infra/kraken/lib/torrent/scheduler"
	torrentstorage "code.uber.internal/infra/kraken/lib/torrent/storage"
	"code.uber.internal/infra/kraken/metrics"
	"code.uber.internal/infra/kraken/origin/blobclient"
	"code.uber.internal/infra/kraken/origin/blobserver"
	"code.uber.internal/infra/kraken/tracker/announceclient"
	"code.uber.internal/infra/kraken/utils/configutil"
	"code.uber.internal/infra/kraken/utils/handler"
	"code.uber.internal/infra/kraken/utils/log"

	"github.com/andres-erbsen/clock"
	"github.com/pressly/chi"
)

// addTorrentDebugEndpoints mounts experimental debugging endpoints which are
// compatible with the agent server.
func addTorrentDebugEndpoints(h http.Handler, c torrent.Client) http.Handler {
	r := chi.NewRouter()

	r.Patch("/x/config/scheduler", handler.Wrap(func(w http.ResponseWriter, r *http.Request) error {
		var config scheduler.Config
		if err := json.NewDecoder(r.Body).Decode(&config); err != nil {
			return handler.Errorf("decode body: %s", err)
		}
		c.Reload(config)
		return nil
	}))

	r.Get("/x/blacklist", handler.Wrap(func(w http.ResponseWriter, r *http.Request) error {
		blacklist, err := c.BlacklistSnapshot()
		if err != nil {
			return handler.Errorf("blacklist snapshot: %s", err)
		}
		if err := json.NewEncoder(w).Encode(&blacklist); err != nil {
			return handler.Errorf("encode blacklist: %s", err)
		}
		return nil
	}))

	r.Mount("/", h)

	return r
}

func main() {
	blobServerPort := flag.Int("blobserver_port", 0, "port which registry listens on")
	blobServerHostName := flag.String("blobserver_hostname", "", "optional hostname which blobserver will use to lookup a local host in a blob server hashnode config")
	peerIP := flag.String("peer_ip", "", "ip which peer will announce itself as")
	peerPort := flag.Int("peer_port", 0, "port which peer will announce itself as")
	configFile := flag.String("config", "", "Configuration file that has to be loaded from one of UBER_CONFIG_DIR locations")
	zone := flag.String("zone", "", "zone/datacenter name")
	cluster := flag.String("cluster", "", "cluster name (e.g. prod01-sjc1)")

	flag.Parse()

	if blobServerPort == nil || *blobServerPort == 0 {
		panic("0 is not a valid port for blob server")
	}

	var hostname string
	if blobServerHostName == nil || *blobServerHostName == "" {
		var err error
		hostname, err = os.Hostname()
		if err != nil {
			log.Fatalf("Error getting hostname: %s", err)
		}
	} else {
		hostname = *blobServerHostName
	}
	log.Infof("Configuring origin with hostname '%s'", hostname)

	var config Config
	if err := configutil.Load(*configFile, &config); err != nil {
		panic(err)
	}

	zlog := log.ConfigureLogger(config.ZapLogging)
	defer zlog.Sync()

	stats, closer, err := metrics.New(config.Metrics, *cluster)
	if err != nil {
		log.Fatalf("Failed to init metrics: %s", err)
	}
	defer closer.Close()

	stats = stats.Tagged(map[string]string{
		"origin": hostname,
	})

	fs, err := store.NewOriginFileStore(config.OriginStore, clock.New())
	if err != nil {
		log.Fatalf("Failed to create origin file store: %s", err)
	}

	pctx, err := core.NewPeerContext(
		core.PeerIDFactory(config.Torrent.PeerIDFactory), *zone, *peerIP, *peerPort, true)
	if err != nil {
		log.Fatalf("Failed to create peer context: %s", err)
	}

	torrentClient, err := torrent.NewSchedulerClient(
		config.Torrent,
		stats,
		pctx,
		announceclient.Disabled(),
		announcequeue.Disabled(),
		torrentstorage.NewOriginTorrentArchive(fs))
	if err != nil {
		log.Fatalf("Failed to create scheduler client: %s", err)
	}

	backendManager, err := backend.NewManager(config.Namespaces, config.AuthNamespaces)
	if err != nil {
		log.Fatalf("Error creating backend manager: %s", err)
	}

	server, err := blobserver.New(
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

	h := addTorrentDebugEndpoints(server.Handler(), torrentClient)

	addr := fmt.Sprintf(":%d", *blobServerPort)
	log.Infof("Starting origin server on %s", addr)
	log.Fatal(http.ListenAndServe(addr, h))
}
