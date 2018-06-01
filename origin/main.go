package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/backend"
	"code.uber.internal/infra/kraken/lib/blobrefresh"
	"code.uber.internal/infra/kraken/lib/metainfogen"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/lib/torrent/networkevent"
	"code.uber.internal/infra/kraken/lib/torrent/scheduler"
	"code.uber.internal/infra/kraken/metrics"
	"code.uber.internal/infra/kraken/origin/blobclient"
	"code.uber.internal/infra/kraken/origin/blobserver"
	"code.uber.internal/infra/kraken/utils/configutil"
	"code.uber.internal/infra/kraken/utils/handler"
	"code.uber.internal/infra/kraken/utils/log"

	"github.com/andres-erbsen/clock"
	"github.com/pressly/chi"
)

// addTorrentDebugEndpoints mounts experimental debugging endpoints which are
// compatible with the agent server.
func addTorrentDebugEndpoints(h http.Handler, sched scheduler.ReloadableScheduler) http.Handler {
	r := chi.NewRouter()

	r.Patch("/x/config/scheduler", handler.Wrap(func(w http.ResponseWriter, r *http.Request) error {
		var config scheduler.Config
		if err := json.NewDecoder(r.Body).Decode(&config); err != nil {
			return handler.Errorf("decode body: %s", err)
		}
		sched.Reload(config)
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

	fs, err := store.NewOriginFileStore(config.OriginStore, clock.New(), stats)
	if err != nil {
		log.Fatalf("Failed to create origin file store: %s", err)
	}

	pctx, err := core.NewPeerContext(config.PeerIDFactory, *zone, *cluster, *peerIP, *peerPort, true)
	if err != nil {
		log.Fatalf("Failed to create peer context: %s", err)
	}

	backendManager, err := backend.NewManager(config.Backends, config.Auth)
	if err != nil {
		log.Fatalf("Error creating backend manager: %s", err)
	}

	metaInfoGenerator, err := metainfogen.New(config.MetaInfoGen, fs)
	if err != nil {
		log.Fatalf("Error creating metainfo generator: %s", err)
	}

	blobRefresher := blobrefresh.New(config.BlobRefresh, stats, fs, backendManager, metaInfoGenerator)

	netevents, err := networkevent.NewProducer(config.NetworkEvent)
	if err != nil {
		log.Fatalf("Error creating network event producer: %s", err)
	}

	sched, err := scheduler.NewOriginScheduler(
		config.Scheduler, stats, pctx, fs, netevents, blobRefresher)
	if err != nil {
		log.Fatalf("Error creating scheduler: %s", err)
	}

	server, err := blobserver.New(
		config.BlobServer,
		stats,
		fmt.Sprintf("%s:%d", hostname, *blobServerPort),
		fs,
		blobclient.NewProvider(),
		pctx,
		backendManager,
		blobRefresher,
		metaInfoGenerator)
	if err != nil {
		log.Fatalf("Error initializing blob server: %s", err)
	}

	h := addTorrentDebugEndpoints(server.Handler(), sched)

	addr := fmt.Sprintf(":%d", *blobServerPort)
	log.Infof("Starting origin server on %s", addr)
	log.Fatal(http.ListenAndServe(addr, h))
}
