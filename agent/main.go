package main

import (
	"flag"
	"fmt"
	"net/http"

	dockercontext "github.com/docker/distribution/context"
	docker "github.com/docker/distribution/registry"

	"code.uber.internal/infra/kraken/agent/agentserver"
	"code.uber.internal/infra/kraken/lib/dockerregistry"
	"code.uber.internal/infra/kraken/lib/dockerregistry/transfer"
	"code.uber.internal/infra/kraken/lib/dockerregistry/transfer/manifestclient"
	"code.uber.internal/infra/kraken/lib/peercontext"
	"code.uber.internal/infra/kraken/lib/serverset"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/lib/torrent"
	torrentstorage "code.uber.internal/infra/kraken/lib/torrent/storage"
	"code.uber.internal/infra/kraken/metrics"
	"code.uber.internal/infra/kraken/tracker/announceclient"
	"code.uber.internal/infra/kraken/tracker/metainfoclient"
	"code.uber.internal/infra/kraken/utils/configutil"
	"code.uber.internal/infra/kraken/utils/log"
)

func main() {
	peerIP := flag.String("peer_ip", "", "ip which peer will announce itself as")
	peerPort := flag.Int("peer_port", 0, "port which peer will announce itself as")
	agentServerPort := flag.Int("agent_server_port", 0, "port which agent server will listen on")
	configFile := flag.String("config", "", "Configuration file that has to be loaded from one of UBER_CONFIG_DIR locations")
	zone := flag.String("zone", "", "zone/datacenter name")

	flag.Parse()

	if agentServerPort == nil || *agentServerPort == 0 {
		panic("must specify non-zero agent server port")
	}

	var config Config
	if err := configutil.Load(*configFile, &config); err != nil {
		panic(err)
	}

	zlog := log.ConfigureLogger(config.ZapLogging)
	defer zlog.Sync()

	pctx, err := peercontext.New(
		peercontext.PeerIDFactory(config.Torrent.PeerIDFactory), *zone, *peerIP, *peerPort)
	if err != nil {
		log.Fatalf("Failed to create peer context: %s", err)
	}

	stats, closer, err := metrics.New(config.Metrics)
	if err != nil {
		log.Fatalf("Failed to init metrics: %s", err)
	}
	defer closer.Close()

	fs, err := store.NewLocalFileStore(&config.Store, config.Registry.TagDeletion.Enable)
	if err != nil {
		log.Fatalf("Failed to create local store: %s", err)
	}

	trackers, err := serverset.NewRoundRobin(config.Tracker.RoundRobin)
	if err != nil {
		log.Fatalf("Error creating tracker round robin: %s", err)
	}

	archive := torrentstorage.NewAgentTorrentArchive(
		config.AgentTorrentArchive, fs, metainfoclient.Default(trackers))

	torrentClient, err := torrent.NewSchedulerClient(
		config.Torrent,
		fs,
		stats,
		pctx,
		announceclient.Default(pctx, trackers),
		archive)
	if err != nil {
		log.Fatalf("Failed to create scheduler client: %s", err)
		panic(err)
	}
	defer torrentClient.Close()

	agentServer := agentserver.New(config.AgentServer, fs, torrentClient)

	transferer := transfer.NewAgentTransferer(torrentClient, manifestclient.New(trackers))

	dockerConfig := config.Registry.CreateDockerConfig(dockerregistry.Name, transferer, fs, stats)
	registry, err := docker.NewRegistry(dockercontext.Background(), dockerConfig)
	if err != nil {
		log.Fatalf("Failed to init registry: %s", err)
	}

	log.Info("Starting registry...")
	go func() {
		log.Fatal(registry.ListenAndServe())
	}()

	addr := fmt.Sprintf(":%d", *agentServerPort)
	log.Infof("Starting agent server on %s", addr)
	go func() {
		log.Fatal(http.ListenAndServe(addr, agentServer.Handler()))
	}()

	select {}
}
