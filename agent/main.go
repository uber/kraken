package main

import (
	"flag"
	"fmt"
	"net/http"
	"time"

	dockercontext "github.com/docker/distribution/context"
	docker "github.com/docker/distribution/registry"
	"github.com/uber-go/tally"

	"code.uber.internal/infra/kraken/agent/agentserver"
	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/backend"
	"code.uber.internal/infra/kraken/lib/dockerregistry"
	"code.uber.internal/infra/kraken/lib/dockerregistry/transfer"
	"code.uber.internal/infra/kraken/lib/serverset"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/lib/torrent/networkevent"
	"code.uber.internal/infra/kraken/lib/torrent/scheduler"
	"code.uber.internal/infra/kraken/metrics"
	"code.uber.internal/infra/kraken/utils/configutil"
	"code.uber.internal/infra/kraken/utils/log"
)

// heartbeat periodically emits a counter metric which allows us to monitor the
// number of active agents.
func heartbeat(stats tally.Scope) {
	for {
		stats.Counter("heartbeat").Inc(1)
		time.Sleep(10 * time.Second)
	}
}

func main() {
	peerIP := flag.String("peer_ip", "", "ip which peer will announce itself as")
	peerPort := flag.Int("peer_port", 0, "port which peer will announce itself as")
	agentServerPort := flag.Int("agent_server_port", 0, "port which agent server will listen on")
	configFile := flag.String("config", "", "Configuration file that has to be loaded from one of UBER_CONFIG_DIR locations")
	zone := flag.String("zone", "", "zone/datacenter name")
	cluster := flag.String("cluster", "", "cluster name (e.g. prod01-sjc1)")

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

	pctx, err := core.NewPeerContext(
		config.PeerIDFactory, *zone, *cluster, *peerIP, *peerPort, false)
	if err != nil {
		log.Fatalf("Failed to create peer context: %s", err)
	}

	stats, closer, err := metrics.New(config.Metrics, *cluster)
	if err != nil {
		log.Fatalf("Failed to init metrics: %s", err)
	}
	defer closer.Close()

	trackers, err := serverset.NewRoundRobin(config.Tracker.RoundRobin)
	if err != nil {
		log.Fatalf("Error creating tracker round robin: %s", err)
	}

	fs, err := store.NewLocalFileStore(config.Store, stats, config.Registry.TagDeletion.Enable)
	if err != nil {
		log.Fatalf("Failed to create local store: %s", err)
	}

	netevents, err := networkevent.NewProducer(config.NetworkEvent)
	if err != nil {
		log.Fatalf("Failed to create network event producer: %s", err)
	}

	sched, err := scheduler.NewAgentScheduler(config.Scheduler, stats, pctx, fs, netevents, trackers)
	if err != nil {
		log.Fatalf("Error creating scheduler: %s", err)
	}

	backendManager, err := backend.NewManager(config.Registry.Namespaces, config.AuthNamespaces)
	if err != nil {
		log.Fatalf("Error creating backend manager: %s", err)
	}
	tagClient, err := backendManager.GetClient(config.Registry.TagNamespace)
	if err != nil {
		log.Fatalf("Error creating backend tag client: %s", err)
	}
	transferer := transfer.NewAgentTransferer(fs, tagClient, config.Registry.BlobNamespace, sched)

	dockerConfig := config.Registry.CreateDockerConfig(dockerregistry.Name, transferer, fs, stats)
	registry, err := docker.NewRegistry(dockercontext.Background(), dockerConfig)
	if err != nil {
		log.Fatalf("Failed to init registry: %s", err)
	}

	agentServer := agentserver.New(config.AgentServer, stats, fs, sched)
	addr := fmt.Sprintf(":%d", *agentServerPort)
	log.Infof("Starting agent server on %s", addr)
	go func() {
		log.Fatal(http.ListenAndServe(addr, agentServer.Handler()))
	}()

	log.Info("Starting registry...")
	go func() {
		log.Fatal(registry.ListenAndServe())
	}()

	go heartbeat(stats)

	select {}
}
