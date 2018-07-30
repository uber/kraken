package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/uber-go/tally"

	"code.uber.internal/infra/kraken/agent/agentserver"
	"code.uber.internal/infra/kraken/build-index/tagclient"
	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/dockerregistry/transfer"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/lib/torrent/networkevent"
	"code.uber.internal/infra/kraken/lib/torrent/scheduler"
	"code.uber.internal/infra/kraken/metrics"
	"code.uber.internal/infra/kraken/nginx"
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
	agentRegistryPort := flag.Int("agent_registry_port", 5055, "port which agent registry listens on")
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

	stats, closer, err := metrics.New(config.Metrics, *cluster)
	if err != nil {
		log.Fatalf("Failed to init metrics: %s", err)
	}
	defer closer.Close()

	go metrics.EmitVersion(stats)

	pctx, err := core.NewPeerContext(
		config.PeerIDFactory, *zone, *cluster, *peerIP, *peerPort, false)
	if err != nil {
		log.Fatalf("Failed to create peer context: %s", err)
	}

	cads, err := store.NewCADownloadStore(config.CADownloadStore, stats)
	if err != nil {
		log.Fatalf("Failed to create local store: %s", err)
	}

	netevents, err := networkevent.NewProducer(config.NetworkEvent)
	if err != nil {
		log.Fatalf("Failed to create network event producer: %s", err)
	}

	sched, err := scheduler.NewAgentScheduler(
		config.Scheduler, stats, pctx, cads, netevents, config.Tracker)
	if err != nil {
		log.Fatalf("Error creating scheduler: %s", err)
	}

	transferer := transfer.NewReadOnlyTransferer(cads, tagclient.New(config.BuildIndex), sched)

	registry, err := config.Registry.Build(config.Registry.ReadOnlyParameters(transferer, cads, stats))
	if err != nil {
		log.Fatalf("Failed to init registry: %s", err)
	}

	agentServer := agentserver.New(config.AgentServer, stats, cads, sched)
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

	// Wipe log files created by the old nginx process which ran as root.
	// TODO(codyg): Swap these with the v2 log files once they are deleted.
	for _, name := range []string{
		"/var/log/udocker/kraken-agent/nginx-access.log",
		"/var/log/udocker/kraken-agent/nginx-error.log",
	} {
		if err := os.Remove(name); err != nil && !os.IsNotExist(err) {
			log.Warnf("Could not remove old root-owned nginx log: %s", err)
		}
	}

	log.Fatal(nginx.Run(config.Nginx, *agentRegistryPort))
}
