package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/uber-go/tally"

	"github.com/uber/kraken/agent/agentserver"
	"github.com/uber/kraken/build-index/tagclient"
	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/dockerregistry/transfer"
	"github.com/uber/kraken/lib/hashring"
	"github.com/uber/kraken/lib/store"
	"github.com/uber/kraken/lib/torrent/networkevent"
	"github.com/uber/kraken/lib/torrent/scheduler"
	"github.com/uber/kraken/metrics"
	"github.com/uber/kraken/nginx"
	"github.com/uber/kraken/utils/configutil"
	"github.com/uber/kraken/utils/log"
	"github.com/uber/kraken/utils/netutil"
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
	configFile := flag.String("config", "", "configuration file path")
	zone := flag.String("zone", "", "zone/datacenter name")
	cluster := flag.String("cluster", "", "cluster name (e.g. prod01-zone1)")

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

	if peerIP == nil || *peerIP == "" {
		peerIP = new(string)
		localIP, err := netutil.GetLocalIP()
		if err != nil {
			log.Fatalf("Error getting local ip: %s", err)
		}
		*peerIP = localIP
	}

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

	trackers, err := config.Tracker.Cluster.Build()
	if err != nil {
		log.Fatalf("Error building tracker upstream: %s", err)
	}

	tls, err := config.TLS.BuildClient()
	if err != nil {
		log.Fatalf("Error building client tls config: %s", err)
	}

	hashRing := hashring.New(
		config.Tracker.HashRing,
		trackers)

	sched, err := scheduler.NewAgentScheduler(
		config.Scheduler, stats, pctx, cads, netevents, hashRing, tls)
	if err != nil {
		log.Fatalf("Error creating scheduler: %s", err)
	}

	buildIndexes, err := config.BuildIndex.Build()
	if err != nil {
		log.Fatalf("Error building build-index upstream: %s", err)
	}

	tagClient := tagclient.NewClusterClient(buildIndexes, tls)

	transferer := transfer.NewReadOnlyTransferer(stats, cads, tagClient, sched)

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
		"/var/log/kraken/kraken-agent/nginx-access.log",
		"/var/log/kraken/kraken-agent/nginx-error.log",
	} {
		if err := os.Remove(name); err != nil && !os.IsNotExist(err) {
			log.Warnf("Could not remove old root-owned nginx log: %s", err)
		}
	}

	log.Fatal(nginx.Run(config.Nginx, map[string]interface{}{
		"port":            *agentRegistryPort,
		"registry_backup": config.RegistryBackup},
		nginx.WithTLS(config.TLS)))
}
