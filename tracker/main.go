package main

import (
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	xconfig "code.uber.internal/go-common.git/x/config"
	"code.uber.internal/go-common.git/x/log"

	"code.uber.internal/infra/kraken/metrics"
	"code.uber.internal/infra/kraken/origin/blobclient"
	"code.uber.internal/infra/kraken/tracker/peerhandoutpolicy"
	"code.uber.internal/infra/kraken/tracker/service"
	"code.uber.internal/infra/kraken/tracker/storage"
)

func main() {
	var config Config
	if err := xconfig.Load(&config); err != nil {
		panic(err)
	}
	// Disable JSON logging because it's completely unreadable.
	formatter := true
	config.Logging.TextFormatter = &formatter
	log.Configure(&config.Logging, false)

	// stats
	stats, closer, err := metrics.New(config.Metrics)
	if err != nil {
		log.Fatalf("Failed to init metrics: %s", err)
	}
	defer closer.Close()

	// root metrics scope for the tracker
	stats = stats.SubScope("kraken.tracker")

	storeProvider := storage.NewStoreProvider(config.Storage, config.Nemo)
	peerStore, err := storeProvider.GetPeerStore()
	if err != nil {
		log.Fatalf("Could not create PeerStore: %s", err)
	}
	torrentStore, err := storeProvider.GetTorrentStore()
	if err != nil {
		log.Fatalf("Could not create TorrentStore: %s", err)
	}
	manifestStore, err := storeProvider.GetManifestStore()
	if err != nil {
		log.Fatalf("Could not create ManifestStore: %s", err)
	}

	policy, err := peerhandoutpolicy.Get(
		config.PeerHandoutPolicy.Priority, config.PeerHandoutPolicy.Sampling)
	if err != nil {
		log.Fatalf("Could not load peer handout policy: %s", err)
	}

	originResolver, err := blobclient.NewRoundRobinResolver(
		blobclient.NewProvider(config.OriginCluster.Client),
		config.OriginCluster.Retries,
		config.OriginCluster.DNS)
	if err != nil {
		log.Fatalf("Error creating origin resolver: %s", err)
	}

	h := service.Handler(
		config.Service,
		stats,
		policy,
		peerStore,
		torrentStore,
		manifestStore,
		originResolver)

	addr := fmt.Sprintf(":%d", config.BackendPort)
	log.Infof("Listening on %s", addr)

	go log.Fatal(http.ListenAndServe(addr, h))

	// Handle SIGINT and SIGTERM.
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch // blocks until shutdown is signaled

	log.Info("Shutdown complete")
}
