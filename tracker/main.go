package main

import (
	"flag"
	"fmt"
	"net/http"

	"code.uber.internal/infra/kraken/lib/backend"
	"code.uber.internal/infra/kraken/metrics"
	"code.uber.internal/infra/kraken/origin/blobclient"
	"code.uber.internal/infra/kraken/tracker/peerhandoutpolicy"
	"code.uber.internal/infra/kraken/tracker/storage"
	"code.uber.internal/infra/kraken/tracker/trackerserver"
	"code.uber.internal/infra/kraken/utils/configutil"
	"code.uber.internal/infra/kraken/utils/log"
)

func main() {
	configFile := flag.String("config", "", "Configuration file that has to be loaded from one of UBER_CONFIG_DIR locations")
	cluster := flag.String("cluster", "", "cluster name (e.g. prod01-sjc1)")

	flag.Parse()

	var config Config
	if err := configutil.Load(*configFile, &config); err != nil {
		panic(err)
	}
	log.ConfigureLogger(config.ZapLogging)

	stats, closer, err := metrics.New(config.Metrics, *cluster)
	if err != nil {
		log.Fatalf("Failed to init metrics: %s", err)
	}
	defer closer.Close()

	storeProvider := storage.NewStoreProvider(config.Storage)
	peerStore, err := storeProvider.GetPeerStore()
	if err != nil {
		log.Fatalf("Could not create PeerStore: %s", err)
	}
	torrentStore, err := storeProvider.GetMetaInfoStore()
	if err != nil {
		log.Fatalf("Could not create MetaInfoStore: %s", err)
	}

	policy, err := peerhandoutpolicy.Get(
		config.PeerHandoutPolicy.Priority, config.PeerHandoutPolicy.Sampling)
	if err != nil {
		log.Fatalf("Could not load peer handout policy: %s", err)
	}

	originCluster := blobclient.NewClusterClient(
		blobclient.NewClientResolver(blobclient.NewProvider(), config.Origin))

	backends, err := backend.NewManager(config.Namespaces, config.Auth)
	if err != nil {
		log.Fatalf("Error creating backend manager: %s", err)
	}
	tags, err := backends.GetClient(config.TagNamespace)
	if err != nil {
		log.Fatalf("Error creating backend tag client: %s", err)
	}

	server := trackerserver.New(
		config.TrackerServer,
		stats,
		policy,
		peerStore,
		torrentStore,
		originCluster,
		tags)

	addr := fmt.Sprintf(":%d", config.Port)
	log.Infof("Listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, server.Handler()))
}
