package main

import (
	"flag"
	"fmt"
	"net/http"

	"code.uber.internal/infra/kraken/lib/serverset"
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

	storeProvider := storage.NewStoreProvider(config.Storage, config.Nemo)
	peerStore, err := storeProvider.GetPeerStore()
	if err != nil {
		log.Fatalf("Could not create PeerStore: %s", err)
	}
	torrentStore, err := storeProvider.GetMetaInfoStore()
	if err != nil {
		log.Fatalf("Could not create MetaInfoStore: %s", err)
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

	origins, err := serverset.NewRoundRobin(config.Origin.RoundRobin)
	if err != nil {
		log.Fatalf("Error creating origin round robin: %s", err)
	}
	originCluster := blobclient.NewClusterClient(
		blobclient.NewClientResolver(blobclient.NewProvider(), origins))

	h := trackerserver.Handler(
		config.TrackerServer,
		stats,
		policy,
		peerStore,
		torrentStore,
		manifestStore,
		originCluster)

	addr := fmt.Sprintf(":%d", config.Port)
	log.Infof("Listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, h))
}
