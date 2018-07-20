package main

import (
	"flag"

	"code.uber.internal/infra/kraken/metrics"
	"code.uber.internal/infra/kraken/nginx"
	"code.uber.internal/infra/kraken/origin/blobclient"
	"code.uber.internal/infra/kraken/tracker/peerhandoutpolicy"
	"code.uber.internal/infra/kraken/tracker/peerstore"
	"code.uber.internal/infra/kraken/tracker/trackerserver"
	"code.uber.internal/infra/kraken/utils/configutil"
	"code.uber.internal/infra/kraken/utils/log"

	"github.com/andres-erbsen/clock"
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

	go metrics.EmitVersion(stats)

	peerStore, err := peerstore.NewRedisStore(config.PeerStore.Redis, clock.New())
	if err != nil {
		log.Fatalf("Could not create PeerStore: %s", err)
	}

	policy, err := peerhandoutpolicy.NewPriorityPolicy(stats, config.PeerHandoutPolicy.Priority)
	if err != nil {
		log.Fatalf("Could not load peer handout policy: %s", err)
	}

	r, err := blobclient.NewClientResolver(blobclient.NewProvider(), config.Origin)
	if err != nil {
		log.Fatalf("Error creating origin client resolver: %s", err)
	}
	originCluster := blobclient.NewClusterClient(r)

	server := trackerserver.New(config.TrackerServer, stats, policy, peerStore, originCluster)
	go func() {
		log.Fatal(server.ListenAndServe())
	}()

	log.Info("Starting nginx...")
	log.Fatal(nginx.Run(config.Nginx, config.Port))
}
