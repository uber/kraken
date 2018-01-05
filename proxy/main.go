package main

import (
	"flag"

	dockercontext "github.com/docker/distribution/context"
	docker "github.com/docker/distribution/registry"

	"code.uber.internal/infra/kraken/lib/dockerregistry"
	"code.uber.internal/infra/kraken/lib/dockerregistry/transfer"
	"code.uber.internal/infra/kraken/lib/dockerregistry/transfer/manifestclient"
	"code.uber.internal/infra/kraken/lib/serverset"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/metrics"
	"code.uber.internal/infra/kraken/origin/blobclient"
	"code.uber.internal/infra/kraken/utils/configutil"
	"code.uber.internal/infra/kraken/utils/log"
)

func main() {
	configFile := flag.String("config", "", "Configuration file that has to be loaded from one of UBER_CONFIG_DIR locations")
	flag.Parse()

	var config Config
	if err := configutil.Load(*configFile, &config); err != nil {
		panic(err)
	}

	log.ConfigureLogger(config.ZapLogging)

	stats, closer, err := metrics.New(config.Metrics)
	if err != nil {
		log.Fatalf("Failed to init metrics: %s", err)
	}
	defer closer.Close()

	fs, err := store.NewLocalFileStore(&config.Store, config.Registry.TagDeletion.Enable)
	if err != nil {
		log.Fatalf("Failed to create local store: %s", err)
	}

	origins, err := serverset.NewRoundRobin(config.Origin.RoundRobin)
	if err != nil {
		log.Fatalf("Error creating origin round robin: %s", err)
	}
	originCluster := blobclient.NewClusterClient(blobclient.NewProvider(), origins)

	trackers, err := serverset.NewRoundRobin(config.Tracker.RoundRobin)
	if err != nil {
		log.Fatalf("Error creating tracker round robin: %s", err)
	}
	manifestClient := manifestclient.New(trackers)

	transferer := transfer.NewOriginClusterTransferer(
		originCluster, manifestClient, fs)

	dockerConfig := config.Registry.CreateDockerConfig(dockerregistry.Name, transferer, fs, stats)
	registry, err := docker.NewRegistry(dockercontext.Background(), dockerConfig)
	if err != nil {
		log.Fatalf("Failed to init registry: %s", err)
	}

	log.Info("Starting registry...")
	log.Fatal(registry.ListenAndServe())
}
