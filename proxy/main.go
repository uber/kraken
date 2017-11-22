package main

import (
	xconfig "code.uber.internal/go-common.git/x/config"
	"code.uber.internal/go-common.git/x/log"
	dockercontext "github.com/docker/distribution/context"
	docker "github.com/docker/distribution/registry"

	"code.uber.internal/infra/kraken/lib/dockerregistry"
	"code.uber.internal/infra/kraken/lib/dockerregistry/transfer"
	"code.uber.internal/infra/kraken/lib/dockerregistry/transfer/manifestclient"
	"code.uber.internal/infra/kraken/lib/serverset"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/metrics"
	"code.uber.internal/infra/kraken/origin/blobclient"
	"code.uber.internal/infra/kraken/tracker/metainfoclient"
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
	originResolver := blobclient.NewClusterResolver(
		blobclient.NewProvider(config.Origin.Client), origins)

	trackers, err := serverset.NewRoundRobin(config.Tracker.RoundRobin)
	if err != nil {
		log.Fatalf("Error creating tracker round robin: %s", err)
	}
	manifestClient := manifestclient.New(trackers)
	metaInfoClient := metainfoclient.Default(trackers)

	transferer := transfer.NewOriginClusterTransferer(
		config.Transfer, originResolver, manifestClient, metaInfoClient, fs)

	dockerConfig := config.Registry.CreateDockerConfig(dockerregistry.Name, transferer, fs, stats)
	registry, err := docker.NewRegistry(dockercontext.Background(), dockerConfig)
	if err != nil {
		log.Fatalf("Failed to init registry: %s", err)
	}

	log.Info("Starting registry...")
	log.Fatal(registry.ListenAndServe())
}
