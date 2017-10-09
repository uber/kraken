package main

import (
	xconfig "code.uber.internal/go-common.git/x/config"
	"code.uber.internal/go-common.git/x/log"
	dockerconfig "github.com/docker/distribution/configuration"
	dockercontext "github.com/docker/distribution/context"
	docker "github.com/docker/distribution/registry"

	"code.uber.internal/infra/kraken/lib/dockerregistry"
	"code.uber.internal/infra/kraken/lib/dockerregistry/transfer"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/metrics"
	"code.uber.internal/infra/kraken/origin/blobserver"
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

	store, err := store.NewLocalFileStore(&config.Store, config.Registry.TagDeletion.Enable)
	if err != nil {
		log.Fatalf("Failed to create local store: %s", err)
	}

	transferer := transfer.NewOriginClusterTransferer(
		config.Concurrency,
		store,
		config.TrackAddr,
		config.OriginAddr,
		blobserver.HTTPClientProvider{},
	)

	config.Registry.Docker.Storage = dockerconfig.Storage{
		dockerregistry.Name: dockerconfig.Parameters{
			"config":     &config.Registry,
			"transferer": transferer,
			"store":      store,
			"metrics":    stats,
		},
	}

	registry, err := docker.NewRegistry(dockercontext.Background(), &config.Registry.Docker)
	if err != nil {
		log.Fatalf("Failed to init registry: %s", err)
	}

	log.Info("Starting registry...")
	log.Fatal(registry.ListenAndServe())
}
