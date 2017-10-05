package main

import (
	"flag"

	xconfig "code.uber.internal/go-common.git/x/config"
	"code.uber.internal/go-common.git/x/log"
	dockerconfig "github.com/docker/distribution/configuration"
	dockercontext "github.com/docker/distribution/context"
	docker "github.com/docker/distribution/registry"

	"code.uber.internal/infra/kraken/lib/dockerregistry"
	"code.uber.internal/infra/kraken/lib/peercontext"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/lib/torrent"
	"code.uber.internal/infra/kraken/metrics"
)

func main() {
	announceIP := flag.String("announce_ip", "", "ip which peer will announce itself as")
	announcePort := flag.Int("announce_port", 0, "port which peer will announce itself as")
	flag.Parse()

	var config Config
	if err := xconfig.Load(&config); err != nil {
		panic(err)
	}
	// Disable JSON logging because it's completely unreadable.
	formatter := true
	config.Logging.TextFormatter = &formatter
	log.Configure(&config.Logging, false)

	pctx, err := peercontext.New(
		peercontext.PeerIDFactory(config.Torrent.PeerIDFactory), *announceIP, *announcePort)
	if err != nil {
		log.Fatalf("Failed to create peer context: %s", err)
	}

	stats, closer, err := metrics.New(config.Metrics)
	if err != nil {
		log.Fatalf("Failed to init metrics: %s", err)
	}
	defer closer.Close()

	store, err := store.NewLocalFileStore(&config.Store, config.Registry.TagDeletion.Enable)
	if err != nil {
		log.Fatalf("Failed to create local store: %s", err)
	}

	client, err := torrent.NewSchedulerClient(&config.Torrent, store, stats, pctx)
	if err != nil {
		log.Fatalf("Failed to create scheduler client: %s", err)
		panic(err)
	}
	defer client.Close()

	config.Registry.Docker.Storage = dockerconfig.Storage{
		dockerregistry.Name: dockerconfig.Parameters{
			"config":        &config.Registry,
			"torrentclient": client,
			"store":         store,
			"metrics":       stats,
		},
		"redirect": dockerconfig.Parameters{
			"disable": true,
		},
	}

	registry, err := docker.NewRegistry(dockercontext.Background(), &config.Registry.Docker)
	if err != nil {
		log.Fatalf("Failed to init registry: %s", err)
	}

	log.Info("Starting registry...")
	log.Fatal(registry.ListenAndServe())
}
