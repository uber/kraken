package main

import (
	"flag"

	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/infra/kraken/client/dockerregistry"
	"code.uber.internal/infra/kraken/client/store"
	"code.uber.internal/infra/kraken/client/torrent"
	"code.uber.internal/infra/kraken/configuration"
	"code.uber.internal/infra/kraken/metrics"

	dockerconfig "github.com/docker/distribution/configuration"
	ctx "github.com/docker/distribution/context"
	docker "github.com/docker/distribution/registry"
)

func main() {
	var configFile string
	var disableTorrent bool
	var clientTimeout int
	flag.StringVar(&configFile, "config", "", "agent configuration file")
	flag.BoolVar(&disableTorrent, "disableTorrent", false, "disable torrent")
	flag.IntVar(&clientTimeout, "clientTimeout", 120, "torrent client timeout in seconds")
	flag.Parse()

	// load config
	var config *configuration.Config
	if configFile != "" {
		log.Infof("Load agent configuration. Config: %s", configFile)
		cp := configuration.GetConfigFilePath(configFile)
		config = configuration.NewConfigWithPath(cp)
	} else {
		log.Info("Load agent configuration")
		config = configuration.NewConfig()
	}
	config.Registry.DisableTorrent = disableTorrent

	// init metrics
	metricsScope, metricsCloser, err := metrics.New(config.Metrics)
	if err != nil {
		log.Fatal(err)
	}
	defer metricsCloser.Close()

	// init storage
	store, err := store.NewLocalStore(&config.Store, config.Registry.TagDeletion.Enable)
	if err != nil {
		log.Fatal(err)
	}

	// init torrent client
	log.Info("Init torrent agent")
	client, err := torrent.NewSchedulerClient(&config.Torrent, store)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// init docker registry
	log.Info("Init registry")
	config.Registry.Docker.Storage = dockerconfig.Storage{
		dockerregistry.Name: dockerconfig.Parameters{
			"config":        &config.Registry,
			"torrentclient": client,
			"store":         store,
			"metrics":       metricsScope,
		},
		"redirect": dockerconfig.Parameters{
			"disable": true,
		},
	}

	registry, err := docker.NewRegistry(ctx.Background(), &config.Registry.Docker)
	if err != nil {
		log.Fatal(err.Error())
	}

	log.Info("Start registry")
	err = registry.ListenAndServe()
	if err != nil {
		log.Fatal(err.Error())
	}
}
