package main

import (
	"flag"

	"code.uber.internal/go-common.git/x/log"

	"code.uber.internal/infra/kraken/client/dockerregistry"
	"code.uber.internal/infra/kraken/client/server"
	"code.uber.internal/infra/kraken/client/store"
	"code.uber.internal/infra/kraken/client/torrentclient"
	"code.uber.internal/infra/kraken/configuration"
	"code.uber.internal/infra/kraken/metrics"
	rc "github.com/docker/distribution/configuration"
	ctx "github.com/docker/distribution/context"
	dr "github.com/docker/distribution/registry"
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
		log.Info("Load agent configuration. Config: %s", configFile)
		cp := configuration.GetConfigFilePath(configFile)
		config = configuration.NewConfigWithPath(cp)
	} else {
		log.Info("Load agent configuration")
		config = configuration.NewConfig()
	}
	config.DisableTorrent = disableTorrent

	// init metrics
	metricsScope, metricsCloser, err := metrics.NewMetrics(config.Metrics)
	if err != nil {
		log.Fatal(err)
	}
	defer metricsCloser.Close()

	// init storage
	store := store.NewLocalFileStore(config)

	// init torrent client
	log.Info("Init torrent agent")
	client, err := torrentclient.NewClient(config, store, metricsScope, clientTimeout)
	defer client.Close()

	if err != nil {
		log.Fatal(err)
	}

	// start agent server
	aWeb := server.NewAgentWebApp(config, client)
	go aWeb.Serve()

	// init docker registry
	log.Info("Init registry")
	config.Registry.Storage = rc.Storage{
		dockerregistry.Name: rc.Parameters{
			"config":        config,
			"torrentclient": client,
			"store":         store,
			"metrics":       metricsScope,
		},
		"redirect": rc.Parameters{
			"disable": true,
		},
	}

	registry, err := dr.NewRegistry(ctx.Background(), &config.Registry)
	if err != nil {
		log.Fatal(err.Error())
	}

	log.Info("Start registry")
	err = registry.ListenAndServe()
	if err != nil {
		log.Fatal(err.Error())
	}
}
