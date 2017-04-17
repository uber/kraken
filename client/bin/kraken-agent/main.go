package main

import (
	"flag"

	"code.uber.internal/go-common.git/x/log"

	"code.uber.internal/infra/kraken/client/dockerregistry"
	"code.uber.internal/infra/kraken/client/server"
	"code.uber.internal/infra/kraken/client/store"
	"code.uber.internal/infra/kraken/client/torrentclient"
	"code.uber.internal/infra/kraken/configuration"
	rc "github.com/docker/distribution/configuration"
	ctx "github.com/docker/distribution/context"
	dr "github.com/docker/distribution/registry"
)

func main() {
	var configFile string
	var disableTorrent bool
	flag.StringVar(&configFile, "config", "agent/test.yaml", "agent configuration file")
	flag.BoolVar(&disableTorrent, "disableTorrent", false, "disable torrent")
	flag.Parse()

	// load config
	log.Info("Load agent configuration")
	cp := configuration.GetConfigFilePath(configFile)
	config := configuration.NewConfig(cp)
	config.DisableTorrent = disableTorrent

	// init storage
	store := store.NewLocalFileStore(config)

	// init torrent client
	log.Info("Init torrent agent")
	client, err := torrentclient.NewClient(config, store)
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
