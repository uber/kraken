package main

import (
	"flag"
	"os"

	"code.uber.internal/go-common.git/x/log"

	cache "code.uber.internal/infra/dockermover/storage"
	"code.uber.internal/infra/kraken/client/dockerregistry"
	"code.uber.internal/infra/kraken/client/server"
	"code.uber.internal/infra/kraken/client/storage"
	"code.uber.internal/infra/kraken/configuration"
	"github.com/anacrolix/torrent"
	rc "github.com/docker/distribution/configuration"
	ctx "github.com/docker/distribution/context"
	dr "github.com/docker/distribution/registry"
)

func main() {
	var configFile string
	flag.StringVar(&configFile, "config", "test.yaml", "configuration file")
	flag.Parse()

	// load config
	log.Info("Load agent configuration")
	cp := configuration.GetConfigFilePath(configFile)
	config := configuration.NewConfig(cp)

	// init temp dir
	os.Remove(config.PushTempDir)
	err := os.MkdirAll(config.PushTempDir, 0755)
	if err != nil {
		log.Fatal(err)
	}

	// init cache dir
	err = os.MkdirAll(config.CacheDir, 0755)
	if err != nil {
		log.Fatal(err)
	}

	// init new cache
	// the storage driver and torrent agent storage share the same lru
	lru, err := cache.NewFileCacheMap(config.CacheMapSize, config.CacheSize)
	if err != nil {
		log.Fatal(err)
	}

	// init storage
	storage, err := storage.NewManager(config, lru)
	if err != nil {
		log.Fatal(err)
	}

	// init torrent client
	log.Info("Init torrent agent")
	client, err := torrent.NewClient(config.CreateAgentConfig(storage))
	if err != nil {
		log.Fatal(err)
	}

	// load existing downloaded files from disk
	log.Info("Init torrents from disk")
	storage.LoadFromDisk(client)

	// start agent server
	aWeb := server.NewAgentWebApp(config, client)
	go aWeb.Serve()

	// init docker registry
	log.Info("Init registry")
	config.Registry.Storage = rc.Storage{
		dockerregistry.Name: rc.Parameters{
			"config":         config,
			"torrent-client": client,
			"cache":          lru,
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
