package main

import (
	"flag"

	"code.uber.internal/go-common.git/x/log"

	"code.uber.internal/infra/kraken/client/dockerregistry"
	"code.uber.internal/infra/kraken/client/store"
	"code.uber.internal/infra/kraken/configuration"
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

	log.Info("Create registry")
	config.Registry.Storage = rc.Storage{
		dockerregistry.Name: rc.Parameters{
			"config": config,
			"store":  store.NewLocalFileStore(config),
		},
		"redirect": rc.Parameters{
			"disable": true,
		},
	}

	registry, err := dr.NewRegistry(ctx.Background(), &config.Registry)
	if err != nil {
		log.Fatal(err.Error())
	}

	log.Info("Listen")
	err = registry.ListenAndServe()
	if err != nil {
		log.Fatal(err.Error())
	}
}
