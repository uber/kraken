package main

import (
	"flag"

	"code.uber.internal/go-common.git/x/log"

	"code.uber.internal/infra/kraken/client/storagedriver"
	"code.uber.internal/infra/kraken/configuration"
	rc "github.com/docker/distribution/configuration"
	ctx "github.com/docker/distribution/context"
	dr "github.com/docker/distribution/registry"
)

func main() {
	var configFile string
	var createTestTorrent bool
	var testKey string
	var testFp string
	flag.BoolVar(&createTestTorrent, "createTestTorrent", false, "true if you want to create a test torrent")
	flag.StringVar(&testKey, "testKey", "", "test layer key")
	flag.StringVar(&testFp, "testPath", "", "test layer path")
	flag.StringVar(&configFile, "config", "test.yaml", "configuration file")
	flag.Parse()

	// load config
	cp := configuration.GetConfigFilePath(configFile)
	config := configuration.NewConfig(cp)

	log.Info("Load registry config")
	registryConfig := &rc.Configuration{}

	registryConfig.Version = rc.MajorMinorVersion(uint(0), uint(1))
	registryConfig.Log.Level = rc.Loglevel("debug")
	registryConfig.Notifications = config.Notifications
	registryConfig.Storage = rc.Storage{
		storagedriver.Name: rc.Parameters{
			"config":     configFile,
			"createTest": createTestTorrent,
			"testKey":    testKey,
			"testPath":   testFp,
		},
		"redirect": rc.Parameters{
			"disable": true,
		},
	}
	registryConfig.HTTP.Net = "tcp"
	registryConfig.HTTP.Addr = "0.0.0.0:" + config.RegistryPort

	log.Info("Create registry")
	registry, err := dr.NewRegistry(ctx.Background(), registryConfig)
	if err != nil {
		log.Fatal(err.Error())
	}

	log.Info("Listen")
	err = registry.ListenAndServe()
	if err != nil {
		log.Fatal(err.Error())
	}
}
