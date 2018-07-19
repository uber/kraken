package main

import (
	"flag"

	"code.uber.internal/infra/kraken/build-index/tagclient"
	"code.uber.internal/infra/kraken/lib/dockerregistry/transfer"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/metrics"
	"code.uber.internal/infra/kraken/nginx"
	"code.uber.internal/infra/kraken/origin/blobclient"
	"code.uber.internal/infra/kraken/proxy/registryoverride"
	"code.uber.internal/infra/kraken/utils/configutil"
	"code.uber.internal/infra/kraken/utils/log"
)

func main() {
	configFile := flag.String("config", "", "Configuration file that has to be loaded from one of UBER_CONFIG_DIR locations")
	cluster := flag.String("cluster", "", "cluster name (e.g. prod01-sjc1)")
	port := flag.Int("port", 0, "Nginx port")
	flag.Parse()

	var config Config
	if err := configutil.Load(*configFile, &config); err != nil {
		panic(err)
	}

	log.ConfigureLogger(config.ZapLogging)

	if port == nil || *port == 0 {
		log.Fatal("Argument -port required")
	}

	stats, closer, err := metrics.New(config.Metrics, *cluster)
	if err != nil {
		log.Fatalf("Failed to init metrics: %s", err)
	}
	defer closer.Close()

	cas, err := store.NewCAStore(config.CAStore, stats)
	if err != nil {
		log.Fatalf("Failed to create store: %s", err)
	}

	r, err := blobclient.NewClientResolver(blobclient.NewProvider(), config.Origin)
	if err != nil {
		log.Fatalf("Error creating origin client resolver: %s", err)
	}
	originCluster := blobclient.NewClusterClient(r)

	tagClient := tagclient.New(config.BuildIndex)

	transferer := transfer.NewReadWriteTransferer(tagClient, originCluster, cas)

	registry, err := config.Registry.Build(config.Registry.ReadWriteParameters(transferer, cas, stats))
	if err != nil {
		log.Fatalf("Error creating registry: %s", err)
	}
	go func() {
		log.Info("Starting registry...")
		log.Fatal(registry.ListenAndServe())
	}()

	ros := registryoverride.NewServer(config.RegistryOverride, tagClient)
	go func() {
		log.Fatal(ros.ListenAndServe())
	}()

	log.Info("Starting nginx...")
	log.Fatal(nginx.Run(config.Nginx, *port))
}
