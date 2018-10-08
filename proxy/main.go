package main

import (
	"flag"

	"code.uber.internal/infra/kraken/build-index/tagclient"
	"code.uber.internal/infra/kraken/lib/dockerregistry/transfer"
	"code.uber.internal/infra/kraken/lib/healthcheck"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/lib/upstream"
	"code.uber.internal/infra/kraken/metrics"
	"code.uber.internal/infra/kraken/nginx"
	"code.uber.internal/infra/kraken/origin/blobclient"
	"code.uber.internal/infra/kraken/proxy/registryoverride"
	"code.uber.internal/infra/kraken/utils/configutil"
	"code.uber.internal/infra/kraken/utils/flagutil"
	"code.uber.internal/infra/kraken/utils/log"
)

func main() {
	configFile := flag.String("config", "", "Configuration file that has to be loaded from one of UBER_CONFIG_DIR locations")
	cluster := flag.String("cluster", "", "cluster name (e.g. prod01-sjc1)")
	var ports flagutil.Ints
	flag.Var(&ports, "port", "ports to listen on (may specify multiple)")
	flag.Parse()

	var config Config
	if err := configutil.Load(*configFile, &config); err != nil {
		panic(err)
	}

	log.ConfigureLogger(config.ZapLogging)

	if len(ports) == 0 {
		log.Fatal("Must specify at least one -port")
	}

	stats, closer, err := metrics.New(config.Metrics, *cluster)
	if err != nil {
		log.Fatalf("Failed to init metrics: %s", err)
	}
	defer closer.Close()

	go metrics.EmitVersion(stats)

	cas, err := store.NewCAStore(config.CAStore, stats)
	if err != nil {
		log.Fatalf("Failed to create store: %s", err)
	}

	tls, err := config.TLS.BuildClient()
	if err != nil {
		log.Fatalf("Error building client tls config: %s", err)
	}

	origins, err := config.Origin.Build(upstream.WithHealthCheck(healthcheck.Default(tls)))
	if err != nil {
		log.Fatalf("Error building origin host list: %s", err)
	}

	r := blobclient.NewClientResolver(blobclient.NewProvider(blobclient.WithTLS(tls)), origins)
	originCluster := blobclient.NewClusterClient(r)

	buildIndexes, err := config.BuildIndex.Build(upstream.WithHealthCheck(healthcheck.Default(tls)))
	if err != nil {
		log.Fatalf("Error building build-index host list: %s", err)
	}

	tagClient := tagclient.NewClusterClient(buildIndexes, tls)

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
	log.Fatal(nginx.Run(config.Nginx, map[string]interface{}{
		"ports": ports,
		"registry_server": nginx.GetServer(
			config.Registry.Docker.HTTP.Net, config.Registry.Docker.HTTP.Addr),
		"registry_override_server": nginx.GetServer(
			config.RegistryOverride.Listener.Net, config.RegistryOverride.Listener.Addr),
	}))
}
