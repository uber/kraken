package cmd

import (
	"github.com/spf13/cobra"
	"github.com/uber/kraken/build-index/tagclient"
	"github.com/uber/kraken/lib/dockerregistry/transfer"
	"github.com/uber/kraken/lib/healthcheck"
	"github.com/uber/kraken/lib/store"
	"github.com/uber/kraken/lib/upstream"
	"github.com/uber/kraken/metrics"
	"github.com/uber/kraken/nginx"
	"github.com/uber/kraken/origin/blobclient"
	"github.com/uber/kraken/proxy/registryoverride"
	"github.com/uber/kraken/utils/configutil"
	"github.com/uber/kraken/utils/log"
)

func init() {
	rootCmd.PersistentFlags().IntSliceVar(
		&ports, "port", []int{}, "port to listen on (may specify multiple)")
	rootCmd.PersistentFlags().StringVarP(
		&configFile, "config", "", "", "configuration file path")
	rootCmd.PersistentFlags().StringVarP(
		&krakenCluster, "cluster", "", "", "cluster name (e.g. prod01-zone1)")
}

var (
	ports         []int
	configFile    string
	krakenCluster string

	rootCmd = &cobra.Command{
		Short: "kraken-proxy handles uploads and direct downloads",
		Run: func(rootCmd *cobra.Command, args []string) {
			start()
		},
	}
)

func Execute() {
	rootCmd.Execute()
}

func start() {
	if len(ports) == 0 {
		panic("must specify a port")
	}

	var config Config
	if err := configutil.Load(configFile, &config); err != nil {
		panic(err)
	}

	log.ConfigureLogger(config.ZapLogging)

	if len(ports) == 0 {
		log.Fatal("Must specify at least one -port")
	}

	stats, closer, err := metrics.New(config.Metrics, krakenCluster)
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

	r := blobclient.NewClientResolver(blobclient.NewProvider(blobclient.WithTLS(tls)), healthcheck.HostList(origins))
	originCluster := blobclient.NewClusterClient(r)

	buildIndexes, err := config.BuildIndex.Build(upstream.WithHealthCheck(healthcheck.Default(tls)))
	if err != nil {
		log.Fatalf("Error building build-index host list: %s", err)
	}

	tagClient := tagclient.NewClusterClient(buildIndexes, tls)

	transferer := transfer.NewReadWriteTransferer(stats, tagClient, originCluster, cas)

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
			config.RegistryOverride.Listener.Net, config.RegistryOverride.Listener.Addr)},
		nginx.WithTLS(config.TLS)))
}
