package cmd

import (
	"github.com/andres-erbsen/clock"
	"github.com/spf13/cobra"
	"github.com/uber/kraken/lib/healthcheck"
	"github.com/uber/kraken/lib/upstream"
	"github.com/uber/kraken/metrics"
	"github.com/uber/kraken/nginx"
	"github.com/uber/kraken/origin/blobclient"
	"github.com/uber/kraken/tracker/originstore"
	"github.com/uber/kraken/tracker/peerhandoutpolicy"
	"github.com/uber/kraken/tracker/peerstore"
	"github.com/uber/kraken/tracker/trackerserver"
	"github.com/uber/kraken/utils/configutil"
	"github.com/uber/kraken/utils/log"
)

func init() {
	rootCmd.PersistentFlags().IntVarP(
		&port, "port", "", 0, "port to listen on")
	rootCmd.PersistentFlags().StringVarP(
		&configFile, "config", "", "", "configuration file path")
	rootCmd.PersistentFlags().StringVarP(
		&krakenCluster, "cluster", "", "", "cluster name (e.g. prod01-zone1)")
}

var (
	port          int
	configFile    string
	krakenCluster string

	rootCmd = &cobra.Command{
		Short: "kraken-tracker keeps track of all the peers and their data in the p2p network.",
		Run: func(rootCmd *cobra.Command, args []string) {
			start()
		},
	}
)

func Execute() {
	rootCmd.Execute()
}

func start() {
	var config Config
	if err := configutil.Load(configFile, &config); err != nil {
		panic(err)
	}
	log.ConfigureLogger(config.ZapLogging)

	stats, closer, err := metrics.New(config.Metrics, krakenCluster)
	if err != nil {
		log.Fatalf("Failed to init metrics: %s", err)
	}
	defer closer.Close()

	go metrics.EmitVersion(stats)

	peerStore, err := peerstore.NewRedisStore(config.PeerStore.Redis, clock.New())
	if err != nil {
		log.Fatalf("Could not create PeerStore: %s", err)
	}

	tls, err := config.TLS.BuildClient()
	if err != nil {
		log.Fatalf("Error building client tls config: %s", err)
	}

	origins, err := config.Origin.Build(upstream.WithHealthCheck(healthcheck.Default(tls)))
	if err != nil {
		log.Fatalf("Error building origin host list: %s", err)
	}

	healthyOrigins := healthcheck.HostList(origins)

	originStore := originstore.New(
		config.OriginStore, clock.New(), healthyOrigins, blobclient.NewProvider(blobclient.WithTLS(tls)))

	policy, err := peerhandoutpolicy.NewPriorityPolicy(stats, config.PeerHandoutPolicy.Priority)
	if err != nil {
		log.Fatalf("Could not load peer handout policy: %s", err)
	}

	r := blobclient.NewClientResolver(blobclient.NewProvider(blobclient.WithTLS(tls)), healthyOrigins)
	originCluster := blobclient.NewClusterClient(r)

	server := trackerserver.New(
		config.TrackerServer, stats, policy, peerStore, originStore, originCluster)
	go func() {
		log.Fatal(server.ListenAndServe())
	}()

	log.Info("Starting nginx...")
	log.Fatal(nginx.Run(config.Nginx, map[string]interface{}{
		"port": port,
		"server": nginx.GetServer(
			config.TrackerServer.Listener.Net, config.TrackerServer.Listener.Addr)},
		nginx.WithTLS(config.TLS)))
}
