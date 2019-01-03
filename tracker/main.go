package main

import (
	"flag"

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

	"github.com/andres-erbsen/clock"
)

func main() {
	configFile := flag.String("config", "", "configuration file path")
	cluster := flag.String("cluster", "", "cluster name (e.g. prod01-zone1)")

	flag.Parse()

	var config Config
	if err := configutil.Load(*configFile, &config); err != nil {
		panic(err)
	}
	log.ConfigureLogger(config.ZapLogging)

	stats, closer, err := metrics.New(config.Metrics, *cluster)
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

	originStore := originstore.New(
		config.OriginStore, clock.New(), origins, blobclient.NewProvider(blobclient.WithTLS(tls)))

	policy, err := peerhandoutpolicy.NewPriorityPolicy(stats, config.PeerHandoutPolicy.Priority)
	if err != nil {
		log.Fatalf("Could not load peer handout policy: %s", err)
	}

	r := blobclient.NewClientResolver(blobclient.NewProvider(blobclient.WithTLS(tls)), origins)
	originCluster := blobclient.NewClusterClient(r)

	server := trackerserver.New(
		config.TrackerServer, stats, policy, peerStore, originStore, originCluster)
	go func() {
		log.Fatal(server.ListenAndServe())
	}()

	log.Info("Starting nginx...")
	log.Fatal(nginx.Run(config.Nginx, map[string]interface{}{
		"port": config.Port,
		"server": nginx.GetServer(
			config.TrackerServer.Listener.Net, config.TrackerServer.Listener.Addr)},
		nginx.WithTLS(config.TLS)))
}
