package main

import (
	"flag"
	"fmt"
	"net/http"

	"code.uber.internal/infra/kraken/build-index/tagclient"
	"code.uber.internal/infra/kraken/build-index/tagserver"
	"code.uber.internal/infra/kraken/build-index/tagtype"
	"code.uber.internal/infra/kraken/lib/backend"
	"code.uber.internal/infra/kraken/lib/persistedretry"
	"code.uber.internal/infra/kraken/lib/persistedretry/tagreplication"
	"code.uber.internal/infra/kraken/metrics"
	"code.uber.internal/infra/kraken/origin/blobclient"
	"code.uber.internal/infra/kraken/utils/configutil"
	"code.uber.internal/infra/kraken/utils/log"
)

func main() {
	configFile := flag.String("config", "", "configuration file that has to be loaded from one of UBER_CONFIG_DIR locations")
	cluster := flag.String("cluster", "", "cluster name (e.g. prod01-sjc1)")
	port := flag.Int("port", 0, "tag server port")

	flag.Parse()

	if *port == 0 {
		panic("no port provided")
	}

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

	r, err := blobclient.NewClientResolver(blobclient.NewProvider(), config.Origin)
	if err != nil {
		log.Fatalf("Error creating origin client resolver: %s", err)
	}
	originClient := blobclient.NewClusterClient(r)

	trExecutor := tagreplication.NewExecutor(
		stats,
		originClient,
		tagclient.NewProvider())

	remotes, err := config.Remotes.Build()
	if err != nil {
		log.Fatalf("Error building remotes from configuration: %s", err)
	}

	trStore, err := tagreplication.NewStore(config.SQLiteSourcePath, remotes)
	if err != nil {
		log.Fatalf("Error creating replicate store: %s", err)
	}

	trManager, err := persistedretry.NewManager(
		config.TagReplication,
		stats,
		trStore,
		trExecutor)
	if err != nil {
		log.Fatalf("Error creating tag replication manager: %s", err)
	}

	backends, err := backend.NewManager(config.Backends, config.Auth)
	if err != nil {
		log.Fatalf("Error creating backend manager: %s", err)
	}

	localReplicas, err := config.LocalReplicas.Build(*port)
	if err != nil {
		log.Fatalf("Error building local replica host list: %s", err)
	}

	tagTypes, err := tagtype.NewManager(config.TagTypes, originClient)
	if err != nil {
		log.Fatalf("Error creating tag type manager: %s", err)
	}

	server := tagserver.New(
		config.TagServer,
		stats,
		backends,
		config.Origin,
		originClient,
		localReplicas,
		remotes,
		trManager,
		tagclient.NewProvider(),
		tagTypes,
	)

	addr := fmt.Sprintf(":%d", *port)
	log.Infof("Listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, server.Handler()))
}
