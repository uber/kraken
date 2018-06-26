package main

import (
	"flag"
	"fmt"
	"net/http"

	"code.uber.internal/infra/kraken/build-index/tagclient"
	"code.uber.internal/infra/kraken/build-index/tagserver"
	"code.uber.internal/infra/kraken/build-index/tagstore"
	"code.uber.internal/infra/kraken/build-index/tagtype"
	"code.uber.internal/infra/kraken/lib/backend"
	"code.uber.internal/infra/kraken/lib/persistedretry"
	"code.uber.internal/infra/kraken/lib/persistedretry/tagreplication"
	"code.uber.internal/infra/kraken/lib/persistedretry/writeback"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/localdb"
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

	ss, err := store.NewSimpleStore(config.Store, stats)
	if err != nil {
		log.Fatalf("Error creating simple store: %s", err)
	}

	backends, err := backend.NewManager(config.Backends, config.Auth)
	if err != nil {
		log.Fatalf("Error creating backend manager: %s", err)
	}

	r, err := blobclient.NewClientResolver(blobclient.NewProvider(), config.Origin)
	if err != nil {
		log.Fatalf("Error creating origin client resolver: %s", err)
	}
	originClient := blobclient.NewClusterClient(r)

	localDB, err := localdb.New(config.LocalDB)
	if err != nil {
		log.Fatalf("Error creating local db: %s", err)
	}

	localReplicas, err := config.LocalReplicas.Build(*port)
	if err != nil {
		log.Fatalf("Error building local replica host list: %s", err)
	}

	remotes, err := config.Remotes.Build()
	if err != nil {
		log.Fatalf("Error building remotes from configuration: %s", err)
	}

	tagReplicationExecutor := tagreplication.NewExecutor(
		stats,
		originClient,
		tagclient.NewProvider())
	tagReplicationStore, err := tagreplication.NewStore(localDB, remotes)
	if err != nil {
		log.Fatalf("Error creating tag replication store: %s", err)
	}
	tagReplicationManager, err := persistedretry.NewManager(
		config.TagReplication,
		stats,
		tagReplicationStore,
		tagReplicationExecutor)
	if err != nil {
		log.Fatalf("Error creating tag replication manager: %s", err)
	}

	writeBackManager, err := persistedretry.NewManager(
		config.WriteBack,
		stats,
		writeback.NewStore(localDB),
		writeback.NewExecutor(stats, ss, backends))
	if err != nil {
		log.Fatalf("Error creating write-back manager: %s", err)
	}

	tagStore := tagstore.New(
		config.TagStore,
		stats,
		ss,
		backends,
		writeBackManager,
		remotes,
		tagclient.NewProvider())

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
		tagStore,
		remotes,
		tagReplicationManager,
		tagclient.NewProvider(),
		tagTypes)

	addr := fmt.Sprintf(":%d", *port)
	log.Infof("Listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, server.Handler()))
}
