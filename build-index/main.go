package main

import (
	"database/sql"
	"flag"
	"fmt"
	"net/http"

	_ "github.com/mattn/go-sqlite3"

	"code.uber.internal/infra/kraken/build-index/remotes"
	"code.uber.internal/infra/kraken/build-index/tagclient"
	"code.uber.internal/infra/kraken/build-index/tagserver"
	"code.uber.internal/infra/kraken/lib/backend"
	"code.uber.internal/infra/kraken/metrics"
	"code.uber.internal/infra/kraken/origin/blobclient"
	"code.uber.internal/infra/kraken/utils/configutil"
	"code.uber.internal/infra/kraken/utils/log"
)

func main() {
	configFile := flag.String("config", "", "configuration file that has to be loaded from one of UBER_CONFIG_DIR locations")
	cluster := flag.String("cluster", "", "cluster name (e.g. prod01-sjc1)")

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

	db, err := sql.Open("sqlite3", config.SQLiteSourcePath)
	if err != nil {
		log.Fatalf("Failed to open sqlite3: %s", err)
	}
	defer db.Close()

	// Dummy statement to make sure we are not compiling a stub driver with CGO_ENABLE=0.
	_, err = db.Exec(`PRAGMA database_list;`)
	if err != nil {
		log.Fatalf("Failed to list databases: %s\n", err)
	}

	originCluster := blobclient.NewClusterClient(
		blobclient.NewClientResolver(blobclient.NewProvider(), config.Origin))

	replicator, err := remotes.New(config.Remotes, originCluster, tagclient.NewProvider())
	if err != nil {
		log.Fatalf("Error creating remote replicator: %s", err)
	}

	backends, err := backend.NewManager(config.Namespaces, config.AuthNamespaces)
	if err != nil {
		log.Fatalf("Error creating backend manager: %s", err)
	}

	server := tagserver.New(config.TagServer, stats, backends, replicator, config.Origin)

	addr := fmt.Sprintf(":%d", config.Port)
	log.Infof("Listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, server.Handler()))
}
