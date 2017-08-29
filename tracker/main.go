package main

import (
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"code.uber.internal/go-common.git/x/log"
	config "code.uber.internal/infra/kraken/config/tracker"
	"code.uber.internal/infra/kraken/tracker/service"
	"code.uber.internal/infra/kraken/tracker/storage"
)

func main() {
	cfg := config.Initialize()
	log.Configure(&cfg.Logging, false)

	log.Info("Starting Kraken Tracker server...")

	// Run database migration if there is any.
	storage.RunDBMigration(cfg.Database.MySQL)

	peerStore, err := storage.GetPeerStore(cfg.Database)
	if err != nil {
		log.Fatalf("Could not create PeerStore: %s", err)
	}
	torrentStore, err := storage.GetTorrentStore(cfg.Database)
	if err != nil {
		log.Fatalf("Could not create TorrentStore: %s", err)
	}
	manifestStore, err := storage.GetManifestStore(cfg.Database)
	if err != nil {
		log.Fatalf("Could not create ManifestStore: %s", err)
	}

	webApp := service.InitializeAPI(cfg, peerStore, torrentStore, manifestStore)

	log.Infof("Binding to port %d", cfg.BackendPort)

	go log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", cfg.BackendPort), webApp))

	// Handle SIGINT and SIGTERM.
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch // blocks until shutdown is signaled

	log.Info("Shutdown complete")
}
