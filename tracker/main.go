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
	cfg, err := config.Initialize()
	if err != nil {
		log.Fatalf("Could not intialize config: %s", err)
	}
	log.Configure(&cfg.Logging, false)

	storeProvider := storage.NewStoreProvider(cfg.Database, cfg.Nemo)
	peerStore, err := storeProvider.GetPeerStore()
	if err != nil {
		log.Fatalf("Could not create PeerStore: %s", err)
	}
	torrentStore, err := storeProvider.GetTorrentStore()
	if err != nil {
		log.Fatalf("Could not create TorrentStore: %s", err)
	}
	manifestStore, err := storeProvider.GetManifestStore()
	if err != nil {
		log.Fatalf("Could not create ManifestStore: %s", err)
	}

	webApp := service.InitializeAPI(cfg, peerStore, torrentStore, manifestStore)

	addr := fmt.Sprintf(":%d", cfg.BackendPort)
	log.Infof("Listening on %s", addr)

	go log.Fatal(http.ListenAndServe(addr, webApp))

	// Handle SIGINT and SIGTERM.
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch // blocks until shutdown is signaled

	log.Info("Shutdown complete")
}
