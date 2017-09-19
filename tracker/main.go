package main

import (
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	xconfig "code.uber.internal/go-common.git/x/config"
	"code.uber.internal/go-common.git/x/log"

	"code.uber.internal/infra/kraken/tracker/peerhandoutpolicy"
	"code.uber.internal/infra/kraken/tracker/service"
	"code.uber.internal/infra/kraken/tracker/storage"
)

func main() {
	var config Config
	if err := xconfig.Load(&config); err != nil {
		panic(err)
	}
	// Disable JSON logging because it's completely unreadable.
	formatter := true
	config.Logging.TextFormatter = &formatter
	log.Configure(&config.Logging, false)

	storeProvider := storage.NewStoreProvider(config.Storage, config.Nemo)
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

	policy, err := peerhandoutpolicy.Get(
		config.PeerHandoutPolicy.Priority, config.PeerHandoutPolicy.Sampling)
	if err != nil {
		log.Fatalf("Could not load peer handout policy: %s", err)
	}

	h := service.Handler(config.Service, policy, peerStore, torrentStore, manifestStore)

	addr := fmt.Sprintf(":%d", config.BackendPort)
	log.Infof("Listening on %s", addr)

	go log.Fatal(http.ListenAndServe(addr, h))

	// Handle SIGINT and SIGTERM.
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch // blocks until shutdown is signaled

	log.Info("Shutdown complete")
}
