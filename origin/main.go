package main

import (
	"fmt"
	"net/http"

	xconfig "code.uber.internal/go-common.git/x/config"
	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/origin/blobserver"
	"code.uber.internal/infra/kraken/origin/client"
	"code.uber.internal/infra/kraken/utils"
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

	hostname, err := utils.GetLocalIP()
	if err != nil {
		log.Fatalf("Error getting local IP: %s", err)
	}
	localStore, err := store.NewLocalFileStore(&config.LocalStore, true)
	if err != nil {
		log.Fatalf("Error initializing local store: %s", err)
	}
	s, err := blobserver.New(config.BlobServer, hostname, localStore, client.NewBlobAPIClient)
	if err != nil {
		log.Fatalf("Error initializing blob server: %s", err)
	}

	addr := fmt.Sprintf(":%d", config.Port)
	log.Info("Starting origin server %s on %s", hostname, addr)
	log.Fatal(http.ListenAndServe(addr, s.Handler()))
}
