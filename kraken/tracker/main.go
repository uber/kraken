package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"code.uber.internal/go-common.git/x/log"
	cfg "code.uber.internal/infra/kraken/config/tracker"
	"code.uber.internal/infra/kraken/kraken/tracker/service"
	"code.uber.internal/infra/kraken/kraken/tracker/storage"
	"net/http"
)

func main() {
	appCfg := cfg.Initialize()
	log.Configure(&appCfg.Logging, false)

	log.Info("Starting Kraken Tracker server...")

	//run db migration if there is any
	storage.RunDBMigration(appCfg)

	//Register storafe backends
	storage.Init()

	//create default data storage engine
	datastore, err := storage.CreateStorage(appCfg)

	if err != nil {
		log.Fatalf("could not create storage: %s", err.Error())
	}

	webApp := service.InitializeAPI(appCfg, datastore)

	log.Infof("Binding to port %d", appCfg.BackendPort)

	go func() { log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", appCfg.BackendPort), webApp)) }()

	// Handle SIGINT and SIGTERM.
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch // blocks until shutdown is signaled

	log.Info("Shutdown complete...")
}
