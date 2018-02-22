package main

import (
	"flag"

	dockercontext "github.com/docker/distribution/context"
	docker "github.com/docker/distribution/registry"

	"code.uber.internal/infra/kraken/lib/backend"
	"code.uber.internal/infra/kraken/lib/dockerregistry"
	"code.uber.internal/infra/kraken/lib/dockerregistry/transfer"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/metrics"
	"code.uber.internal/infra/kraken/utils/configutil"
	"code.uber.internal/infra/kraken/utils/log"
)

func main() {
	configFile := flag.String("config", "", "Configuration file that has to be loaded from one of UBER_CONFIG_DIR locations")
	flag.Parse()

	var config Config
	if err := configutil.Load(*configFile, &config); err != nil {
		panic(err)
	}

	log.ConfigureLogger(config.ZapLogging)

	stats, closer, err := metrics.New(config.Metrics)
	if err != nil {
		log.Fatalf("Failed to init metrics: %s", err)
	}
	defer closer.Close()

	backendManager, err := backend.NewManager(config.Registry.Namespaces)
	if err != nil {
		log.Fatalf("Error creating backend manager: %s", err)
	}
	tagClient, err := backendManager.GetClient(config.Registry.TagNamespace)
	if err != nil {
		log.Fatalf("Error creating backend tag client: %s", err)
	}
	blobClient, err := backendManager.GetClient(config.Registry.BlobNamespace)
	if err != nil {
		log.Fatalf("Error creating backend blob client: %s", err)
	}
	fs, err := store.NewLocalFileStore(config.Store, stats, config.Registry.TagDeletion.Enable)
	if err != nil {
		log.Fatalf("Failed to create local store: %s", err)
	}
	transferer, err := transfer.NewRemoteBackendTransferer(tagClient, blobClient, fs)
	if err != nil {
		log.Fatalf("Error creating image transferer: %s", err)
	}

	dockerConfig := config.Registry.CreateDockerConfig(dockerregistry.Name, transferer, fs, stats)
	registry, err := docker.NewRegistry(dockercontext.Background(), dockerConfig)
	if err != nil {
		log.Fatalf("Failed to init registry: %s", err)
	}

	log.Info("Starting registry...")
	log.Fatal(registry.ListenAndServe())
}
