// Copyright (c) 2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.package cmd

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/backend"
	"github.com/uber/kraken/lib/blobrefresh"
	"github.com/uber/kraken/lib/hashring"
	"github.com/uber/kraken/lib/healthcheck"
	"github.com/uber/kraken/lib/hostlist"
	"github.com/uber/kraken/lib/metainfogen"
	"github.com/uber/kraken/lib/persistedretry"
	"github.com/uber/kraken/lib/persistedretry/writeback"
	"github.com/uber/kraken/lib/store"
	"github.com/uber/kraken/lib/torrent/networkevent"
	"github.com/uber/kraken/lib/torrent/scheduler"
	"github.com/uber/kraken/localdb"
	"github.com/uber/kraken/metrics"
	"github.com/uber/kraken/nginx"
	"github.com/uber/kraken/origin/blobclient"
	"github.com/uber/kraken/origin/blobserver"
	"github.com/uber/kraken/utils/configutil"
	"github.com/uber/kraken/utils/handler"
	"github.com/uber/kraken/utils/log"
	"github.com/uber/kraken/utils/netutil"

	"github.com/andres-erbsen/clock"
	"github.com/pressly/chi"
	"github.com/spf13/cobra"
)

var (
	peerIP             string
	peerPort           int
	blobServerHostName string
	blobServerPort     int
	configFile         string
	zone               string
	krakenCluster      string

	rootCmd = &cobra.Command{
		Short: "kraken-origin serves as dedicated seeder in kraken's p2p network.",
		Run: func(rootCmd *cobra.Command, args []string) {
			run()
		},
	}
)

func init() {
	rootCmd.PersistentFlags().StringVarP(
		&peerIP, "peer-ip", "", "", "ip which peer will announce itself as")
	rootCmd.PersistentFlags().IntVarP(
		&peerPort, "peer-port", "", 0, "port which peer will announce itself as")
	rootCmd.PersistentFlags().StringVarP(
		&blobServerHostName, "blobserver-hostname", "", "", "optional hostname to identify origin")
	rootCmd.PersistentFlags().IntVarP(
		&blobServerPort, "blobserver-port", "", 0, "port which blob server listens on")
	rootCmd.PersistentFlags().StringVarP(
		&configFile, "config", "", "", "configuration file path")
	rootCmd.PersistentFlags().StringVarP(
		&zone, "zone", "", "", "zone/datacenter name")
	rootCmd.PersistentFlags().StringVarP(
		&krakenCluster, "cluster", "", "", "cluster name (e.g. prod01-zone1)")
}

func Execute() {
	rootCmd.Execute()
}

func run() {
	if peerPort == 0 {
		panic("must specify non-zero peer port")
	}
	if blobServerPort == 0 {
		panic("must specify non-zero blob server port")
	}

	var hostname string
	if blobServerHostName == "" {
		var err error
		hostname, err = os.Hostname()
		if err != nil {
			log.Fatalf("Error getting hostname: %s", err)
		}
	} else {
		hostname = blobServerHostName
	}
	log.Infof("Configuring origin with hostname '%s'", hostname)

	var config Config
	if err := configutil.Load(configFile, &config); err != nil {
		panic(err)
	}

	zlog := log.ConfigureLogger(config.ZapLogging)
	defer zlog.Sync()

	stats, closer, err := metrics.New(config.Metrics, krakenCluster)
	if err != nil {
		log.Fatalf("Failed to init metrics: %s", err)
	}
	defer closer.Close()

	go metrics.EmitVersion(stats)

	if peerIP == "" {
		localIP, err := netutil.GetLocalIP()
		if err != nil {
			log.Fatalf("Error getting local ip: %s", err)
		}
		peerIP = localIP
	}

	cas, err := store.NewCAStore(config.CAStore, stats)
	if err != nil {
		log.Fatalf("Failed to create castore: %s", err)
	}

	pctx, err := core.NewPeerContext(
		config.PeerIDFactory, zone, krakenCluster, peerIP, peerPort, true)
	if err != nil {
		log.Fatalf("Failed to create peer context: %s", err)
	}

	backendManager, err := backend.NewManager(config.Backends, config.Auth)
	if err != nil {
		log.Fatalf("Error creating backend manager: %s", err)
	}

	localDB, err := localdb.New(config.LocalDB)
	if err != nil {
		log.Fatalf("Error creating local db: %s", err)
	}

	writeBackManager, err := persistedretry.NewManager(
		config.WriteBack,
		stats,
		writeback.NewStore(localDB),
		writeback.NewExecutor(stats, cas, backendManager))
	if err != nil {
		log.Fatalf("Error creating write-back manager: %s", err)
	}

	metaInfoGenerator, err := metainfogen.New(config.MetaInfoGen, cas)
	if err != nil {
		log.Fatalf("Error creating metainfo generator: %s", err)
	}

	blobRefresher := blobrefresh.New(config.BlobRefresh, stats, cas, backendManager, metaInfoGenerator)

	netevents, err := networkevent.NewProducer(config.NetworkEvent)
	if err != nil {
		log.Fatalf("Error creating network event producer: %s", err)
	}

	sched, err := scheduler.NewOriginScheduler(
		config.Scheduler, stats, pctx, cas, netevents, blobRefresher)
	if err != nil {
		log.Fatalf("Error creating scheduler: %s", err)
	}

	cluster, err := hostlist.New(config.Cluster)
	if err != nil {
		log.Fatalf("Error creating cluster host list: %s", err)
	}

	tls, err := config.TLS.BuildClient()
	if err != nil {
		log.Fatalf("Error building client tls config: %s", err)
	}

	healthCheckFilter := healthcheck.NewFilter(config.HealthCheck, healthcheck.Default(tls))

	hashRing := hashring.New(
		config.HashRing,
		cluster,
		healthCheckFilter,
		hashring.WithWatcher(backend.NewBandwidthWatcher(backendManager)))
	go hashRing.Monitor(nil)

	addr := fmt.Sprintf("%s:%d", hostname, blobServerPort)
	if !hashRing.Contains(addr) {
		// When DNS is used for hash ring membership, the members will be IP
		// addresses instead of hostnames.
		ip, err := netutil.GetLocalIP()
		if err != nil {
			log.Fatalf("Error getting local ip: %s", err)
		}
		addr = fmt.Sprintf("%s:%d", ip, blobServerPort)
		if !hashRing.Contains(addr) {
			log.Fatalf("Neither %s nor %s (port %d) found in hash ring", hostname, ip, blobServerPort)
		}
	}

	server, err := blobserver.New(
		config.BlobServer,
		stats,
		clock.New(),
		addr,
		hashRing,
		cas,
		blobclient.NewProvider(blobclient.WithTLS(tls)),
		blobclient.NewClusterProvider(blobclient.WithTLS(tls)),
		pctx,
		backendManager,
		blobRefresher,
		metaInfoGenerator,
		writeBackManager)
	if err != nil {
		log.Fatalf("Error initializing blob server: %s", err)
	}

	h := addTorrentDebugEndpoints(server.Handler(), sched)

	go func() { log.Fatal(server.ListenAndServe(h)) }()

	log.Info("Starting nginx...")
	log.Fatal(nginx.Run(
		config.Nginx,
		map[string]interface{}{
			"port":   blobServerPort,
			"server": nginx.GetServer(config.BlobServer.Listener.Net, config.BlobServer.Listener.Addr),
		},
		nginx.WithTLS(config.TLS)))
}

// addTorrentDebugEndpoints mounts experimental debugging endpoints which are
// compatible with the agent server.
func addTorrentDebugEndpoints(h http.Handler, sched scheduler.ReloadableScheduler) http.Handler {
	r := chi.NewRouter()

	r.Patch("/x/config/scheduler", handler.Wrap(func(w http.ResponseWriter, r *http.Request) error {
		var config scheduler.Config
		if err := json.NewDecoder(r.Body).Decode(&config); err != nil {
			return handler.Errorf("decode body: %s", err)
		}
		sched.Reload(config)
		return nil
	}))

	r.Mount("/", h)

	return r
}
