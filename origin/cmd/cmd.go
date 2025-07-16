// Copyright (c) 2016-2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package cmd

import (
	"crypto/tls"
	"encoding/json"
	"flag"
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
	"github.com/go-chi/chi"
	"github.com/jmoiron/sqlx"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

// Flags defines origin CLI flags.
type Flags struct {
	PeerIP             string
	PeerPort           int
	BlobServerHostName string
	BlobServerPort     int
	ConfigFile         string
	Zone               string
	KrakenCluster      string
	SecretsFile        string
}

// ParseFlags parses origin CLI flags.
func ParseFlags() *Flags {
	var flags Flags
	flag.StringVar(
		&flags.PeerIP, "peer-ip", "", "ip which peer will announce itself as")
	flag.IntVar(
		&flags.PeerPort, "peer-port", 0, "port which peer will announce itself as")
	flag.StringVar(
		&flags.BlobServerHostName, "blobserver-hostname", "", "optional hostname to identify origin")
	flag.IntVar(
		&flags.BlobServerPort, "blobserver-port", 0, "port which blob server listens on")
	flag.StringVar(
		&flags.ConfigFile, "config", "", "configuration file path")
	flag.StringVar(
		&flags.Zone, "zone", "", "zone/datacenter name")
	flag.StringVar(
		&flags.KrakenCluster, "cluster", "", "cluster name (e.g. prod01-zone1)")
	flag.StringVar(
		&flags.SecretsFile, "secrets", "", "path to a secrets YAML file to load into configuration")
	flag.Parse()
	return &flags
}

type options struct {
	config  *Config
	metrics tally.Scope
	logger  *zap.Logger
}

// Option defines an optional Run parameter.
type Option func(*options)

// WithConfig ignores config/secrets flags and directly uses the provided config
// struct.
func WithConfig(c Config) Option {
	return func(o *options) { o.config = &c }
}

// WithMetrics ignores metrics config and directly uses the provided tally scope.
func WithMetrics(s tally.Scope) Option {
	return func(o *options) { o.metrics = s }
}

// WithLogger ignores logging config and directly uses the provided logger.
func WithLogger(l *zap.Logger) Option {
	return func(o *options) { o.logger = l }
}

// Run runs the origin.
func Run(flags *Flags, opts ...Option) {
	validateFlags(flags)

	var overrides options
	for _, o := range opts {
		o(&overrides)
	}

	config := setupConfiguration(flags, &overrides)
	logger := setupLogging(config, &overrides)
	defer func() {
		if logger != nil {
			logger.Sync()
		}
	}()

	stats, statsCloser := setupMetrics(config, flags, &overrides)
	defer statsCloser()

	hostname := setupHostname(flags)
	peerIP := setupPeerIP(flags)

	components := setupCoreComponents(config, flags, hostname, peerIP, stats)
	server := setupBlobServer(config, flags, hostname, components, stats)

	startServices(config, flags, server, components.scheduler)
}

func validateFlags(flags *Flags) {
	if flags.PeerPort == 0 {
		panic("must specify non-zero peer port")
	}
	if flags.BlobServerPort == 0 {
		panic("must specify non-zero blob server port")
	}
}

func setupConfiguration(flags *Flags, overrides *options) Config {
	var config Config
	if overrides.config != nil {
		config = *overrides.config
	} else {
		if err := configutil.Load(flags.ConfigFile, &config); err != nil {
			panic(err)
		}
		if flags.SecretsFile != "" {
			if err := configutil.Load(flags.SecretsFile, &config); err != nil {
				panic(err)
			}
		}
	}
	return config
}

func setupLogging(config Config, overrides *options) *zap.Logger {
	if overrides.logger != nil {
		log.SetGlobalLogger(overrides.logger.Sugar())
		return overrides.logger
	} else {
		zlog := log.ConfigureLogger(config.ZapLogging)
		return zlog.Desugar()
	}
}

func setupMetrics(config Config, flags *Flags, overrides *options) (tally.Scope, func()) {
	if overrides.metrics != nil {
		return overrides.metrics, func() {}
	}

	s, closer, err := metrics.New(config.Metrics, flags.KrakenCluster)
	if err != nil {
		log.Fatalf("Failed to init metrics: %s", err)
	}

	go metrics.EmitVersion(s)
	return s, func() { closer.Close() }
}

func setupHostname(flags *Flags) string {
	var hostname string
	if flags.BlobServerHostName == "" {
		var err error
		hostname, err = os.Hostname()
		if err != nil {
			log.Fatalf("Error getting hostname: %s", err)
		}
	} else {
		hostname = flags.BlobServerHostName
	}
	log.Infof("Configuring origin with hostname '%s'", hostname)
	return hostname
}

func setupPeerIP(flags *Flags) string {
	if flags.PeerIP == "" {
		localIP, err := netutil.GetLocalIP()
		if err != nil {
			log.Fatalf("Error getting local ip: %s", err)
		}
		return localIP
	}
	return flags.PeerIP
}

type coreComponents struct {
	cas              *store.CAStore
	pctx             core.PeerContext
	backendManager   *backend.Manager
	writeBackManager persistedretry.Manager
	metaInfoGen      *metainfogen.Generator
	blobRefresher    *blobrefresh.Refresher
	scheduler        scheduler.ReloadableScheduler
	hashRing         hashring.Ring
	tls              *tls.Config
}

func setupCoreComponents(config Config, flags *Flags, hostname, peerIP string, stats tally.Scope) *coreComponents {
	cas := setupCAStore(config, stats)
	pctx := setupPeerContext(config, flags, peerIP)
	backendManager := setupBackendManager(config, stats)

	localDB := setupLocalDB(config)
	writeBackManager := setupWriteBackManager(config, stats, cas, backendManager, localDB)
	metaInfoGen := setupMetaInfoGenerator(config, cas)
	blobRefresher := setupBlobRefresher(config, stats, cas, backendManager, metaInfoGen)

	netevents := setupNetworkEvents(config)
	schedulerInstance := setupScheduler(config, stats, pctx, cas, netevents, blobRefresher)

	cluster := setupCluster(config)
	tlsConfig := setupTLS(config)
	hashRing := setupHashRing(config, flags, hostname, cluster, tlsConfig, backendManager)

	return &coreComponents{
		cas:              cas,
		pctx:             pctx,
		backendManager:   backendManager,
		writeBackManager: writeBackManager,
		metaInfoGen:      metaInfoGen,
		blobRefresher:    blobRefresher,
		scheduler:        schedulerInstance,
		hashRing:         hashRing,
		tls:              tlsConfig,
	}
}

func setupCAStore(config Config, stats tally.Scope) *store.CAStore {
	cas, err := store.NewCAStore(config.CAStore, stats)
	if err != nil {
		log.Fatalf("Failed to create castore: %s", err)
	}
	return cas
}

func setupPeerContext(config Config, flags *Flags, peerIP string) core.PeerContext {
	pctx, err := core.NewPeerContext(
		config.PeerIDFactory, flags.Zone, flags.KrakenCluster, peerIP, flags.PeerPort, true)
	if err != nil {
		log.Fatalf("Failed to create peer context: %s", err)
	}
	return pctx
}

func setupBackendManager(config Config, stats tally.Scope) *backend.Manager {
	backendManager, err := backend.NewManager(config.BackendManager, config.Backends, config.Auth, stats)
	if err != nil {
		log.Fatalf("Error creating backend manager: %s", err)
	}
	return backendManager
}

func setupLocalDB(config Config) *sqlx.DB {
	localDB, err := localdb.New(config.LocalDB)
	if err != nil {
		log.Fatalf("Error creating local db: %s", err)
	}
	return localDB
}

func setupWriteBackManager(config Config, stats tally.Scope, cas *store.CAStore, backendManager *backend.Manager, localDB *sqlx.DB) persistedretry.Manager {
	writeBackManager, err := persistedretry.NewManager(
		config.WriteBack,
		stats,
		writeback.NewStore(localDB),
		writeback.NewExecutor(stats, cas, backendManager))
	if err != nil {
		log.Fatalf("Error creating write-back manager: %s", err)
	}
	return writeBackManager
}

func setupMetaInfoGenerator(config Config, cas *store.CAStore) *metainfogen.Generator {
	metaInfoGenerator, err := metainfogen.New(config.MetaInfoGen, cas)
	if err != nil {
		log.Fatalf("Error creating metainfo generator: %s", err)
	}
	return metaInfoGenerator
}

func setupBlobRefresher(config Config, stats tally.Scope, cas *store.CAStore, backendManager *backend.Manager, metaInfoGen *metainfogen.Generator) *blobrefresh.Refresher {
	return blobrefresh.New(config.BlobRefresh, stats, cas, backendManager, metaInfoGen)
}

func setupNetworkEvents(config Config) networkevent.Producer {
	netevents, err := networkevent.NewProducer(config.NetworkEvent)
	if err != nil {
		log.Fatalf("Error creating network event producer: %s", err)
	}
	return netevents
}

func setupScheduler(config Config, stats tally.Scope, pctx core.PeerContext, cas *store.CAStore, netevents networkevent.Producer, blobRefresher *blobrefresh.Refresher) scheduler.ReloadableScheduler {
	sched, err := scheduler.NewOriginScheduler(
		config.Scheduler, stats, pctx, cas, netevents, blobRefresher)
	if err != nil {
		log.Fatalf("Error creating scheduler: %s", err)
	}
	return sched
}

func setupCluster(config Config) hostlist.List {
	cluster, err := hostlist.New(config.Cluster)
	if err != nil {
		log.Fatalf("Error creating cluster host list: %s", err)
	}
	return cluster
}

func setupTLS(config Config) *tls.Config {
	tls, err := config.TLS.BuildClient()
	if err != nil {
		log.Fatalf("Error building client tls config: %s", err)
	}
	return tls
}

func setupHashRing(config Config, flags *Flags, hostname string, cluster hostlist.List, tls *tls.Config, backendManager *backend.Manager) hashring.Ring {
	healthCheckFilter := healthcheck.NewFilter(config.HealthCheck, healthcheck.Default(tls))

	hashRing := hashring.New(
		config.HashRing,
		cluster,
		healthCheckFilter,
		hashring.WithWatcher(backend.NewBandwidthWatcher(backendManager)))
	go hashRing.Monitor(nil)

	// Validate that this origin is in the hash ring
	addr := fmt.Sprintf("%s:%d", hostname, flags.BlobServerPort)
	if !hashRing.Contains(addr) {
		// When DNS is used for hash ring membership, the members will be IP
		// addresses instead of hostnames.
		ip, err := netutil.GetLocalIP()
		if err != nil {
			log.Fatalf("Error getting local ip: %s", err)
		}
		addr = fmt.Sprintf("%s:%d", ip, flags.BlobServerPort)
		if !hashRing.Contains(addr) {
			log.Fatalf(
				"Neither %s nor %s (port %d) found in hash ring",
				hostname, ip, flags.BlobServerPort)
		}
	}

	return hashRing
}

func setupBlobServer(config Config, flags *Flags, hostname string, components *coreComponents, stats tally.Scope) *blobserver.Server {
	addr := fmt.Sprintf("%s:%d", hostname, flags.BlobServerPort)

	server, err := blobserver.New(
		config.BlobServer,
		stats,
		clock.New(),
		addr,
		components.hashRing,
		components.cas,
		blobclient.NewProvider(blobclient.WithTLS(components.tls)),
		blobclient.NewClusterProvider(blobclient.WithTLS(components.tls)),
		components.pctx,
		components.backendManager,
		components.blobRefresher,
		components.metaInfoGen,
		components.writeBackManager)
	if err != nil {
		log.Fatalf("Error initializing blob server: %s", err)
	}

	return server
}

func startServices(config Config, flags *Flags, server *blobserver.Server, sched scheduler.ReloadableScheduler) {
	h := addTorrentDebugEndpoints(server.Handler(), sched)

	go func() { log.Fatal(server.ListenAndServe(h)) }()

	log.Info("Starting nginx...")
	log.Fatal(nginx.Run(
		config.Nginx,
		map[string]interface{}{
			"port":   flags.BlobServerPort,
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
