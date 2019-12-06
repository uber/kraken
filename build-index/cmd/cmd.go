// Copyright (c) 2016-2019 Uber Technologies, Inc.
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
// limitations under the License.
package cmd

import (
	"flag"

	"github.com/uber/kraken/build-index/tagclient"
	"github.com/uber/kraken/build-index/tagserver"
	"github.com/uber/kraken/build-index/tagstore"
	"github.com/uber/kraken/build-index/tagtype"
	"github.com/uber/kraken/lib/backend"
	"github.com/uber/kraken/lib/healthcheck"
	"github.com/uber/kraken/lib/hostlist"
	"github.com/uber/kraken/lib/persistedretry"
	"github.com/uber/kraken/lib/persistedretry/tagreplication"
	"github.com/uber/kraken/lib/persistedretry/writeback"
	"github.com/uber/kraken/lib/store"
	"github.com/uber/kraken/lib/upstream"
	"github.com/uber/kraken/localdb"
	"github.com/uber/kraken/metrics"
	"github.com/uber/kraken/nginx"
	"github.com/uber/kraken/origin/blobclient"
	"github.com/uber/kraken/utils/configutil"
	"github.com/uber/kraken/utils/log"

	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

// Flags defines build-index CLI flags.
type Flags struct {
	Port          int
	ConfigFile    string
	KrakenCluster string
	SecretsFile   string
}

// ParseFlags parses build-index CLI flags.
func ParseFlags() *Flags {
	var flags Flags
	flag.IntVar(
		&flags.Port, "port", 0, "tag server port")
	flag.StringVar(
		&flags.ConfigFile, "config", "", "configuration file path")
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

// Run runs the build-index.
func Run(flags *Flags, opts ...Option) {
	if flags.Port == 0 {
		panic("must specify non-zero port")
	}

	var overrides options
	for _, o := range opts {
		o(&overrides)
	}

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

	if overrides.logger != nil {
		log.SetGlobalLogger(overrides.logger.Sugar())
	} else {
		zlog := log.ConfigureLogger(config.ZapLogging)
		defer zlog.Sync()
	}

	stats := overrides.metrics
	if stats == nil {
		s, closer, err := metrics.New(config.Metrics, flags.KrakenCluster)
		if err != nil {
			log.Fatalf("Failed to init metrics: %s", err)
		}
		stats = s
		defer closer.Close()
	}

	go metrics.EmitVersion(stats)

	ss, err := store.NewSimpleStore(config.Store, stats)
	if err != nil {
		log.Fatalf("Error creating simple store: %s", err)
	}

	backends, err := backend.NewManager(config.Backends, config.Auth)
	if err != nil {
		log.Fatalf("Error creating backend manager: %s", err)
	}

	tls, err := config.TLS.BuildClient()
	if err != nil {
		log.Fatalf("Error building client tls config: %s", err)
	}

	origins, err := config.Origin.Build(upstream.WithHealthCheck(healthcheck.Default(tls)))
	if err != nil {
		log.Fatalf("Error building origin host list: %s", err)
	}

	r := blobclient.NewClientResolver(blobclient.NewProvider(blobclient.WithTLS(tls)), origins)
	originClient := blobclient.NewClusterClient(r)

	localOriginDNS, err := config.Origin.StableAddr()
	if err != nil {
		log.Fatalf("Error getting stable origin addr: %s", err)
	}

	localDB, err := localdb.New(config.LocalDB)
	if err != nil {
		log.Fatalf("Error creating local db: %s", err)
	}

	cluster, err := config.Cluster.Build(upstream.WithHealthCheck(healthcheck.Default(tls)))
	if err != nil {
		log.Fatalf("Error building cluster host list: %s", err)
	}
	neighbors, err := hostlist.StripLocal(cluster, flags.Port)
	if err != nil {
		log.Fatalf("Error stripping local machine from cluster list: %s", err)
	}

	remotes, err := config.Remotes.Build()
	if err != nil {
		log.Fatalf("Error building remotes from configuration: %s", err)
	}

	tagReplicationExecutor := tagreplication.NewExecutor(
		stats,
		originClient,
		tagclient.NewProvider(tls))
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

	tagStore := tagstore.New(config.TagStore, stats, ss, backends, writeBackManager)

	depResolver, err := tagtype.NewMap(config.TagTypes, originClient)
	if err != nil {
		log.Fatalf("Error creating tag type manager: %s", err)
	}

	server := tagserver.New(
		config.TagServer,
		stats,
		backends,
		localOriginDNS,
		originClient,
		neighbors,
		tagStore,
		remotes,
		tagReplicationManager,
		tagclient.NewProvider(tls),
		depResolver)
	go func() {
		log.Fatal(server.ListenAndServe())
	}()

	log.Info("Starting nginx...")
	log.Fatal(nginx.Run(
		config.Nginx,
		map[string]interface{}{
			"port":   flags.Port,
			"server": nginx.GetServer(config.TagServer.Listener.Net, config.TagServer.Listener.Addr),
		},
		nginx.WithTLS(config.TLS)))
}
