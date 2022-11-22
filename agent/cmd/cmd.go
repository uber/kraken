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
	"fmt"
	"net/http"
	"time"

	"github.com/uber/kraken/agent/agentserver"
	"github.com/uber/kraken/build-index/tagclient"
	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/containerruntime"
	"github.com/uber/kraken/lib/containerruntime/dockerdaemon"
	"github.com/uber/kraken/lib/dockerregistry/transfer"
	"github.com/uber/kraken/lib/store"
	"github.com/uber/kraken/lib/torrent/networkevent"
	"github.com/uber/kraken/lib/torrent/scheduler"
	"github.com/uber/kraken/metrics"
	"github.com/uber/kraken/nginx"
	"github.com/uber/kraken/utils/configutil"
	"github.com/uber/kraken/utils/log"
	"github.com/uber/kraken/utils/netutil"

	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

// Flags defines agent CLI flags.
type Flags struct {
	PeerIP            string
	PeerPort          int
	AgentServerPort   int
	AgentRegistryPort int
	ConfigFile        string
	Zone              string
	KrakenCluster     string
	SecretsFile       string
}

// ParseFlags parses agent CLI flags.
func ParseFlags() *Flags {
	var flags Flags
	flag.StringVar(
		&flags.PeerIP, "peer-ip", "", "ip which peer will announce itself as")
	flag.IntVar(
		&flags.PeerPort, "peer-port", 0, "port which peer will announce itself as")
	flag.IntVar(
		&flags.AgentServerPort, "agent-server-port", 0, "port which agent server listens on")
	flag.IntVar(
		&flags.AgentRegistryPort, "agent-registry-port", 0, "port which agent registry listens on")
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

// Run runs the agent.
func Run(flags *Flags, opts ...Option) {
	if flags.PeerPort == 0 {
		panic("must specify non-zero peer port")
	}
	if flags.AgentServerPort == 0 {
		panic("must specify non-zero agent server port")
	}
	if flags.AgentRegistryPort == 0 {
		panic("must specify non-zero agent registry port")
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

	if flags.PeerIP == "" {
		localIP, err := netutil.GetLocalIP()
		if err != nil {
			log.Fatalf("Error getting local ip: %s", err)
		}
		flags.PeerIP = localIP
	}

	pctx, err := core.NewPeerContext(
		config.PeerIDFactory, flags.Zone, flags.KrakenCluster, flags.PeerIP, flags.PeerPort, false)
	if err != nil {
		log.Fatalf("Failed to create peer context: %s", err)
	}

	cads, err := store.NewCADownloadStore(config.CADownloadStore, stats)
	if err != nil {
		log.Fatalf("Failed to create local store: %s", err)
	}

	netevents, err := networkevent.NewProducer(config.NetworkEvent)
	if err != nil {
		log.Fatalf("Failed to create network event producer: %s", err)
	}

	trackers, err := config.Tracker.Build()
	if err != nil {
		log.Fatalf("Error building tracker upstream: %s", err)
	}
	go trackers.Monitor(nil)

	tls, err := config.TLS.BuildClient()
	if err != nil {
		log.Fatalf("Error building client tls config: %s", err)
	}

	sched, err := scheduler.NewAgentScheduler(
		config.Scheduler, stats, pctx, cads, netevents, trackers, tls)
	if err != nil {
		log.Fatalf("Error creating scheduler: %s", err)
	}

	buildIndexes, err := config.BuildIndex.Build()
	if err != nil {
		log.Fatalf("Error building build-index upstream: %s", err)
	}

	tagClient := tagclient.NewClusterClient(buildIndexes, tls)

	transferer := transfer.NewReadOnlyTransferer(stats, cads, tagClient, sched)

	registry, err := config.Registry.Build(config.Registry.ReadOnlyParameters(transferer, cads, stats))
	if err != nil {
		log.Fatalf("Failed to init registry: %s", err)
	}

	registryAddr := fmt.Sprintf("127.0.0.1:%d", flags.AgentRegistryPort)
	containerRuntimeCfg := config.ContainerRuntime
	dockerdaemonCfg := dockerdaemon.Config{}
	if config.DockerDaemon != dockerdaemonCfg {
		log.Warn("please move docker config under \"container_runtime\"")
		containerRuntimeCfg.Docker = config.DockerDaemon
	}
	containerRuntimeFactory, err := containerruntime.NewFactory(containerRuntimeCfg, registryAddr)
	if err != nil {
		log.Fatalf("Failed to create container runtime factory: %s", err)
	}

	agentServer := agentserver.New(
		config.AgentServer, stats, cads, sched, tagClient, containerRuntimeFactory)
	addr := fmt.Sprintf(":%d", flags.AgentServerPort)
	log.Infof("Starting agent server on %s", addr)
	go func() {
		log.Fatal(http.ListenAndServe(addr, agentServer.Handler()))
	}()

	log.Info("Starting registry...")
	go func() {
		log.Fatal(registry.ListenAndServe())
	}()

	go heartbeat(stats)

	log.Fatal(nginx.Run(config.Nginx, map[string]interface{}{
		"allowed_cidrs": config.AllowedCidrs,
		"port":          flags.AgentRegistryPort,
		"registry_server": nginx.GetServer(
			config.Registry.Docker.HTTP.Net, config.Registry.Docker.HTTP.Addr),
		"agent_server":    fmt.Sprintf("127.0.0.1:%d", flags.AgentServerPort),
		"registry_backup": config.RegistryBackup},
		nginx.WithTLS(config.TLS)))
}

// heartbeat periodically emits a counter metric which allows us to monitor the
// number of active agents.
func heartbeat(stats tally.Scope) {
	for {
		stats.Counter("heartbeat").Inc(1)
		time.Sleep(10 * time.Second)
	}
}
