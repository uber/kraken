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
	"context"
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
	"github.com/uber/kraken/tracker/announceclient"
	"github.com/uber/kraken/utils/configutil"
	"github.com/uber/kraken/utils/log"
	"github.com/uber/kraken/utils/netutil"

	"github.com/docker/distribution/registry"
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

// App represents the agent application with all its components.
type App struct {
	config  Config
	flags   *Flags
	stats   tally.Scope
	logger  *zap.Logger
	
	// Components
	peerContext     *core.PeerContext
	cads            *store.CADownloadStore
	scheduler       scheduler.ReloadableScheduler
	tagClient       tagclient.Client
	registry        *registry.Registry
	agentServer     *agentserver.Server
	
	// Cleanup functions
	cleanup []func()
}

// NewApp creates a new agent application.
func NewApp(flags *Flags, opts ...Option) (*App, error) {
	app := &App{
		flags:   flags,
		cleanup: make([]func(), 0),
	}
	
	if err := app.parseOptions(opts...); err != nil {
		return nil, fmt.Errorf("parse options: %w", err)
	}
	
	if err := app.validateFlags(); err != nil {
		return nil, fmt.Errorf("validate flags: %w", err)
	}
	
	if err := app.loadConfig(); err != nil {
		return nil, fmt.Errorf("load config: %w", err)
	}
	
	if err := app.setupLogging(); err != nil {
		return nil, fmt.Errorf("setup logging: %w", err)
	}
	
	if err := app.setupMetrics(); err != nil {
		return nil, fmt.Errorf("setup metrics: %w", err)
	}
	
	return app, nil
}

func (a *App) parseOptions(opts ...Option) error {
	var overrides options
	for _, o := range opts {
		o(&overrides)
	}
	
	if overrides.config != nil {
		a.config = *overrides.config
	}
	if overrides.metrics != nil {
		a.stats = overrides.metrics
	}
	if overrides.logger != nil {
		a.logger = overrides.logger
	}
	
	return nil
}

func (a *App) validateFlags() error {
	if a.flags.PeerPort == 0 {
		return fmt.Errorf("must specify non-zero peer port")
	}
	if a.flags.AgentServerPort == 0 {
		return fmt.Errorf("must specify non-zero agent server port")
	}
	if a.flags.AgentRegistryPort == 0 {
		return fmt.Errorf("must specify non-zero agent registry port")
	}
	return nil
}

func (a *App) loadConfig() error {
	if a.config == (Config{}) {
		if err := configutil.Load(a.flags.ConfigFile, &a.config); err != nil {
			return fmt.Errorf("load config file: %w", err)
		}
		if a.flags.SecretsFile != "" {
			if err := configutil.Load(a.flags.SecretsFile, &a.config); err != nil {
				return fmt.Errorf("load secrets file: %w", err)
			}
		}
	}
	return nil
}

func (a *App) setupLogging() error {
	if a.logger != nil {
		log.SetGlobalLogger(a.logger.Sugar())
	} else {
		zlog := log.ConfigureLogger(a.config.ZapLogging)
		a.logger = zlog.Desugar()
		a.cleanup = append(a.cleanup, func() { zlog.Sync() })
	}
	return nil
}

func (a *App) setupMetrics() error {
	if a.stats == nil {
		s, closer, err := metrics.New(a.config.Metrics, a.flags.KrakenCluster)
		if err != nil {
			return fmt.Errorf("init metrics: %w", err)
		}
		a.stats = s
		a.cleanup = append(a.cleanup, func() { closer.Close() })
	}
	
	go metrics.EmitVersion(a.stats)
	return nil
}

func (a *App) setupPeerContext() error {
	peerIP := a.flags.PeerIP
	if peerIP == "" {
		localIP, err := netutil.GetLocalIP()
		if err != nil {
			return fmt.Errorf("get local IP: %w", err)
		}
		peerIP = localIP
	}
	
	pctx, err := core.NewPeerContext(
		a.config.PeerIDFactory, a.flags.Zone, a.flags.KrakenCluster, peerIP, a.flags.PeerPort, false)
	if err != nil {
		return fmt.Errorf("create peer context: %w", err)
	}
	
	a.peerContext = pctx
	return nil
}

func (a *App) setupStorage() error {
	cads, err := store.NewCADownloadStore(a.config.CADownloadStore, a.stats)
	if err != nil {
		return fmt.Errorf("create CA download store: %w", err)
	}
	a.cads = cads
	return nil
}

func (a *App) setupScheduler() error {
	netevents, err := networkevent.NewProducer(a.config.NetworkEvent)
	if err != nil {
		return fmt.Errorf("create network event producer: %w", err)
	}
	
	trackers, err := a.config.Tracker.Build()
	if err != nil {
		return fmt.Errorf("build tracker upstream: %w", err)
	}
	go trackers.Monitor(nil)
	
	tls, err := a.config.TLS.BuildClient()
	if err != nil {
		return fmt.Errorf("build client TLS config: %w", err)
	}
	
	announceClient := announceclient.New(a.peerContext, trackers, tls)
	sched, err := scheduler.NewAgentScheduler(
		a.config.Scheduler, a.stats, a.peerContext, a.cads, netevents, trackers, announceClient, tls)
	if err != nil {
		return fmt.Errorf("create scheduler: %w", err)
	}
	
	a.scheduler = sched
	return nil
}

func (a *App) setupTagClient() error {
	buildIndexes, err := a.config.BuildIndex.Build()
	if err != nil {
		return fmt.Errorf("build build-index upstream: %w", err)
	}
	
	tls, err := a.config.TLS.BuildClient()
	if err != nil {
		return fmt.Errorf("build client TLS config: %w", err)
	}
	
	a.tagClient = tagclient.NewClusterClient(buildIndexes, tls)
	return nil
}

func (a *App) setupRegistry() error {
	transferer := transfer.NewReadOnlyTransferer(a.stats, a.cads, a.tagClient, a.scheduler)
	
	registry, err := a.config.Registry.Build(a.config.Registry.ReadOnlyParameters(transferer, a.cads, a.stats))
	if err != nil {
		return fmt.Errorf("init registry: %w", err)
	}
	
	a.registry = registry
	return nil
}

func (a *App) setupAgentServer() error {
	registryAddr := fmt.Sprintf("127.0.0.1:%d", a.flags.AgentRegistryPort)
	
	containerRuntimeCfg := a.config.ContainerRuntime
	dockerdaemonCfg := dockerdaemon.Config{}
	if a.config.DockerDaemon != dockerdaemonCfg {
		log.Warn("please move docker config under \"container_runtime\"")
		containerRuntimeCfg.Docker = a.config.DockerDaemon
	}
	
	containerRuntimeFactory, err := containerruntime.NewFactory(containerRuntimeCfg, registryAddr)
	if err != nil {
		return fmt.Errorf("create container runtime factory: %w", err)
	}
	
	tls, err := a.config.TLS.BuildClient()
	if err != nil {
		return fmt.Errorf("build client TLS config: %w", err)
	}
	
	announceClient := announceclient.New(a.peerContext, nil, tls)
	a.agentServer = agentserver.New(
		a.config.AgentServer, a.stats, a.cads, a.scheduler, a.tagClient, announceClient, containerRuntimeFactory)
	
	return nil
}

// Initialize sets up all the agent components.
func (a *App) Initialize() error {
	setupSteps := []struct {
		name string
		fn   func() error
	}{
		{"peer context", a.setupPeerContext},
		{"storage", a.setupStorage},
		{"scheduler", a.setupScheduler},
		{"tag client", a.setupTagClient},
		{"registry", a.setupRegistry},
		{"agent server", a.setupAgentServer},
	}
	
	for _, step := range setupSteps {
		if err := step.fn(); err != nil {
			return fmt.Errorf("setup %s: %w", step.name, err)
		}
	}
	
	return nil
}

// Run starts the agent application.
func (a *App) Run(ctx context.Context) error {
	agentAddr := fmt.Sprintf(":%d", a.flags.AgentServerPort)
	log.Infof("Starting agent server on %s", agentAddr)
	
	agentSrv := &http.Server{
		Addr:    agentAddr,
		Handler: a.agentServer.Handler(),
	}
	
	go func() {
		if err := agentSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Errorf("Agent server error: %v", err)
		}
	}()
	
	log.Info("Starting registry...")
	go func() {
		if err := a.registry.ListenAndServe(); err != nil {
			log.Errorf("Registry error: %v", err)
		}
	}()
	
	// Start heartbeat
	go a.heartbeat()
	
	// Start nginx
	nginxDone := make(chan error, 1)
	go func() {
		err := nginx.Run(a.config.Nginx, map[string]interface{}{
			"allowed_cidrs": a.config.AllowedCidrs,
			"port":          a.flags.AgentRegistryPort,
			"registry_server": nginx.GetServer(
				a.config.Registry.Docker.HTTP.Net, a.config.Registry.Docker.HTTP.Addr),
			"agent_server":    fmt.Sprintf("127.0.0.1:%d", a.flags.AgentServerPort),
			"registry_backup": a.config.RegistryBackup},
			nginx.WithTLS(a.config.TLS))
		nginxDone <- err
	}()
	
	// Wait for context cancellation or nginx error
	select {
	case <-ctx.Done():
		log.Info("Shutting down agent...")
		return a.shutdown(agentSrv)
	case err := <-nginxDone:
		return fmt.Errorf("nginx error: %w", err)
	}
}

func (a *App) shutdown(agentSrv *http.Server) error {
	// Shutdown agent server gracefully
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	
	if err := agentSrv.Shutdown(shutdownCtx); err != nil {
		log.Errorf("Agent server shutdown error: %v", err)
	}
	
	// Run cleanup functions
	for i := len(a.cleanup) - 1; i >= 0; i-- {
		a.cleanup[i]()
	}
	
	return nil
}

func (a *App) heartbeat() {
	for {
		a.stats.Counter("heartbeat").Inc(1)
		time.Sleep(10 * time.Second)
	}
}

// Run runs the agent (legacy function for backward compatibility).
func Run(flags *Flags, opts ...Option) {
	app, err := NewApp(flags, opts...)
	if err != nil {
		log.Fatalf("Failed to create app: %v", err)
	}
	
	if err := app.Initialize(); err != nil {
		log.Fatalf("Failed to initialize app: %v", err)
	}
	
	ctx := context.Background()
	if err := app.Run(ctx); err != nil {
		log.Fatalf("App run error: %v", err)
	}
}
