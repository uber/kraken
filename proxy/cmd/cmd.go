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

	"github.com/uber/kraken/build-index/tagclient"
	"github.com/uber/kraken/lib/dockerregistry/transfer"
	"github.com/uber/kraken/lib/healthcheck"
	"github.com/uber/kraken/lib/store"
	"github.com/uber/kraken/lib/upstream"
	"github.com/uber/kraken/metrics"
	"github.com/uber/kraken/nginx"
	"github.com/uber/kraken/origin/blobclient"
	"github.com/uber/kraken/proxy/proxyserver"
	"github.com/uber/kraken/proxy/registryoverride"
	"github.com/uber/kraken/utils/configutil"
	"github.com/uber/kraken/utils/flagutil"
	"github.com/uber/kraken/utils/log"
)

// Flags defines proxy CLI flags.
type Flags struct {
	Ports         flagutil.Ints
	ServerPort    int
	ConfigFile    string
	KrakenCluster string
	SecretsFile   string
}

// ParseFlags parses proxy CLI flags.
func ParseFlags() *Flags {
	var flags Flags
	flag.Var(
		&flags.Ports, "port", "port to listen on (may specify multiple)")
	flag.IntVar(
		&flags.ServerPort, "server-port", 0, "http server port to listen on")
	flag.StringVar(
		&flags.ConfigFile, "config", "", "configuration file path")
	flag.StringVar(
		&flags.KrakenCluster, "cluster", "", "cluster name (e.g. prod01-zone1)")
	flag.StringVar(
		&flags.SecretsFile, "secrets", "", "path to a secrets YAML file to load into configuration")
	flag.Parse()
	return &flags
}

// Run runs the proxy.
func Run(flags *Flags) {
	if len(flags.Ports) == 0 {
		panic("must specify a port")
	}

	var config Config
	if err := configutil.Load(flags.ConfigFile, &config); err != nil {
		panic(err)
	}
	if flags.SecretsFile != "" {
		if err := configutil.Load(flags.SecretsFile, &config); err != nil {
			panic(err)
		}
	}

	log.ConfigureLogger(config.ZapLogging)

	if len(flags.Ports) == 0 {
		log.Fatal("Must specify at least one -port")
	}

	stats, closer, err := metrics.New(config.Metrics, flags.KrakenCluster)
	if err != nil {
		log.Fatalf("Failed to init metrics: %s", err)
	}
	defer closer.Close()

	go metrics.EmitVersion(stats)

	cas, err := store.NewCAStore(config.CAStore, stats)
	if err != nil {
		log.Fatalf("Failed to create store: %s", err)
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
	originCluster := blobclient.NewClusterClient(r)

	buildIndexes, err := config.BuildIndex.Build(upstream.WithHealthCheck(healthcheck.Default(tls)))
	if err != nil {
		log.Fatalf("Error building build-index host list: %s", err)
	}

	tagClient := tagclient.NewClusterClient(buildIndexes, tls)

	transferer := transfer.NewReadWriteTransferer(stats, tagClient, originCluster, cas)

	// open preheat function only when define a server-port
	if flags.ServerPort != 0 {
		server := proxyserver.New(stats, originCluster)
		addr := fmt.Sprintf(":%d", flags.ServerPort)
		log.Infof("Starting http server on %s", addr)
		go func() {
			log.Fatal(http.ListenAndServe(addr, server.Handler()))
		}()
	}

	registry, err := config.Registry.Build(config.Registry.ReadWriteParameters(transferer, cas, stats))
	if err != nil {
		log.Fatalf("Error creating registry: %s", err)
	}
	go func() {
		log.Info("Starting registry...")
		log.Fatal(registry.ListenAndServe())
	}()

	ros := registryoverride.NewServer(config.RegistryOverride, tagClient)
	go func() {
		log.Fatal(ros.ListenAndServe())
	}()

	log.Info("Starting nginx...")
	log.Fatal(nginx.Run(config.Nginx, map[string]interface{}{
		"ports": flags.Ports,
		"registry_server": nginx.GetServer(
			config.Registry.Docker.HTTP.Net, config.Registry.Docker.HTTP.Addr),
		"registry_override_server": nginx.GetServer(
			config.RegistryOverride.Listener.Net, config.RegistryOverride.Listener.Addr)},
		nginx.WithTLS(config.TLS)))
}
