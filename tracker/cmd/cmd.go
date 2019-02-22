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
// limitations under the License.
package cmd

import (
	"github.com/andres-erbsen/clock"
	"github.com/spf13/cobra"
	"github.com/uber/kraken/lib/healthcheck"
	"github.com/uber/kraken/lib/upstream"
	"github.com/uber/kraken/metrics"
	"github.com/uber/kraken/nginx"
	"github.com/uber/kraken/origin/blobclient"
	"github.com/uber/kraken/tracker/originstore"
	"github.com/uber/kraken/tracker/peerhandoutpolicy"
	"github.com/uber/kraken/tracker/peerstore"
	"github.com/uber/kraken/tracker/trackerserver"
	"github.com/uber/kraken/utils/configutil"
	"github.com/uber/kraken/utils/log"
)

var (
	port          int
	configFile    string
	krakenCluster string

	rootCmd = &cobra.Command{
		Short: "kraken-tracker keeps track of all the peers and their data in the p2p network.",
		Run: func(rootCmd *cobra.Command, args []string) {
			run()
		},
	}
)

func init() {
	rootCmd.PersistentFlags().IntVarP(
		&port, "port", "", 0, "port to listen on")
	rootCmd.PersistentFlags().StringVarP(
		&configFile, "config", "", "", "configuration file path")
	rootCmd.PersistentFlags().StringVarP(
		&krakenCluster, "cluster", "", "", "cluster name (e.g. prod01-zone1)")
}

func Execute() {
	rootCmd.Execute()
}

func run() {
	var config Config
	if err := configutil.Load(configFile, &config); err != nil {
		panic(err)
	}
	log.ConfigureLogger(config.ZapLogging)

	stats, closer, err := metrics.New(config.Metrics, krakenCluster)
	if err != nil {
		log.Fatalf("Failed to init metrics: %s", err)
	}
	defer closer.Close()

	go metrics.EmitVersion(stats)

	peerStore, err := peerstore.NewRedisStore(config.PeerStore.Redis, clock.New())
	if err != nil {
		log.Fatalf("Could not create PeerStore: %s", err)
	}

	tls, err := config.TLS.BuildClient()
	if err != nil {
		log.Fatalf("Error building client tls config: %s", err)
	}

	origins, err := config.Origin.Build(upstream.WithHealthCheck(healthcheck.Default(tls)))
	if err != nil {
		log.Fatalf("Error building origin host list: %s", err)
	}

	originStore := originstore.New(
		config.OriginStore, clock.New(), origins, blobclient.NewProvider(blobclient.WithTLS(tls)))

	policy, err := peerhandoutpolicy.NewPriorityPolicy(stats, config.PeerHandoutPolicy.Priority)
	if err != nil {
		log.Fatalf("Could not load peer handout policy: %s", err)
	}

	r := blobclient.NewClientResolver(blobclient.NewProvider(blobclient.WithTLS(tls)), origins)
	originCluster := blobclient.NewClusterClient(r)

	server := trackerserver.New(
		config.TrackerServer, stats, policy, peerStore, originStore, originCluster)
	go func() {
		log.Fatal(server.ListenAndServe())
	}()

	log.Info("Starting nginx...")
	log.Fatal(nginx.Run(config.Nginx, map[string]interface{}{
		"port": port,
		"server": nginx.GetServer(
			config.TrackerServer.Listener.Net, config.TrackerServer.Listener.Addr)},
		nginx.WithTLS(config.TLS)))
}
