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
	"github.com/uber/kraken/agent/agentserver"
	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/containerruntime"
	"github.com/uber/kraken/lib/containerruntime/dockerdaemon"
	"github.com/uber/kraken/lib/dockerregistry"
	"github.com/uber/kraken/lib/store"
	"github.com/uber/kraken/lib/torrent/networkevent"
	"github.com/uber/kraken/lib/torrent/scheduler"
	"github.com/uber/kraken/lib/upstream"
	"github.com/uber/kraken/metrics"
	"github.com/uber/kraken/nginx"
	"github.com/uber/kraken/utils/httputil"

	"go.uber.org/zap"
)

// Config defines agent configuration.
type Config struct {
	ZapLogging       zap.Config                     `yaml:"zap"`
	Metrics          metrics.Config                 `yaml:"metrics"`
	CADownloadStore  store.CADownloadStoreConfig    `yaml:"store"`
	Registry         dockerregistry.Config          `yaml:"registry"`
	Scheduler        scheduler.Config               `yaml:"scheduler"`
	PeerIDFactory    core.PeerIDFactory             `yaml:"peer_id_factory"`
	NetworkEvent     networkevent.Config            `yaml:"network_event"`
	Tracker          upstream.PassiveHashRingConfig `yaml:"tracker"`
	BuildIndex       upstream.PassiveConfig         `yaml:"build_index"`
	AgentServer      agentserver.Config             `yaml:"agentserver"`
	RegistryBackup   string                         `yaml:"registry_backup"`
	Nginx            nginx.Config                   `yaml:"nginx"`
	TLS              httputil.TLSConfig             `yaml:"tls"`
	AllowedCidrs     []string                       `yaml:"allowed_cidrs"`
	ContainerRuntime containerruntime.Config        `yaml:"container_runtime"`

	// Deprecated
	DockerDaemon dockerdaemon.Config `yaml:"docker_daemon"`
}
