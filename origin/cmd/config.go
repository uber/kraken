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
	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/backend"
	"github.com/uber/kraken/lib/blobrefresh"
	"github.com/uber/kraken/lib/hashring"
	"github.com/uber/kraken/lib/healthcheck"
	"github.com/uber/kraken/lib/hostlist"
	"github.com/uber/kraken/lib/metainfogen"
	"github.com/uber/kraken/lib/persistedretry"
	"github.com/uber/kraken/lib/store"
	"github.com/uber/kraken/lib/torrent/networkevent"
	"github.com/uber/kraken/lib/torrent/scheduler"
	"github.com/uber/kraken/localdb"
	"github.com/uber/kraken/metrics"
	"github.com/uber/kraken/nginx"
	"github.com/uber/kraken/origin/blobserver"
	"github.com/uber/kraken/utils/httputil"

	"go.uber.org/zap"
)

// Config defines origin server configuration.
// TODO(evelynl94): consolidate cluster and hashring.
type Config struct {
	Verbose       bool
	ZapLogging    zap.Config               `yaml:"zap"`
	Cluster       hostlist.Config          `yaml:"cluster"`
	HashRing      hashring.Config          `yaml:"hashring"`
	HealthCheck   healthcheck.FilterConfig `yaml:"healthcheck"`
	BlobServer    blobserver.Config        `yaml:"blobserver"`
	CAStore       store.CAStoreConfig      `yaml:"castore"`
	Scheduler     scheduler.Config         `yaml:"scheduler"`
	NetworkEvent  networkevent.Config      `yaml:"network_event"`
	PeerIDFactory core.PeerIDFactory       `yaml:"peer_id_factory"`
	Metrics       metrics.Config           `yaml:"metrics"`
	MetaInfoGen   metainfogen.Config       `yaml:"metainfogen"`
	Backends      []backend.Config         `yaml:"backends"`
	Auth          backend.AuthConfig       `yaml:"auth"`
	BlobRefresh   blobrefresh.Config       `yaml:"blobrefresh"`
	LocalDB       localdb.Config           `yaml:"localdb"`
	WriteBack     persistedretry.Config    `yaml:"writeback"`
	Nginx         nginx.Config             `yaml:"nginx"`
	TLS           httputil.TLSConfig       `yaml:"tls"`
}
