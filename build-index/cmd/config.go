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
	"github.com/uber/kraken/build-index/tagserver"
	"github.com/uber/kraken/build-index/tagstore"
	"github.com/uber/kraken/build-index/tagtype"
	"github.com/uber/kraken/lib/backend"
	"github.com/uber/kraken/lib/persistedretry"
	"github.com/uber/kraken/lib/persistedretry/tagreplication"
	"github.com/uber/kraken/lib/store"
	"github.com/uber/kraken/lib/upstream"
	"github.com/uber/kraken/localdb"
	"github.com/uber/kraken/metrics"
	"github.com/uber/kraken/nginx"
	"github.com/uber/kraken/utils/httputil"

	"go.uber.org/zap"
)

// Config defines build-index configuration.
type Config struct {
	ZapLogging     zap.Config                   `yaml:"zap"`
	Metrics        metrics.Config               `yaml:"metrics"`
	Backends       []backend.Config             `yaml:"backends"`
	Auth           backend.AuthConfig           `yaml:"auth"`
	TagServer      tagserver.Config             `yaml:"tagserver"`
	Remotes        tagreplication.RemotesConfig `yaml:"remotes"`
	TagReplication persistedretry.Config        `yaml:"tag_replication"`
	TagTypes       []tagtype.Config             `yaml:"tag_types"`
	Origin         upstream.ActiveConfig        `yaml:"origin"`
	LocalDB        localdb.Config               `yaml:"localdb"`
	Cluster        upstream.ActiveConfig        `yaml:"cluster"`
	TagStore       tagstore.Config              `yaml:"tag_store"`
	Store          store.SimpleStoreConfig      `yaml:"store"`
	WriteBack      persistedretry.Config        `yaml:"writeback"`
	Nginx          nginx.Config                 `yaml:"nginx"`
	TLS            httputil.TLSConfig           `yaml:"tls"`
}
