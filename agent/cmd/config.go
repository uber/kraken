package cmd

import (
	"github.com/uber/kraken/agent/agentserver"
	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/dockerregistry"
	"github.com/uber/kraken/lib/store"
	"github.com/uber/kraken/lib/torrent/networkevent"
	"github.com/uber/kraken/lib/torrent/scheduler"
	"github.com/uber/kraken/lib/upstream"
	"github.com/uber/kraken/metrics"
	"github.com/uber/kraken/nginx"
	"github.com/uber/kraken/tracker/trackerclient"
	"github.com/uber/kraken/utils/httputil"

	"go.uber.org/zap"
)

// Config defines agent configuration.
type Config struct {
	ZapLogging      zap.Config                  `yaml:"zap"`
	Metrics         metrics.Config              `yaml:"metrics"`
	CADownloadStore store.CADownloadStoreConfig `yaml:"store"`
	Registry        dockerregistry.Config       `yaml:"registry"`
	Scheduler       scheduler.Config            `yaml:"scheduler"`
	PeerIDFactory   core.PeerIDFactory          `yaml:"peer_id_factory"`
	NetworkEvent    networkevent.Config         `yaml:"network_event"`
	Tracker         trackerclient.Config        `yaml:"tracker"`
	BuildIndex      upstream.PassiveConfig      `yaml:"build_index"`
	AgentServer     agentserver.Config          `yaml:"agentserver"`
	RegistryBackup  string                      `yaml:"registry_backup"`
	Nginx           nginx.Config                `yaml:"nginx"`
	TLS             httputil.TLSConfig          `yaml:"tls"`
}
