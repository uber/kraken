package trackerclient

// ClientConfig defines tracker's client config.
import (
	"github.com/uber/kraken/lib/hashring"
	"github.com/uber/kraken/lib/upstream"
)

// Config defines tracker's client config.
type Config struct {
	HashRing hashring.Config        `yaml:"hashring"`
	Cluster  upstream.PassiveConfig `yaml:"cluster"`
}
